"""
Failovarr — cross-provider channel consolidation with failover for Dispatcharr.

Problem this solves
-------------------
You run two IPTV subscriptions as mutual backups — e.g. Trex and Strong. Each
auto-syncs its own full channel list, so you end up with two parallel universes of
the same channels. What you want is ONE channel per real-world channel, with both
providers' streams attached as failover, so a connection outage on one provider
transparently falls over to the other — and so a channel that only one provider
carries is still kept, never lost.

Why deterministic matching works
---------------------------------
Providers name the same channel almost identically — they differ only by the
separator and quality decoration: ``US| CNN HD`` (Trex) vs ``US: CNN HD`` (Strong).
So the matcher is a **normalize-and-group**, not fuzzy scoring:

* Strip the region prefix (``US|`` / ``EN|``), NFKD-fold superscripts
  (``ᴴᴰ`` -> HD, ``ᴿᴬᵂ`` -> RAW), drop quality tags, collapse to an uppercase key.
* Keep non-region prefixes (``GO|`` / ``PRIME|`` / ``MU|``) IN the key so distinct
  feeds don't over-merge.
* Local stations (US ABC/NBC/CBS/FOX) key on **callsign** (``KABC``) — the only
  stable cross-provider identifier; city labels are inconsistent.
* PPV / event groups are skipped (their dated names would spawn thousands of stale
  channels — hand those to Dispatcharr's native auto-sync). Foreign-country
  prefixes are filtered, keeping US-market channels regardless of language.

The union of every distinct key from any provider becomes the channel set. Both
providers carry a key -> a failover pair (priority order). One carries it ->
single-source, kept. This is a dictionary group-by: it runs in seconds and never
produces the confident-but-wrong matches fuzzy scoring does.

Ownership & reconcile
---------------------
Failovarr CREATES and OWNS its channels (unlike streammirrarr, which rides on
auto-created channels). Ownership = membership in the two managed profiles
(base + adult). The reconcile anchor is a persistent ``keymap.json`` (normalized
key -> channel id + number) kept OUTSIDE the plugin folder so it survives the
repo-managed update (which atomic-swaps the folder). Every routine run reconciles
IN PLACE — add new, update stream ordering, re-assert EPG, prune only keys gone
from every provider — so channel numbers, EPG mappings and manual tweaks are
preserved. A one-time ``seed_reset`` action rebuilds from scratch.

No external dependencies — pure stdlib + Django ORM.
"""

import datetime
import glob
import json
import logging
import os
import re
import threading
import time
import unicodedata
import urllib.parse
import urllib.request

from django.db import close_old_connections, transaction

from apps.channels.models import (
    Channel,
    ChannelGroup,
    ChannelProfile,
    ChannelProfileMembership,
    ChannelStream,
    Stream,
)
from apps.channels.models import ChannelGroupM3UAccount
from apps.m3u.models import M3UAccount

try:
    from core.utils import send_websocket_update
except Exception:  # pragma: no cover - defensive: never block on websocket import
    def send_websocket_update(*_a, **_k):
        return None

__version__ = "0.3.0"

logger = logging.getLogger("plugins.failovarr")

PLUGIN_DIR = os.path.dirname(os.path.abspath(__file__))
PLUGIN_KEY = os.path.basename(PLUGIN_DIR).replace(" ", "_").lower()
STATUS_FILE = os.path.join(PLUGIN_DIR, "last_run.json")
SCHED_DIR = os.path.join(PLUGIN_DIR, ".sched")
RUN_LOCK = os.path.join(SCHED_DIR, "run.lock")
ATTEMPT_FILE = os.path.join(SCHED_DIR, "attempt.ts")

# Persistent state (keymap, health, backups) lives OUTSIDE the plugin folder so it
# survives plugin updates — the repo-managed install atomic-swaps the plugin
# folder, wiping anything inside it. The plugins dir's parent is Dispatcharr's
# persistent, bind-mounted /data in a standard install. Falls back to the plugin
# dir if that isn't writable.
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(PLUGIN_DIR)), "failovarr-data")
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    DATA_DIR = PLUGIN_DIR
KEYMAP_FILE = os.path.join(DATA_DIR, "keymap.json")
# PPV events are ephemeral and churn constantly, so they get their OWN keymap and are
# kept out of the stable channel set's ownership/health accounting entirely.
PPV_KEYMAP_FILE = os.path.join(DATA_DIR, "ppv_keymap.json")
HEALTH_FILE = os.path.join(DATA_DIR, "health.json")
BACKUP_GLOB = os.path.join(DATA_DIR, "backup_*.json")

# Channel-drop safeguard. If a reconcile's computed diff would prune more than
# HEALTH_DROP_FRAC of the existing owned channels (a provider outage returning an
# empty/tiny stream set), skip the pruning and fire a high-priority alert instead.
HEALTH_MIN_BASELINE = 50   # never alarm below this many existing channels
HEALTH_DROP_FRAC = 0.5     # would-prune > 50% of existing = a collapse

# Scheduler tuning.
SCHED_TICK_SECS = 30
SCHED_WINDOW_SECS = 6 * 3600
SCHED_COOLDOWN_SECS = 15 * 60
MAX_DAILY_ATTEMPTS = 3        # hard cap on scheduled runs per day (stops retry/notify spam)
# A crashed holder is reclaimed IMMEDIATELY via the pid-liveness check; this age-only
# ceiling is just the backstop for the rare pid-reuse case, set well above any real run
# (reconciles take seconds; even a seed + EPG refresh is a couple of minutes) so a
# legitimate long run is never stolen out from under itself.
LOCK_STALE_SECS = 60 * 60
BACKUP_KEEP = 7

# ---------------------------------------------------------------- defaults
DEFAULT_SKIP_GROUPS = (
    r"PPV|EVENT|EXCLUSIVE|8K\b|ESPN\s*\+|ESPN PLUS|FLO |NFHS|MILB|DAZN|NCAAB|NBA TEAM|BTN"
)
DEFAULT_ADULT_DETECT = r"18[|:+]|\bADULT\b|\bXXX\b|FOR ADULTS"
DEFAULT_REGION_ALLOW = "US,EN"
# Which provider groups hold PPV/event slots (mined for live events when ppv_events on).
_PPV_DEFAULT_GROUPS = r"PPV|EVENT|EXCLUSIVE|\bMILB\b|\bNFHS\b"
# A LIGHT default: only the clearly-junk PPV packages — high-school, per-team-duplicate
# feeds, mislabeled 24/7, a dead service, and obviously-foreign. Racing, college, minor
# league etc. are KEPT (they have real audiences); curate those to taste via Dispatcharr's
# enabled-group toggles or by editing this regex.
_PPV_SKIP_DEFAULT = r"NFHS|TEAM PPV|24/7|\bFITE\b|\bSTAN\b|VIAPLAY"

# Static defaults the scheduler tick merges over saved settings (kept static so the
# 30s tick never runs an expensive aggregate — see _SCHED_DEFAULTS use).
_SCHED_DEFAULTS = {
    "provider_priority": "",
    "skip_groups": DEFAULT_SKIP_GROUPS,
    "include_247": True,
    "filter_foreign_country": True,
    "keep_us_market": True,
    "region_allowlist": DEFAULT_REGION_ALLOW,
    "merge_quality_variants": True,
    "skip_junk_names": True,
    "adult_profile_split": True,
    "base_profile": "failovarr",
    "adult_profile": "failovarr+18",
    "adult_detect": DEFAULT_ADULT_DETECT,
    "epg_match": True,
    "respect_manual_epg": True,
    "channel_number_start": 1,
    "channel_group_name": "Failovarr",
    "merge_group_suffixes": True,
    "locals_by_name": True,
    "ppv_events": False,
    "ppv_min_providers": 1,
    "ppv_groups": _PPV_DEFAULT_GROUPS,
    "ppv_skip": _PPV_SKIP_DEFAULT,
    "ppv_number_start": 90000,
    "ppv_schedule_minutes": 0,
    "skip_stale": True,
    "schedule_time": "",
    "gotify_notify": "off",
    "gotify_server_url": "",
    "gotify_token": "",
    "health_alert": True,
}

# Quality decorations dropped from keys (NFKD folds superscripts to these first).
_QUALITY = {
    "4K", "UHD", "FHD", "HD", "SD", "HQ", "LQ", "RAW", "H265", "H264", "HEVC",
    "AVC", "VIP", "FPS", "60FPS", "50FPS", "DOLBY", "ATMOS", "VISION", "DV", "HDR",
    # resolution tokens (some providers name a group purely by resolution)
    "2160P", "2160", "3840P", "3840", "1080P", "1080", "720P", "720", "480P",
}

# Foreign COUNTRY prefixes to filter (NOT languages — US-market channels are kept
# regardless of language). Matched as the leading ``XX|`` / ``XX:`` token.
_FOREIGN = {
    "CL", "NL", "AR", "MX", "DE", "FR", "ES", "IT", "PT", "GR", "TR", "RU", "PL",
    "RO", "JP", "CN", "KU", "IL", "IR", "AL", "BG", "PK", "AF", "SO", "BE", "MT",
    "IN", "QC", "LA",
}

# PPV/event parsing. Provider event streams pack status + matchup + time + package
# into one name, e.g.
#   "End | FC Augsburg vs. FC Schalke 04 | all | 30-08-2026 | 08:25 (GMT) | 8K EXCLUSIVE | US: SOCCER PPV 124"
# trex and strong resell the same upstream, so the matchup text is near-identical
# across providers → an order-independent matchup key pairs the same live event.
# (_PPV_DEFAULT_GROUPS is defined up in the defaults section, before _SCHED_DEFAULTS.)
_PPV_IDLE = re.compile(r"NO EVENT|COMING SOON|OFF ?AIR|PLACEHOLDER", re.I)
_PPV_DONE = re.compile(r"\b(END|ENDED|FINISHED|FINAL|FULL ?TIME|\bFT\b|OVER|REPLAY|HIGHLIGHTS?)\b", re.I)
_PPV_LIVE = re.compile(r"\b(LIVE|NEXT|UPCOMING|SOON|START|STARTING|STARTS|NOW|PRE[- ]?GAME)\b", re.I)
_PPV_VS = re.compile(r"\bVS\.?\b")
# A "package / slot / quality" trailer segment (e.g. "US: APPLE TV F1 PPV 1",
# "8K EXCLUSIVE") — never the event's identity, so excluded from title extraction.
_PPV_PKG = re.compile(r"\bPPV\b|EXCLUSIVE|\b8K\b", re.I)
# tokens that are decoration/packaging, never part of the event identity
_PPV_STOP = {
    "ALL", "US", "USA", "8K", "4K", "UHD", "HD", "FHD", "RAW", "EXCLUSIVE", "EXCLUSIF",
    "PPV", "VIP", "GMT", "UTC", "EDT", "EST", "PDT", "PST", "CDT", "CST", "MDT", "MST",
    "AM", "PM", "LIVE", "NEXT", "END", "ENDED", "FINISHED", "FINAL", "SOON", "NOW",
    "EVENT", "STREAMING", "THE",
}
# Date/day/month tokens — ignored when judging whether a fallback segment is a real
# event title (kept OUT of _PPV_STOP so a team like "Sun" or "May" still matches).
_PPV_DATE = {
    "MON", "TUE", "TUES", "WED", "THU", "THUR", "THURS", "FRI", "SAT", "SUN",
    "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY",
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "SEPT", "OCT", "NOV", "DEC",
}

# Prefix = everything before the first | or : (may carry a quality tag or spaces,
# e.g. "AR 4K:" or "US|"). The leading alpha token is the country/region code.
_PREFIX_RE = re.compile(r"^\s*([^|:]{1,24})[|:]\s*(.*)$")
_LOCAL_RE = re.compile(r"\b(ABC|NBC|CBS|FOX)\b")
_CALL_PAREN = re.compile(r"\(([WK][A-Z0-9]{2,4})\)")
_CALL_BARE = re.compile(r"\b([WK][A-Z]{2,3})\b")


# ------------------------------------------------------------ normalization
def _fold(s):
    """NFKD-fold: superscripts -> plain (ᴴᴰ->HD, ⁶⁰->60), NBSP->space, drop marks."""
    if not s:
        return ""
    out = []
    for ch in unicodedata.normalize("NFKD", s):
        if unicodedata.category(ch) == "Mn":  # combining mark
            continue
        out.append(ch)
    return "".join(out)


def _prefix_tokens(s):
    """(prefix_tokens, body) split on the first | or :. prefix_tokens is the list of
    alnum tokens in the prefix ("AR 4K:" -> (['AR','4K'], body)); ([], s) if none."""
    m = _PREFIX_RE.match(s)
    if not m:
        return [], s
    toks = [t for t in re.split(r"[^0-9A-Za-z]+", m.group(1).upper()) if t]
    if not toks:
        return [], s
    return toks, m.group(2)


def _tokens(s):
    return [t for t in re.split(r"[^0-9A-Za-z]+", s.upper()) if t]


_TIER_4K = {"4K", "UHD", "2160", "2160P", "3840", "3840P"}
_TIER_FHD = {"FHD", "1080", "1080P"}
_TIER_HD = {"HD", "720", "720P"}
_TIER_SD = {"SD", "480", "480P"}
_FPS_TOK = re.compile(r"^(\d{2,3})FPS$")
_RES_TOK = re.compile(r"^\d{3,4}P$")


def _is_quality_token(t):
    """A resolution/quality/framerate token (dropped from the base key)."""
    return t in _QUALITY or bool(_FPS_TOK.match(t)) or bool(_RES_TOK.match(t))


def _quality_suffix(name):
    """A CANONICAL quality label for a stream — resolution tier + framerate — so the
    same real quality matches across providers despite different decoration
    (`UHD` vs `UHD 3840P` both → `4K`). **HD/FHD (and untagged) are the modern baseline
    → no suffix**, so a channel labelled `HD` on one sub and untagged on the other still
    pairs. Only genuinely-different tiers (4K above, SD below) and framerate get a label.
    Empty when the name is baseline HD."""
    toks = set(_tokens(_fold(name)))
    if toks & _TIER_4K:
        tier = "4K"
    elif toks & _TIER_SD:
        tier = "SD"
    else:
        tier = ""  # HD / FHD / 720 / 1080 / untagged = assumed-HD baseline
    fps = ""
    for t in toks:
        mt = _FPS_TOK.match(t)
        if mt:
            fps = mt.group(1) + "FPS"
            break
    return " ".join(p for p in (tier, fps) if p)


def _consolidation_key(name, region_allow, drop_quality):
    """Group-by key: strip region prefix (US/EN), keep other prefixes. With
    ``drop_quality`` (merge_quality_variants ON) every resolution/framerate variant
    collapses to one channel; otherwise a canonical tier is appended so HD/4K/fps stay
    distinct but still pair across providers."""
    s = _fold(name)
    ptoks, body = _prefix_tokens(s)
    if ptoks and ptoks[0] in region_allow:
        s = body  # region prefix stripped; a non-region prefix (GO/PRIME) stays in
    # Preserve a "+" as its own token so a "+" brand stays DISTINCT from the base
    # channel (AMC vs AMC+, Paramount vs Paramount+) instead of collapsing together.
    s = s.replace("+", " PLUS ")
    base = " ".join(t for t in _tokens(s) if not _is_quality_token(t))
    if drop_quality:
        return base
    suf = _quality_suffix(name)
    return (base + " " + suf).strip() if suf else base


def _epg_key(name):
    """EPG-lookup key: strip ALL prefixes + quality (myepg.top is keyed by bare names)."""
    s = _fold(name)
    ptoks, body = _prefix_tokens(s)
    while ptoks:
        s = body
        ptoks, body = _prefix_tokens(s)
    return " ".join(t for t in _tokens(s) if not _is_quality_token(t))


# Common W/K words that match the bare-callsign shape but are NOT callsigns — so a
# national feed like "NBC WEST" or "ABC KIDS" isn't mis-keyed as a local affiliate.
_CALL_STOP = {"WEST", "KIDS", "WNBA", "WWE", "WIRE", "KND"}


def _callsign(name):
    s = _fold(name).upper()
    m = _CALL_PAREN.search(s)  # a parenthesized (KABC) is explicit — always trust it
    if m:
        return m.group(1)
    for m in _CALL_BARE.finditer(s):  # bare callsign: skip common non-callsign words
        cs = m.group(1)
        if cs not in _CALL_STOP:
            return cs
    return None


def _epg_callsign(name):
    """If an EPG entry name is a bare station callsign (`KCEN-DT`, `WHDC-LD`, `K42HT-D`
    — the US-locals XMLTV format), return the callsign with the transmission suffix
    stripped (`KCEN`), so it can be indexed for callsign lookup. Else None."""
    base = _fold(name or "").strip().upper().split("-")[0].split(" ")[0]
    return base if re.match(r"^[WK][A-Z0-9]{2,4}$", base) else None


def _local_network(group_name):
    m = _LOCAL_RE.search(_fold(group_name or "").upper())
    return m.group(1) if m else None


# US broadcast-network brands used to group city-level local affiliates (DirecTV-style
# "CITY|" feeds) beyond the big four. Token after the CITY prefix -> clean group label.
# ABC/NBC/CBS/FOX are handled upstream (callsign-keyed via _local_network) so aren't here.
_CITY_NET_GROUP = {
    "CW": "CW", "PBS": "PBS",
    "TMO": "Telemundo", "TEL": "Telemundo", "TELE": "Telemundo", "TELEMUNDO": "Telemundo",
    "MNT": "MyNetworkTV", "MYTV": "MyNetworkTV", "MY": "MyNetworkTV", "MYNET": "MyNetworkTV",
    "UNI": "Univision", "UNV": "Univision", "UNIVISION": "Univision",
    "ION": "ION", "IND": "Independent", "INDIE": "Independent",
}


def _city_local_group(name):
    """For a DirecTV-style ``CITY|`` local-affiliate feed, return a clean NETWORK group
    label from the brand token right after the prefix (``CITY| CW KDAF ...`` -> ``CW``),
    or ``Independent`` for a bare-callsign feed with no network brand
    (``CITY| KICU SAN FRANCISCO`` -> ``Independent``). ``None`` when it isn't a CITY feed,
    so normal group handling applies. Grouping only — the channel's matching key is
    unchanged, so this re-groups in place with no renumbering."""
    ptoks, body = _prefix_tokens(_fold(name or ""))
    if not ptoks or ptoks[0] != "CITY":
        return None
    toks = _tokens(body)
    if not toks:
        return None
    first = toks[0]
    if first in _CITY_NET_GROUP:
        return _CITY_NET_GROUP[first]
    if re.match(r"^[WK][A-Z0-9]{2,4}$", first):  # bare callsign, no network brand
        return "Independent"
    return None


def _country_prefix(name):
    ptoks, _ = _prefix_tokens(_fold(name))
    return ptoks[0] if ptoks else None


def _is_non_latin(name):
    """True when the name is predominantly non-ASCII-Latin script (Arabic, Cyrillic,
    CJK, Hebrew…). Such channels are foreign regardless of any recognized prefix, and
    their distinctive text is lost by the Latin tokenizer (they'd collapse together)."""
    s = _fold(name)
    letters = [c for c in s if c.isalpha()]
    if not letters:
        return False
    non_ascii = sum(1 for c in letters if ord(c) > 127)
    return non_ascii >= max(2, len(letters) * 0.5)


# Quality-tier rank for intra-provider stream ordering (lower is tried first, so a
# standard HD/FHD feed is the default before a bandwidth-heavy 4K one).
def _quality_rank(name):
    toks = set(_tokens(_fold(name)))
    if "FHD" in toks:
        return 0
    if "HD" in toks:
        return 1
    if "4K" in toks or "UHD" in toks:
        return 4
    if "SD" in toks:
        return 5
    return 2


def _is_junk(name):
    n = _fold(name or "")
    if not re.search(r"[0-9A-Za-z]", n):
        return True
    # Labelled divider/placeholder, e.g. "##### FOX WISCONSIN #####" or
    # "## MAX ESPN HD/RAW 60fps ##" — providers bookend section headers with 2+ hashes.
    if re.search(r"#{2,}", n):
        return True
    return False


def _display_name(name, region_allow):
    """Human channel name: region prefix stripped, superscripts folded, kept legible."""
    s = _fold(name)
    ptoks, body = _prefix_tokens(s)
    if ptoks and ptoks[0] in region_allow:
        s = body
    s = re.sub(r"\s+", " ", s).strip()
    return s or (name or "").strip()


# Generic trailing words dropped from group labels when merge_group_suffixes is on,
# so providers that name the same category differently ("ABC NETWORK" vs "ABC") merge.
_GENERIC_GROUP_SUFFIX = {"NETWORK", "NETWORKS", "CHANNEL", "CHANNELS", "TV"}

# Default group aliases: providers split the same shelf by singular/plural
# ("US| SPORT" vs "US| SPORTS"), which becomes two adjacent guide categories after
# the prefix is stripped. Canonicalize the obvious, universally-safe case. Users add
# their own pairs via the "Merge equivalent group names" setting.
_GROUP_ALIAS_DEFAULT = "SPORT = SPORTS"


def _parse_group_aliases(raw):
    """Parse a user 'FROM = TO' / 'FROM -> TO' alias list (newline- or comma-separated)
    into ``{FROM_UPPERCASED: TO_verbatim}``. Deterministic, explicit — merges ONLY the
    pairs named, never guesses (no generic plural-folding, which would collide
    News/New, Kids/Kid, Movies/Movie). The lookup is case-insensitive; the TO value's
    casing is kept verbatim so the user controls the resulting label."""
    out = {}
    for line in re.split(r"[\r\n,]+", raw or ""):
        m = re.match(r"\s*(.+?)\s*(?:=|->|=>|:)\s*(.+?)\s*$", line)
        if not m:
            continue
        frm, to = m.group(1).strip(), m.group(2).strip()
        if frm and to:
            out[frm.upper()] = to
    return out


def _parse_subset_profiles(raw, reserved=()):
    """Parse subset-profile config into ``[(profile_name, frozenset(GROUPS_UPPER)), …]``.
    One lineup per line OR separated by ';':  ``name = GROUP, GROUP``. Each becomes an
    ADDITIVE channel profile (an HDHR/Plex lineup) holding only channels whose cleaned
    category group is in the list — the base lineup is never touched. A name colliding
    with the base/adult profile (``reserved``), or a line with no name/groups, is skipped;
    a repeated profile name is ignored after its first occurrence."""
    reserved = {r.lower() for r in reserved}
    out, seen = [], set()
    for line in re.split(r"[\r\n;]+", raw or ""):
        m = re.match(r"\s*(.+?)\s*[:=]\s*(.+?)\s*$", line)
        if not m:
            continue
        name = m.group(1).strip()
        groups = frozenset(g.strip().upper() for g in m.group(2).split(",") if g.strip())
        low = name.lower()
        if not name or not groups or low in reserved or low in seen:
            continue
        seen.add(low)
        out.append((name, groups))
    return out


def _group_display(gname, drop_suffix=False, aliases=None):
    """Clean a provider group name into a channel-group label for the IPTV client:
    strip ANY leading prefix (``US|``/``US:``/``18|``/``MU|`` …) so trex's ``US| PRIME``
    and strong's ``US: PRIME`` land in ONE group, fold superscripts, collapse spaces.
    Groups are the client's category navigation, so this preserves the provider's own
    taxonomy (News/Sports/Locals/24-7/…) instead of one giant bucket. With ``drop_suffix``
    a trailing generic word (NETWORK/CHANNEL/TV) is removed too, merging near-duplicates
    like ``ABC NETWORK`` (trex) with ``ABC`` (strong)."""
    s = _fold(gname or "").replace("\xa0", " ")
    ptoks, body = _prefix_tokens(s)
    if ptoks:  # strip whatever prefix the group carries (region OR provider tag)
        s = body
    # drop decoration that isn't a category: pure-quality words (RAW, 60FPS, UHD,
    # 3840P, 1920P …) and standalone symbol tokens (☼ ▶ ◉ ⚽). Use the same
    # quality-token test as the channel key (regex-based, so any `<res>P`/`<n>FPS`
    # is caught — not just the literal _QUALITY set), so a label like "RELAX 1920P"
    # and a decorated "RELAX ☼" both collapse to the same clean "RELAX" group.
    kept = []
    for w in s.split():
        subs = [t for t in re.split(r"[^0-9A-Za-z]+", w) if t]
        if not subs:
            continue  # token has no letters/digits — decorative glyph, not a word
        if all(_is_quality_token(t.upper()) for t in subs):
            continue
        kept.append(w)
    if drop_suffix:
        while len(kept) > 1 and kept[-1].upper() in _GENERIC_GROUP_SUFFIX:
            kept.pop()
    name = " ".join(kept).strip()
    if name:
        return (aliases or {}).get(name.upper(), name)  # canonicalize named equivalents
    # Nothing left = a group named purely by quality/resolution (e.g. "4K| UHD 3840P").
    # Give it a clean quality label instead of dumping it in the fallback bucket.
    up = _fold(gname or "").upper()
    if any(t in up for t in ("4K", "UHD", "2160", "3840")):
        return (aliases or {}).get("4K", "4K")
    return ""  # let the caller apply the configured fallback group name


def _ppv_teamtoks(side):
    return tuple(t for t in re.split(r"[^0-9A-Z]+", side.upper()) if t and t not in _PPV_STOP)


def _ppv_parse(name):
    """Parse a provider PPV/event stream name into
    {key, status, title} — or None for an idle/unusable slot.

    ``key`` is order-independent so the SAME event on two providers collides:
      * a "A vs B" matchup -> ``VS:<sorted team-token sides>``
      * otherwise the normalized descriptive title -> ``T:<tokens>``
    ``status`` is 'live' | 'done' | 'unknown' (from the leading segment); the caller
    filters. ``title`` is a clean human label for the channel."""
    if not name or _PPV_IDLE.search(name) or _is_junk(name):
        return None
    segs = [s.strip() for s in re.split(r"[|@]", name) if s.strip()]
    if not segs:
        return None
    first = segs[0]
    if _PPV_DONE.search(first):
        status = "done"
    elif _PPV_LIVE.search(first):
        status = "live"
    else:
        status = "unknown"

    # 1) matchup segment (contains "vs")
    vs_seg = next((s for s in segs if _PPV_VS.search(_fold(s).upper())), None)
    if vs_seg:
        folded = _fold(vs_seg).upper()
        parts = _PPV_VS.split(folded)
        if len(parts) >= 2:
            a, b = _ppv_teamtoks(parts[0]), _ppv_teamtoks(parts[-1])
            if a and b:
                canon = "|".join(sorted([" ".join(a), " ".join(b)]))
                title = re.sub(r"\s+", " ", vs_seg).strip()
                return {"key": "VS:" + canon, "status": status, "title": title}

    # 2) fallback: the most word-rich non-noise segment = the event title. Date/day/
    #    month/timezone tokens don't count toward identity (so a pure "Mon 31 Aug
    #    12:00 EDT" segment isn't mistaken for an event); real names still count.
    best, best_alpha = None, []
    for s in segs:
        if _PPV_PKG.search(s):  # a package/slot/quality trailer, not the event title
            continue
        toks = [t for t in re.split(r"[^0-9A-Za-z]+", _fold(s)) if t]
        alpha = [t for t in toks
                 if any(c.isalpha() for c in t) and t.upper() not in _PPV_STOP and t.upper() not in _PPV_DATE]
        if len(alpha) > len(best_alpha):
            best, best_alpha = s, alpha
    if best and len(best_alpha) >= 2:  # need a couple of real words to be an identity
        canon = " ".join(t.upper() for t in best_alpha)
        return {"key": "T:" + canon, "status": status, "title": re.sub(r"\s+", " ", best).strip()}
    return None


def _now():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


def _iso(dt):
    return dt.replace(microsecond=0).isoformat() + "Z"


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return False
    except Exception:
        return True


class Plugin:
    name = "Failovarr"
    version = __version__
    description = (
        "Consolidate N IPTV providers into unified channels with cross-provider "
        "failover, by deterministic normalized-name matching (no fuzzy). One channel "
        "per normalized name, both providers' streams stacked as failover, so a "
        "channel enabled on either provider is never lost. Locals key on callsign; "
        "PPV/event groups skipped; adult split; deterministic EPG match with free "
        "logos. In-place reconcile, safe to run daily."
    )
    author = "Roy Dufek"

    _cancel = threading.Event()
    _sched_thread = None
    _sched_stop = threading.Event()

    actions = [
        {
            "id": "preview",
            "label": "🔍 Preview (dry-run)",
            "description": "Compute exactly what the reconcile would change and report it. Writes nothing.",
            "button_label": "Preview",
            "button_variant": "outline",
        },
        {
            "id": "run",
            "label": "▶️ Reconcile now",
            "description": "Consolidate providers into unified channels with failover, in place (add/update/prune by normalized name).",
            "button_label": "Reconcile",
            "button_variant": "filled",
            "confirm": {
                "required": True,
                "title": "Run cross-provider reconcile?",
                "message": (
                    "Unions the configured providers into unified channels with "
                    "failover, updating in place — new channels added, stream "
                    "ordering refreshed, channels gone from every provider pruned. "
                    "A backup is taken first. Proceed?"
                ),
            },
        },
        {
            "id": "epg_match",
            "label": "📺 Match EPG + logos",
            "description": "Map channels to guide entries by name, then refresh (pulls schedules and applies logos).",
            "button_label": "Match EPG",
            "button_variant": "outline",
        },
        {
            "id": "ppv_preview",
            "label": "🥊 Preview PPV events (dry-run)",
            "description": "Show live/upcoming PPV events and which ones pair across providers. Writes nothing.",
            "button_label": "Preview PPV",
            "button_variant": "outline",
        },
        {
            "id": "ppv_refresh",
            "label": "🥊 Refresh PPV events now",
            "description": "With 'PPV events' ON: build/update live PPV event channels with failover and remove ended ones. With it OFF: remove all PPV channels (clean off-switch).",
            "button_label": "Refresh PPV",
            "button_variant": "filled",
        },
        {
            "id": "seed_reset",
            "label": "🌱 Seed / reset (rebuild from scratch)",
            "description": "Delete all Failovarr-owned channels and rebuild them fresh. For the initial build or a clean re-seed.",
            "button_label": "Seed / reset",
            "button_variant": "filled",
            "button_color": "red",
            "confirm": {
                "required": True,
                "title": "Delete and rebuild all Failovarr channels?",
                "message": (
                    "This DELETES every channel Failovarr owns and rebuilds them "
                    "from scratch. Channel numbers and manual EPG tweaks on those "
                    "channels are lost. A backup is taken first. Use only for the "
                    "initial seed or a deliberate clean rebuild. Proceed?"
                ),
            },
        },
        {
            "id": "view_last",
            "label": "📄 View last results",
            "description": "Show the report from the most recent run.",
            "button_label": "View",
            "button_variant": "subtle",
        },
        {
            "id": "clear_lock",
            "label": "🧹 Clear operation lock",
            "description": "Force-clear a stuck run lock if a previous run was interrupted.",
            "button_label": "Clear lock",
            "button_variant": "subtle",
        },
    ]

    def __init__(self):
        try:
            self._ensure_scheduler()
        except Exception:
            logger.debug("scheduler start deferred", exc_info=True)
        logger.info("[Failovarr] v%s initialized", __version__)

    # ------------------------------------------------------------------ fields
    @property
    def fields(self):
        accounts = []
        profiles = []
        try:
            accounts = list(
                M3UAccount.objects.filter(is_active=True)
                .values("id", "name", "account_type")
                .order_by("name")
            )
        except Exception:
            logger.debug("could not list M3U accounts for fields", exc_info=True)
        try:
            profiles = list(ChannelProfile.objects.values("name").order_by("name"))
        except Exception:
            logger.debug("could not list channel profiles for fields", exc_info=True)

        xc_names = [a["name"] for a in accounts if a["account_type"] == "XC"]
        priority_default = ",".join(xc_names)
        prof_names = [p["name"] for p in profiles]
        base_default = "failovarr" if "failovarr" in prof_names else (prof_names[0] if prof_names else "failovarr")
        adult_default = "failovarr+18" if "failovarr+18" in prof_names else base_default

        return [
            {
                "id": "provider_priority",
                "label": "Providers (comma-separated, priority order)",
                "type": "string",
                "default": priority_default,
                "placeholder": "trex,strong",
                "help_text": (
                    "The M3U accounts to union, in failover priority order. The first "
                    "is the primary (order 0); the rest attach as failover. Only "
                    "streams in each account's ENABLED groups are considered."
                ),
            },
            {
                "id": "skip_groups",
                "label": "Skip these groups (regex)",
                "type": "string",
                "default": DEFAULT_SKIP_GROUPS,
                "help_text": (
                    "Groups whose name matches are skipped entirely — PPV/event feeds "
                    "with dated one-off names. Hand those to Dispatcharr's native "
                    "auto-channel-sync, not Failovarr."
                ),
            },
            {
                "id": "include_247",
                "label": "Include 24/7 channels",
                "type": "boolean",
                "default": True,
                "help_text": "Keep 24/7 loop channels (good background watchables). Off skips groups starting '24/7'.",
            },
            {
                "id": "filter_foreign_country",
                "label": "Filter foreign-country channels",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "Drop streams whose name starts with a foreign country prefix "
                    "(CL/MX/DE/FR/…). Filters on country prefix, NOT language."
                ),
            },
            {
                "id": "keep_us_market",
                "label": "Always keep US-market channels",
                "type": "boolean",
                "default": True,
                "help_text": "Protect US|/EN| channels (incl. Spanish-language US networks) from the foreign filter.",
            },
            {
                "id": "region_allowlist",
                "label": "Region prefixes to strip (comma-separated)",
                "type": "string",
                "default": DEFAULT_REGION_ALLOW,
                "help_text": "Prefixes stripped from the matching key (kept for display). Other prefixes (GO/PRIME) stay in the key.",
            },
            {
                "id": "merge_quality_variants",
                "label": "Merge quality variants (HD/4K/fps → one channel)",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "On: pool every resolution/framerate variant of a channel into one "
                    "(fewer channels, more failover streams). Off: keep 4K and framerate "
                    "variants as their OWN channels — HD is treated as the baseline (so an "
                    "'HD' feed and an untagged feed still pair across providers), while 4K, "
                    "SD and 60/50/25 fps each get their own cross-provider channel."
                ),
            },
            {
                "id": "skip_junk_names",
                "label": "Skip junk / divider names",
                "type": "boolean",
                "default": True,
                "help_text": "Skip all-symbol dividers like '##### FOX WISCONSIN #####'.",
            },
            {
                "id": "adult_profile_split",
                "label": "Split adult channels to a +18 profile",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "Adult channels go to the adult profile ONLY; everything else goes "
                    "to both the base and adult profiles (the adult profile is the "
                    "superset). Turn off to put everything in the base profile."
                ),
            },
            {
                "id": "base_profile",
                "label": "Base (family-safe) profile name",
                "type": "select",
                "options": [{"value": n, "label": n} for n in prof_names] or [{"value": base_default, "label": base_default}],
                "default": base_default,
                "help_text": "The profile that holds everything EXCEPT adult channels.",
            },
            {
                "id": "adult_profile",
                "label": "Adult (+18) profile name",
                "type": "select",
                "options": [{"value": n, "label": n} for n in prof_names] or [{"value": adult_default, "label": adult_default}],
                "default": adult_default,
                "help_text": "The profile that holds every channel including adult (the superset).",
            },
            {
                "id": "adult_detect",
                "label": "Adult group detection (regex)",
                "type": "string",
                "default": DEFAULT_ADULT_DETECT,
                "help_text": "Group names matching this are treated as adult (e.g. '18| FOR ADULTS').",
            },
            {
                "id": "subset_profiles",
                "label": "Subset profiles (HDHR / Plex lineups)",
                "type": "string",
                "default": "",
                "help_text": (
                    "Curated additive lineups for HDHR clients (Plex/Emby/Jellyfin). One "
                    "per line (or separated by ';'): 'name = GROUP, GROUP'. Each becomes a "
                    "channel profile holding ONLY channels in those (cleaned) category "
                    "groups — your base lineup is never touched. Point the client's HDHR "
                    "DVR at /hdhr/<name>/discover.json. Example: 'plex = ENTERTAINMENT'. "
                    "Group names are case-insensitive; blank disables the feature."
                ),
            },
            {
                "id": "epg_match",
                "label": "Match EPG guide + logos",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "After consolidating, map each channel to a real guide entry by "
                    "name and refresh — pulls schedules and applies channel logos."
                ),
            },
            {
                "id": "respect_manual_epg",
                "label": "Respect manual EPG mappings",
                "type": "boolean",
                "default": True,
                "help_text": "Don't overwrite a channel that already has an EPG guide assigned (protects hand-fixed guides).",
            },
            {
                "id": "channel_number_start",
                "label": "First channel number",
                "type": "number",
                "default": 1,
                "min": 0,
                "help_text": "Channels are numbered sequentially from here. Numbers are stable across reconciles.",
            },
            {
                "id": "channel_group_name",
                "label": "Fallback channel group name",
                "type": "string",
                "default": "Failovarr",
                "help_text": "Used only when a channel's provider group can't be determined. Normally each channel takes its primary provider's (cleaned) category group.",
            },
            {
                "id": "merge_group_suffixes",
                "label": "Tidy group names (drop trailing NETWORK / CHANNEL / TV)",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "Merge near-duplicate categories that providers name differently — "
                    "e.g. 'ABC NETWORK' (one provider) and 'ABC' (another) become one "
                    "'ABC' group. Off keeps each provider's exact group wording."
                ),
            },
            {
                "id": "group_aliases",
                "label": "Merge equivalent group names",
                "type": "string",
                "default": _GROUP_ALIAS_DEFAULT,
                "help_text": (
                    "Fold categories that mean the same thing into one guide group. One "
                    "'FROM = TO' pair per line (or comma-separated), case-insensitive on "
                    "the left. Applied to the cleaned label (after the 'US|' prefix is "
                    "stripped), so 'SPORT = SPORTS' puts a provider's 'US| SPORT' channels "
                    "into the 'SPORTS' group. Only the pairs you list are merged — nothing "
                    "is guessed. Clear the box to keep every group exactly as-is."
                ),
            },
            {
                "id": "locals_by_name",
                "label": "Detect local stations by name (merge affiliates across packages)",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "Normally locals are matched by callsign only inside the "
                    "ABC/NBC/CBS/FOX network groups. With this on, a channel whose NAME "
                    "carries a network + callsign is treated as that local no matter "
                    "which group it's in — so the same affiliate packaged twice (e.g. an "
                    "'ABC NETWORK' feed and a 'DirecTV city' feed of WSB Atlanta) merges "
                    "into one channel by callsign. Off = groups-only detection."
                ),
            },
            {
                "id": "skip_stale",
                "label": "Skip dead/stale streams",
                "type": "boolean",
                "default": True,
                "help_text": "Ignore streams Dispatcharr has flagged stale, so you never attach a dead failover.",
            },
            {
                "id": "ppv_events",
                "label": "PPV events with cross-provider failover (experimental)",
                "type": "boolean",
                "default": False,
                "help_text": (
                    "Surface live/upcoming PPV events (skipped by normal consolidation) "
                    "as channels, merging the SAME event across providers into one "
                    "failover channel so it doesn't drop mid-event. Events churn fast — "
                    "they refresh on their own cadence below and are kept separate from "
                    "your stable channels. Off by default."
                ),
            },
            {
                "id": "ppv_skip",
                "label": "PPV categories to skip (regex)",
                "type": "string",
                "default": _PPV_SKIP_DEFAULT,
                "help_text": (
                    "PPV packages to leave out. The default is light — only clear junk "
                    "(high-school, per-team-duplicate feeds, 24/7, a dead service, foreign). "
                    "Racing, college and minor-league are KEPT. Curate to taste here, or "
                    "just disable the PPV groups you don't want in Dispatcharr (Failovarr "
                    "only mines enabled groups)."
                ),
            },
            {
                "id": "ppv_min_providers",
                "label": "PPV: minimum providers per event",
                "type": "number",
                "default": 1,
                "min": 1,
                "help_text": (
                    "1 = every live event from any provider (marquee events are often on "
                    "just one provider, so keep this at 1 to see them). 2 = only events on "
                    "BOTH providers, i.e. cross-provider failover pairs — but those skew "
                    "toward niche feeds both resell, so 2 hides most premium events."
                ),
            },
            {
                "id": "ppv_groups",
                "label": "PPV / event group match (regex)",
                "type": "string",
                "default": _PPV_DEFAULT_GROUPS,
                "help_text": "Which provider groups hold the PPV/event slots to mine for live events.",
            },
            {
                "id": "ppv_schedule_minutes",
                "label": "Extra PPV refresh interval (minutes, 0 = daily only)",
                "type": "number",
                "default": 0,
                "min": 0,
                "help_text": (
                    "PPV events refresh once with the daily reconcile by default (0) — a "
                    "once-a-day IPTV client only sees a daily snapshot anyway, and "
                    "cross-provider failover works during playback with no refresh. Only "
                    "set this (e.g. 120–360) if your client pulls the playlist several "
                    "times a day and you want fresher event listings."
                ),
            },
            {
                "id": "ppv_number_start",
                "label": "First PPV channel number",
                "type": "number",
                "default": 90000,
                "min": 0,
                "help_text": "PPV event channels are numbered from here — a high range so they cluster together and never collide with your stable channels.",
            },
            {
                "id": "schedule_time",
                "label": "Daily reconcile time (HH:MM, UTC — blank = off)",
                "type": "string",
                "default": "",
                "placeholder": "10:00",
                "help_text": "Server time is UTC. 10:00 UTC ≈ 3:00 AM US-Pacific. Blank disables. Retries for 6h on failure. Never seeds/wipes — reconcile only.",
            },
            {
                "id": "gotify_notify",
                "label": "Gotify notification for scheduled runs",
                "type": "select",
                "options": [
                    {"value": "off", "label": "Off"},
                    {"value": "on_failure", "label": "On failure only"},
                    {"value": "on_change", "label": "On change (added/pruned/updated) or failure"},
                    {"value": "on_completion", "label": "On every completion"},
                ],
                "default": "off",
                "help_text": (
                    "Notify a Gotify endpoint after the daily scheduled run. 'On change' "
                    "pings you only when the run actually added, pruned or re-grouped "
                    "channels (e.g. a provider added a new group) — or if it failed."
                ),
            },
            {
                "id": "gotify_server_url",
                "label": "Gotify server URL",
                "type": "string",
                "default": "",
                "placeholder": "https://gotify.example.com",
                "help_text": "Base URL of your Gotify server (no path).",
            },
            {
                "id": "gotify_token",
                "label": "Gotify app token",
                "type": "string",
                "input_type": "password",
                "default": "",
                "placeholder": "A…",
                "help_text": "Your Gotify application token. Stored in the DB, never committed.",
            },
            {
                "id": "health_alert",
                "label": "Channel-drop safeguard (recommended)",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "If a reconcile would suddenly prune most of your channels (the "
                    "signature of a provider outage returning an empty list), skip the "
                    "pruning and send a HIGH-priority Gotify alert even if notifications "
                    "are Off. A deliberate large cleanup goes through on the next run."
                ),
            },
        ]

    # --------------------------------------------------------------- dispatch
    def run(self, action, params, context):
        settings = (context or {}).get("settings", {}) or {}
        if action == "view_last":
            return self._action_view_last()
        if action == "clear_lock":
            return self._action_clear_lock()
        if action in ("preview", "run"):
            return self._action_start("reconcile", dry_run=(action == "preview"), settings=settings)
        if action == "seed_reset":
            return self._action_start("seed", dry_run=False, settings=settings)
        if action == "epg_match":
            return self._action_start("epg", dry_run=False, settings=settings)
        if action in ("ppv_preview", "ppv_refresh"):
            return self._action_start("ppv", dry_run=(action == "ppv_preview"), settings=settings)
        if action == "stop":
            self._cancel.set()
            return {"status": "ok", "message": "Cancellation requested."}
        return {"status": "error", "message": f"Unknown action: {action}"}

    def stop(self, context=None):
        self._cancel.set()
        self._sched_stop.set()

    # --------------------------------------------------------------- actions
    def _action_start(self, job_kind, dry_run, settings):
        if not self._acquire_lock():
            return {
                "status": "error",
                "message": "A run is already in progress (locked). Use 'Clear lock' if it's stuck.",
            }
        self._cancel.clear()
        # Run synchronously and return the real result. Dispatcharr runs on
        # uWSGI + gevent (early monkey-patch); a greenlet spawned from the request
        # handler and left detached is NOT reliably scheduled once the response is
        # sent. These jobs take seconds and uWSGI http-timeout is 600s, so inline
        # execution is safe and hands the UI the actual result immediately.
        try:
            if job_kind == "seed":
                report = self._run_job(dry_run=False, settings=dict(settings), mode="seed")
            elif job_kind == "epg":
                report = self._epg_only(dict(settings))
            elif job_kind == "ppv":
                report = self._ppv_job(dry_run=dry_run, settings=dict(settings))
            else:
                report = self._run_job(dry_run=dry_run, settings=dict(settings), mode="reconcile")
            return {
                "status": report.get("status", "ok"),
                "message": report.get("message", "Done."),
                "result": report,
            }
        except Exception as exc:
            logger.exception("failovarr job failed")
            err = {
                "status": "error",
                "dry_run": dry_run,
                "finished": _iso(_now()),
                "error": str(exc),
                "message": f"Run failed: {exc}",
            }
            self._write_status(err)
            send_websocket_update(
                "updates", "update",
                {"type": "failovarr", "status": "error", "message": str(exc)},
            )
            return err
        finally:
            close_old_connections()
            self._release_lock()

    def _action_view_last(self):
        data = self._read_status()
        if not data:
            return {"status": "ok", "message": "No runs recorded yet."}
        return {"status": "ok", "message": self._format_report(data), "result": data}

    def _action_clear_lock(self):
        existed = os.path.exists(RUN_LOCK)
        self._release_lock()
        self._cancel.clear()
        return {"status": "ok", "message": "Lock cleared." if existed else "No lock was held."}

    # --------------------------------------------------------- cross-proc lock
    def _acquire_lock(self):
        os.makedirs(SCHED_DIR, exist_ok=True)
        payload = f"{os.getpid()}|{time.time()}".encode()
        for _ in range(3):
            try:
                fd = os.open(RUN_LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, payload)
                os.close(fd)
                return True
            except FileExistsError:
                if not self._lock_is_stale():
                    return False
                try:
                    os.remove(RUN_LOCK)
                    logger.warning("[Failovarr] removing a stale run lock")
                except FileNotFoundError:
                    pass
                except Exception:
                    return False
        return False

    def _lock_is_stale(self):
        try:
            with open(RUN_LOCK, "r") as fh:
                pid_s, ts_s = fh.read().strip().split("|")
            pid, ts = int(pid_s), float(ts_s)
        except Exception:
            return True
        if (time.time() - ts) > LOCK_STALE_SECS:
            return True
        return not _pid_alive(pid)

    def _release_lock(self):
        try:
            os.remove(RUN_LOCK)
        except FileNotFoundError:
            pass
        except Exception:
            logger.debug("could not remove run lock", exc_info=True)

    # =================================================================== engine
    def _run_job(self, dry_run, settings, mode="reconcile"):
        """Consolidate providers -> unified channels. mode: reconcile | seed."""
        started = _now()
        providers = self._resolve_providers(settings)
        region_allow = self._region_allow(settings)
        cfg = self._engine_cfg(settings, region_allow)

        self._write_status({
            "status": "running", "mode": mode, "dry_run": dry_run,
            "started": _iso(started),
            "providers": [p.name for p in providers],
            "message": "Gathering streams and computing keys…",
        })

        buckets, gstats = self._gather(providers, cfg)

        # Ownership snapshot BEFORE any change (owned = members of the two profiles).
        base_prof, adult_prof = self._get_profiles(settings)
        owned_before = self._owned_channel_ids(base_prof, adult_prof)

        keymap = {} if mode == "seed" else self._read_keymap()
        # Drop keymap entries whose channel was deleted externally.
        if keymap:
            live = set(
                Channel.objects.filter(id__in=[v["id"] for v in keymap.values()])
                .values_list("id", flat=True)
            )
            keymap = {k: v for k, v in keymap.items() if v["id"] in live}

        desired = set(buckets)
        existing = set(keymap)
        to_create = sorted(desired - existing)
        to_prune = sorted(existing - desired)
        common = desired & existing

        # Channel-collapse safeguard — measured on THIS run's computed diff, not a
        # lagging owned count. A provider outage returns an empty/tiny stream set, so
        # `desired` collapses and `to_prune` becomes most of the existing channels;
        # refuse to prune and alert. A collapse that PERSISTS to the next run is treated
        # as a real reduction (an outage would have recovered by then) and let through,
        # so a deliberate large cleanup is never blocked forever.
        existing_n = len(existing)
        collapse = existing_n >= HEALTH_MIN_BASELINE and len(to_prune) > existing_n * HEALTH_DROP_FRAC
        already_alerted = bool(self._read_health().get("alerted", False))
        prune_blocked = bool(collapse and not already_alerted and mode != "seed")
        if not dry_run:
            # Next-state 'alerted' is True only on a fresh collapse we're blocking;
            # a persisted collapse (accepted) or no-collapse resets it.
            self._write_health({
                "alerted": bool(collapse and not already_alerted),
                "existing": existing_n, "to_prune": len(to_prune),
                "desired": len(buckets), "updated": _iso(_now()),
            })
        health = {
            "existing": existing_n, "desired": len(buckets), "to_prune": len(to_prune),
            "collapse": collapse, "blocked": prune_blocked, "alert": prune_blocked,
        }

        report = {
            "status": "done", "mode": mode, "dry_run": dry_run,
            "started": _iso(started), "finished": _iso(_now()),
            "providers": [p.name for p in providers],
            "health": health,
            "stats": {
                "streams_scanned": gstats["scanned"],
                "streams_skipped_event": gstats["skip_event"],
                "streams_skipped_foreign": gstats["skip_foreign"],
                "streams_skipped_junk": gstats["skip_junk"],
                "streams_skipped_stale": gstats["skip_stale"],
                "keys_total": len(buckets),
                "failover_pairs": gstats["pairs"],
                "single_source": gstats["single"],
                "adult": gstats["adult"],
                "locals_by_callsign": gstats["locals"],
                "channels_existing": len(existing),
                "channels_to_create": len(to_create),
                "channels_to_prune": len(to_prune),
            },
            "samples": {
                "create_examples": [buckets[k]["display"] for k in to_create[:20]],
                "prune_examples": [keymap[k]["id"] for k in to_prune[:20]],
            },
        }

        if dry_run:
            report["message"] = (
                f"DRY-RUN: {len(buckets)} unified channels from {len(providers)} providers "
                f"({gstats['pairs']} failover pairs, {gstats['single']} single-source, "
                f"{gstats['adult']} adult). Would create {len(to_create)}, prune "
                f"{len(to_prune)}, keep {len(common)}. Nothing written."
            )
            self._write_status(report)
            logger.info("[Failovarr] %s", report["message"])
            self._ws_done(report["message"], dry_run=True)
            return report

        # ---- real write path ----
        backup_path = self._backup(owned_before, keymap)

        base_prof, adult_prof = self._get_profiles(settings)
        subsets = self._get_subset_profiles(cfg)  # additive HDHR/Plex lineups
        gcache = {}  # cleaned group name -> ChannelGroup (built lazily)

        with transaction.atomic():
            if mode == "seed" and owned_before:
                # Sanctioned wipe: delete every owned channel, rebuild fresh.
                for chunk in _chunks(list(owned_before), 2000):
                    Channel.objects.filter(id__in=chunk).delete()
                keymap = {}
                existing = set()
                to_create = sorted(desired)
                to_prune = []
                common = set()

            next_num = self._next_number(cfg["number_start"], keymap)

            # CREATE new channels.
            created = self._create_channels(
                to_create, buckets, gcache, base_prof, adult_prof, cfg, keymap, next_num, subsets
            )
            report["stats"]["created"] = created

            # UPDATE existing channels (stream order + membership + adult flag + group).
            updated = 0
            if common:
                updated = self._update_channels(
                    common, buckets, keymap, base_prof, adult_prof, cfg, gcache, subsets
                )
            report["stats"]["updated"] = updated

            # PRUNE keys gone from every provider (unless a collapse is suspected).
            pruned = 0
            if to_prune and not prune_blocked:
                gone_ids = [keymap[k]["id"] for k in to_prune]
                for chunk in _chunks(gone_ids, 2000):
                    Channel.objects.filter(id__in=chunk).delete()
                for k in to_prune:
                    keymap.pop(k, None)
                pruned = len(gone_ids)
            report["stats"]["pruned"] = pruned
            report["stats"]["prune_blocked"] = prune_blocked

        # Persist the anchor right after commit. A write failure is logged loudly (disk
        # error); the only residual gap is a hard process-kill in the millisecond window
        # between commit and this write, which could orphan freshly-created channels from
        # the keymap and duplicate them next run — rare, and fixed by a re-seed.
        self._write_keymap(keymap)

        # EPG match (own transaction / external refresh).
        epg = None
        if cfg["epg_match"]:
            epg = self._epg_match(buckets, keymap, settings, cfg)
            report["epg"] = epg

        report["backup"] = backup_path
        report["finished"] = _iso(_now())
        report["message"] = (
            f"{'Seeded' if mode == 'seed' else 'Reconciled'}: {report['stats'].get('created', 0)} created, "
            f"{report['stats'].get('updated', 0)} updated, {report['stats'].get('pruned', 0)} pruned "
            f"({len(buckets)} channels total: {gstats['pairs']} failover pairs, "
            f"{gstats['single']} single-source, {gstats['adult']} adult)."
        )
        if epg:
            report["message"] += f"  EPG: {epg.get('matched', 0)} mapped."
        if prune_blocked:
            report["message"] += (
                f"  ⚠️ Channel-drop suspected — pruning of {len(to_prune)} channels "
                "was SKIPPED as a precaution."
            )
        self._write_status(report)
        logger.info("[Failovarr] %s (backup: %s)", report["message"], backup_path or "none")
        if health.get("alert") and bool(settings.get("health_alert", True)):
            self._emergency_alert(settings, health)
        self._ws_done(report["message"])
        send_websocket_update("updates", "update", {"type": "channels_refresh"})
        return report

    # ------------------------------------------------------------ gather/bucket
    def _gather(self, providers, cfg):
        """Walk each provider's enabled groups, classify, and union by key.

        Returns (buckets, stats). buckets[key] = {
            display, is_adult, epg_key, callsign,
            prov: {provider_idx: [stream_pk, …]},
        }
        """
        buckets = {}
        stats = {
            "scanned": 0, "skip_event": 0, "skip_foreign": 0, "skip_junk": 0,
            "skip_stale": 0, "pairs": 0, "single": 0, "adult": 0, "locals": 0,
        }
        skip_re = _compile(cfg["skip_re"])
        adult_re = _compile(cfg["adult_re"])
        region_allow = cfg["region_allow"]

        for idx, acct in enumerate(providers):
            gid_name = dict(
                ChannelGroup.objects.filter(
                    id__in=ChannelGroupM3UAccount.objects.filter(
                        m3u_account=acct, enabled=True
                    ).values_list("channel_group_id", flat=True)
                ).values_list("id", "name")
            )
            if not gid_name:
                continue
            qs = Stream.objects.filter(m3u_account=acct, channel_group_id__in=list(gid_name))
            if cfg["skip_stale"]:
                qs = qs.exclude(is_stale=True)
            for pk, name, gid, s_adult in qs.values_list("id", "name", "channel_group_id", "is_adult").iterator():
                stats["scanned"] += 1
                gname = gid_name.get(gid, "") or ""

                # Skip PPV/event GROUPS (by group name only — matching the stream
                # name too would wrongly drop legit channels that happen to contain
                # a token like EVENT/8K).
                if skip_re and skip_re.search(gname):
                    stats["skip_event"] += 1
                    continue
                if not cfg["include_247"] and _fold(gname).lstrip().upper().startswith(("24/7", "24 7", "247")):
                    stats["skip_event"] += 1
                    continue
                if cfg["skip_junk"] and _is_junk(name):
                    stats["skip_junk"] += 1
                    continue
                if cfg["filter_foreign"]:
                    cp = _country_prefix(name)
                    protected = cfg["keep_us_market"] and cp in region_allow
                    if not protected and (cp in _FOREIGN or _is_non_latin(name)):
                        stats["skip_foreign"] += 1
                        continue

                is_adult = bool(s_adult) or bool(adult_re and adult_re.search(gname))

                # Local station? key on callsign (network + callsign). Detect the
                # network from the GROUP (ABC/NBC/CBS/FOX network groups) and — when
                # locals_by_name is on — from the channel NAME too, so the same
                # affiliate packaged under a different group (e.g. a 'DirecTV city'
                # feed) still merges by callsign.
                key = None
                callsign = None
                local_net = None
                net = _local_network(gname)
                if not net and cfg["locals_by_name"]:
                    net = _local_network(name)
                if net:
                    callsign = _callsign(name)
                    if callsign:
                        key = f"{net} {callsign}"
                        local_net = net  # group locals under their network, not the package
                if key is None:
                    key = _consolidation_key(name, region_allow, cfg["merge_quality"])
                if not key:
                    stats["skip_junk"] += 1
                    continue

                # Group: a big-four local sits under its NETWORK; a DirecTV-style CITY|
                # affiliate (CW/PBS/Telemundo/MyNetworkTV/Independent…) is grouped under
                # its network too (name-detected, key unchanged — re-groups in place);
                # everything else takes its primary stream's cleaned provider category.
                if local_net:
                    grp = local_net
                else:
                    grp = (cfg["locals_by_name"] and _city_local_group(name)) or \
                        _group_display(gname, cfg["merge_group_suffixes"], cfg["group_aliases"])

                b = buckets.get(key)
                if b is None:
                    b = buckets[key] = {
                        "display": _display_name(name, region_allow),
                        "is_adult": is_adult,
                        "epg_key": _epg_key(name),
                        "callsign": callsign,
                        "group": grp,
                        "prov": {},
                    }
                else:
                    b["is_adult"] = b["is_adult"] or is_adult
                b["prov"].setdefault(idx, []).append((_quality_rank(name), pk))

        for b in buckets.values():
            if len(b["prov"]) > 1:
                stats["pairs"] += 1
            else:
                stats["single"] += 1
            if b["is_adult"]:
                stats["adult"] += 1
            if b.get("callsign"):  # local station keyed on callsign
                stats["locals"] += 1
        return buckets, stats

    def _ordered_streams(self, bucket, nproviders):
        """Final failover order: provider 0 first, then 1, 2, …; within a provider,
        by quality tier (HD/FHD before 4K) then stream id for determinism."""
        out = []
        for idx in range(nproviders):
            for _rank, pk in sorted(bucket["prov"].get(idx, [])):
                out.append(pk)
        return out

    # -------------------------------------------------------------- create
    def _group_obj(self, name, cache, cfg):
        name = (name or cfg.get("group_name") or "Failovarr").strip() or "Failovarr"
        g = cache.get(name)
        if g is None:
            g, _ = ChannelGroup.objects.get_or_create(name=name)
            cache[name] = g
        return g

    def _create_channels(self, keys, buckets, gcache, base_prof, adult_prof, cfg, keymap, next_num, subsets=()):
        if not keys:
            return 0
        nprov = cfg["nproviders"]
        split = cfg["adult_split"]
        rows = []
        order = []  # parallel: (key, is_adult, streams)
        num = next_num
        for k in keys:
            b = buckets[k]
            streams = self._ordered_streams(b, nprov)
            if not streams:
                continue
            rows.append(Channel(
                name=b["display"][:255] or k,
                channel_number=float(num),
                channel_group=self._group_obj(b.get("group"), gcache, cfg),
                is_adult=b["is_adult"],
            ))
            order.append((k, b["is_adult"], streams, num))
            num += 1

        created_objs = Channel.objects.bulk_create(rows, batch_size=1000)

        cs_rows = []
        mem_rows = []
        for ch, (k, is_adult, streams, cnum) in zip(created_objs, order):
            # store the computed display in the keymap so reconcile can tell an
            # untouched auto-name from one the user has manually renamed.
            keymap[k] = {"id": ch.id, "num": cnum, "name": ch.name}
            for o, spk in enumerate(streams):
                cs_rows.append(ChannelStream(channel_id=ch.id, stream_id=spk, order=o))
            # Membership: adult profile always; base only for non-adult.
            mem_rows.append(ChannelProfileMembership(channel_profile=adult_prof, channel_id=ch.id, enabled=True))
            if not (split and is_adult):
                mem_rows.append(ChannelProfileMembership(channel_profile=base_prof, channel_id=ch.id, enabled=True))
            # Additive subset lineups: also add to any subset profile whose group list matches.
            grp_up = (buckets[k]["group"] or "").upper()
            for subprof, sgroups in subsets:
                if grp_up in sgroups:
                    mem_rows.append(ChannelProfileMembership(channel_profile=subprof, channel_id=ch.id, enabled=True))

        for chunk in _chunks(cs_rows, 2000):
            ChannelStream.objects.bulk_create(chunk, ignore_conflicts=True)
        for chunk in _chunks(mem_rows, 2000):
            ChannelProfileMembership.objects.bulk_create(chunk, ignore_conflicts=True)
        return len(created_objs)

    # -------------------------------------------------------------- update
    def _update_channels(self, keys, buckets, keymap, base_prof, adult_prof, cfg, gcache, subsets=()):
        """Sync stream order, membership, adult flag and channel group for existing channels."""
        nprov = cfg["nproviders"]
        split = cfg["adult_split"]
        ch_ids = [keymap[k]["id"] for k in keys]

        # current streams per channel
        cur = {}
        for ch_id, spk, o, cs_id in (
            ChannelStream.objects.filter(channel_id__in=ch_ids)
            .values_list("channel_id", "stream_id", "order", "id").iterator()
        ):
            cur.setdefault(ch_id, {})[spk] = (o, cs_id)

        # current adult flag + group + name per channel
        info_now = {
            cid: (ad, gid, nm)
            for cid, ad, gid, nm in Channel.objects.filter(id__in=ch_ids)
            .values_list("id", "is_adult", "channel_group_id", "name")
        }
        mem_now = set()
        managed_pids = [base_prof.id, adult_prof.id] + [sp.id for sp, _ in subsets]
        for pid, cid in ChannelProfileMembership.objects.filter(
            channel_id__in=ch_ids, channel_profile_id__in=managed_pids
        ).values_list("channel_profile_id", "channel_id"):
            mem_now.add((pid, cid))

        to_create_cs, to_update_cs, to_delete_cs = [], [], []
        adult_flip = []
        group_flip = []
        name_flip = []
        mem_add = []
        changed = 0

        for k in keys:
            ch_id = keymap[k]["id"]
            b = buckets[k]
            streams = self._ordered_streams(b, nprov)
            existing = cur.get(ch_id, {})
            desired_set = set(streams)
            local_changed = False

            for o, spk in enumerate(streams):
                if spk in existing:
                    cur_o, cs_id = existing[spk]
                    if cur_o != o:
                        to_update_cs.append(ChannelStream(id=cs_id, order=o))
                        local_changed = True
                else:
                    to_create_cs.append(ChannelStream(channel_id=ch_id, stream_id=spk, order=o))
                    local_changed = True
            for spk, (cur_o, cs_id) in existing.items():
                if spk not in desired_set:
                    to_delete_cs.append(cs_id)
                    local_changed = True

            cur_adult, cur_gid, cur_name = info_now.get(ch_id, (None, None, None))
            if cur_adult != b["is_adult"]:
                adult_flip.append((ch_id, b["is_adult"]))
                local_changed = True
            desired_g = self._group_obj(b.get("group"), gcache, cfg)
            if cur_gid != desired_g.id:
                group_flip.append((ch_id, desired_g.id))
                local_changed = True
            # Refresh the display name if the channel's key contents changed it — but
            # ONLY when the user hasn't manually renamed it (current name still equals
            # the last name we computed, tracked in the keymap; absent = migrate).
            desired_name = (b["display"] or k)[:255]
            stored_name = keymap[k].get("name")
            if stored_name is None:
                # First time tracking this key's name (pre-name-tracking keymap):
                # adopt the current name as the baseline WITHOUT renaming, so a manual
                # rename made before tracking existed is never clobbered. If it's
                # actually a stale auto-name, the next run (stored == current) fixes it.
                keymap[k]["name"] = cur_name
            elif stored_name == cur_name and cur_name != desired_name:
                # Untouched since we set it (auto-owned) and now stale -> refresh.
                name_flip.append((ch_id, desired_name))
                keymap[k]["name"] = desired_name
                local_changed = True
            # else: user manually renamed it -> leave it alone

            if (adult_prof.id, ch_id) not in mem_now:
                mem_add.append(ChannelProfileMembership(channel_profile=adult_prof, channel_id=ch_id, enabled=True))
                local_changed = True
            want_base = not (split and b["is_adult"])
            has_base = (base_prof.id, ch_id) in mem_now
            if want_base and not has_base:
                mem_add.append(ChannelProfileMembership(channel_profile=base_prof, channel_id=ch_id, enabled=True))
                local_changed = True
            elif (not want_base) and has_base:
                ChannelProfileMembership.objects.filter(
                    channel_profile=base_prof, channel_id=ch_id
                ).delete()
                local_changed = True

            # Additive subset lineups: membership tracks whether the channel's group is listed.
            grp_up = (b["group"] or "").upper()
            for subprof, sgroups in subsets:
                want = grp_up in sgroups
                has = (subprof.id, ch_id) in mem_now
                if want and not has:
                    mem_add.append(ChannelProfileMembership(channel_profile=subprof, channel_id=ch_id, enabled=True))
                    local_changed = True
                elif has and not want:
                    ChannelProfileMembership.objects.filter(
                        channel_profile=subprof, channel_id=ch_id
                    ).delete()
                    local_changed = True

            if local_changed:
                changed += 1

        if to_delete_cs:
            for chunk in _chunks(to_delete_cs, 5000):
                ChannelStream.objects.filter(id__in=chunk).delete()
        if to_create_cs:
            for chunk in _chunks(to_create_cs, 2000):
                ChannelStream.objects.bulk_create(chunk, ignore_conflicts=True)
        if to_update_cs:
            ChannelStream.objects.bulk_update(to_update_cs, ["order"], batch_size=2000)
        if mem_add:
            for chunk in _chunks(mem_add, 2000):
                ChannelProfileMembership.objects.bulk_create(chunk, ignore_conflicts=True)
        for ch_id, val in adult_flip:
            Channel.objects.filter(id=ch_id).update(is_adult=val)
        for ch_id, val in name_flip:
            Channel.objects.filter(id=ch_id).update(name=val)
        if group_flip:
            by_g = {}
            for ch_id, gid in group_flip:
                by_g.setdefault(gid, []).append(ch_id)
            for gid, ids in by_g.items():
                for chunk in _chunks(ids, 2000):
                    Channel.objects.filter(id__in=chunk).update(channel_group_id=gid)
        return changed

    # -------------------------------------------------------------- EPG
    def _epg_only(self, settings):
        started = _now()
        region_allow = self._region_allow(settings)
        cfg = self._engine_cfg(settings, region_allow)
        providers = self._resolve_providers(settings)
        buckets, _ = self._gather(providers, cfg)
        keymap = self._read_keymap()
        epg = self._epg_match(buckets, keymap, settings, cfg)
        report = {
            "status": "done", "mode": "epg", "dry_run": False,
            "started": _iso(started), "finished": _iso(_now()),
            "epg": epg,
            "message": f"EPG: mapped {epg.get('matched', 0)} channels, refresh triggered.",
        }
        self._write_status(report)
        self._ws_done(report["message"])
        return report

    def _all_epg_sources(self):
        """Every EPG source that has real (non-dummy) entries, most-entries first.
        Matching across ALL sources means adding a second guide (e.g. a US-locals
        XMLTV) lifts coverage automatically — no code change needed."""
        from apps.epg.models import EPGSource, EPGData
        rows = []
        for src in EPGSource.objects.all():
            n = EPGData.objects.filter(epg_source=src).exclude(tvg_id__startswith="dummy").count()
            if n > 0:
                rows.append((n, src))
        rows.sort(key=lambda r: -r[0])
        return [src for _n, src in rows]

    def _build_epg_index(self, sources):
        """name-key -> (epg_data_id, tvg_id, source_id). Real entries only. On a key
        collision prefer a ``.us`` id, then the higher-priority (more-entries) source
        — which is iterated first, so it wins ties naturally."""
        from apps.epg.models import EPGData
        index = {}
        for src in sources:  # best source first
            for eid, tvg_id, ename in (
                EPGData.objects.filter(epg_source=src)
                .exclude(tvg_id__startswith="dummy")
                .values_list("id", "tvg_id", "name").iterator()
            ):
                key = _epg_key(ename)
                if not key:
                    continue
                prev = index.get(key)
                if prev is None:
                    index[key] = (eid, tvg_id, src.id)
                elif str(tvg_id or "").lower().endswith(".us") and not str(prev[1] or "").lower().endswith(".us"):
                    index[key] = (eid, tvg_id, src.id)
                # Also index a bare-callsign name (US-locals guides key by callsign),
                # so a local channel can match on its callsign alone. Don't overwrite an
                # existing key (a real named entry wins).
                cs = _epg_callsign(ename)
                if cs and cs not in index:
                    index[cs] = (eid, tvg_id, src.id)
        return index

    def _epg_match(self, buckets, keymap, settings, cfg):
        sources = self._all_epg_sources()
        if not sources:
            return {"matched": 0, "message": "no EPG source found"}
        index = self._build_epg_index(sources)
        respect_manual = cfg["respect_manual_epg"]

        ch_ids = [keymap[k]["id"] for k in buckets if k in keymap]
        already = dict(Channel.objects.filter(id__in=ch_ids).values_list("id", "epg_data_id"))

        updates = []
        matched_source_ids = set()
        for k, b in buckets.items():
            v = keymap.get(k)
            if not v:
                continue
            ch_id = v["id"]
            if respect_manual and already.get(ch_id):
                continue
            hit = self._lookup_epg(b, index)
            if not hit:
                continue
            eid, tvg, sid = hit
            updates.append((ch_id, eid, tvg))
            matched_source_ids.add(sid)

        for ch_id, eid, tvg in updates:
            Channel.objects.filter(id=ch_id).update(epg_data_id=eid, tvg_id=tvg or "")

        refreshed = []
        if updates:
            try:
                from apps.epg.tasks import refresh_epg_data
                for sid in matched_source_ids:
                    refresh_epg_data(sid)
                    refreshed.append(sid)
            except Exception:
                logger.warning("[Failovarr] refresh_epg_data failed", exc_info=True)

        return {
            "matched": len(updates),
            "sources": [s.name for s in sources],
            "refresh_triggered": bool(refreshed),
        }

    def _lookup_epg(self, bucket, index):
        """Deterministic layered lookup. Returns (eid, tvg, source_id) or None. Every
        candidate is an EXACT index hit — no fuzzy scoring, so no confident-but-wrong
        guides."""
        ek = bucket["epg_key"]
        toks = ek.split()
        cands = [ek]
        # timeshift feed -> base guide (East/West/Pacific share the same schedule)
        t = toks[:]
        while len(t) > 1 and t[-1] in _EPG_TIMESHIFT:
            t = t[:-1]
        if t and " ".join(t) != ek:
            cands.append(" ".join(t))
        # drop a trailing generic word
        if len(toks) > 1 and toks[-1] in ("NETWORK", "CHANNEL", "TV"):
            cands.append(" ".join(toks[:-1]))
        # tiny hand-verified alias table
        if ek in _EPG_ALIAS:
            cands.append(_EPG_ALIAS[ek])
        # callsign for locals
        if bucket.get("callsign"):
            cands.append(bucket["callsign"])
        for c in cands:
            hit = index.get(c)
            if hit:
                return hit
        return None

    # ================================================================ PPV events
    def _gather_ppv(self, providers, cfg):
        """Walk each provider's PPV/event groups, parse live/upcoming events, and union
        the SAME event across providers by matchup key. Returns buckets shaped exactly
        like the stable engine's so `_create_channels`/`_update_channels` are reused."""
        ppv_re = _compile(cfg["ppv_groups"]) or _compile(_PPV_DEFAULT_GROUPS)
        skip_re = _compile(cfg.get("ppv_skip"))
        buckets = {}
        stats = {"scanned": 0, "idle": 0, "done": 0, "pairs": 0, "single": 0, "skip_category": 0}
        for idx, acct in enumerate(providers):
            gid_name = dict(
                ChannelGroup.objects.filter(
                    id__in=ChannelGroupM3UAccount.objects.filter(m3u_account=acct, enabled=True)
                    .values_list("channel_group_id", flat=True)
                ).values_list("id", "name")
            )
            gids = [gid for gid, nm in gid_name.items()
                    if ppv_re.search(nm or "") and not (skip_re and skip_re.search(nm or ""))]
            if not gids:
                continue
            qs = Stream.objects.filter(m3u_account=acct, channel_group_id__in=gids)
            if cfg["skip_stale"]:
                qs = qs.exclude(is_stale=True)
            for pk, name, gid in qs.values_list("id", "name", "channel_group_id").iterator():
                stats["scanned"] += 1
                p = _ppv_parse(name)
                if p is None:
                    stats["idle"] += 1
                    continue
                if p["status"] == "done":  # live + upcoming only
                    stats["done"] += 1
                    continue
                key = p["key"]
                b = buckets.get(key)
                if b is None:
                    b = buckets[key] = {
                        "display": (p["title"] or key)[:255],
                        "is_adult": False,
                        "epg_key": "",
                        "callsign": None,
                        "group": _group_display(gid_name.get(gid, "PPV"), cfg["merge_group_suffixes"], cfg["group_aliases"]),
                        "prov": {},
                    }
                b["prov"].setdefault(idx, []).append((_quality_rank(name), pk))
        for b in buckets.values():
            if len(b["prov"]) > 1:
                stats["pairs"] += 1
            else:
                stats["single"] += 1
        return buckets, stats

    def _ppv_job(self, dry_run, settings):
        started = _now()
        providers = self._resolve_providers(settings)
        region_allow = self._region_allow(settings)
        cfg = self._engine_cfg(settings, region_allow)
        base_prof, adult_prof = self._get_profiles(settings)

        if cfg["ppv_events"]:
            buckets, gstats = self._gather_ppv(providers, cfg)
            # Keep only events carried by at least ppv_min_providers (2 = cross-provider
            # failover pairs only; 1 = every live event, "management" mode).
            minp = cfg["ppv_min_providers"]
            if minp > 1:
                buckets = {k: b for k, b in buckets.items() if len(b["prov"]) >= minp}
        else:
            # PPV turned OFF -> desired set is empty, so a refresh removes every PPV
            # channel (turn it off, hit Refresh, they're gone — a clean off-switch).
            buckets, gstats = {}, {"scanned": 0, "idle": 0, "done": 0, "pairs": 0, "single": 0}
        keymap = self._read_ppv_keymap()
        if keymap:
            live = set(Channel.objects.filter(id__in=[v["id"] for v in keymap.values()]).values_list("id", flat=True))
            keymap = {k: v for k, v in keymap.items() if v["id"] in live}

        desired = set(buckets)
        existing = set(keymap)
        to_create = sorted(desired - existing)
        to_prune = sorted(existing - desired)
        common = desired & existing

        report = {
            "status": "done", "mode": "ppv", "dry_run": dry_run,
            "started": _iso(started), "finished": _iso(_now()),
            "providers": [p.name for p in providers],
            "stats": {
                "streams_scanned": gstats["scanned"],
                "idle_skipped": gstats["idle"],
                "ended_dropped": gstats["done"],
                "events_total": len(buckets),
                "failover_pairs": gstats["pairs"],
                "single_source": gstats["single"],
                "events_existing": len(existing),
                "events_to_create": len(to_create),
                "events_to_prune": len(to_prune),
            },
            "samples": {"pairs": [buckets[k]["display"] for k in desired
                                  if len(buckets[k]["prov"]) > 1][:20]},
        }

        if dry_run:
            if not cfg["ppv_events"]:
                report["message"] = f"DRY-RUN PPV: disabled — would remove {len(to_prune)} PPV channel(s)."
            else:
                report["message"] = (
                    f"DRY-RUN PPV: {len(buckets)} live/upcoming events "
                    f"({gstats['pairs']} cross-provider failover pairs, {gstats['single']} single). "
                    f"Would create {len(to_create)}, prune {len(to_prune)}, keep {len(common)}."
                )
            self._write_status(report)
            self._ws_done(report["message"], dry_run=True)
            return report

        gcache = {}
        with transaction.atomic():
            next_num = self._next_number(cfg["ppv_number_start"], keymap)
            created = self._create_channels(to_create, buckets, gcache, base_prof, adult_prof, cfg, keymap, next_num)
            updated = self._update_channels(common, buckets, keymap, base_prof, adult_prof, cfg, gcache) if common else 0
            pruned = 0
            if to_prune:
                ids = [keymap[k]["id"] for k in to_prune]
                for chunk in _chunks(ids, 2000):
                    Channel.objects.filter(id__in=chunk).delete()
                for k in to_prune:
                    keymap.pop(k, None)
                pruned = len(ids)
        self._write_ppv_keymap(keymap)

        report["stats"].update({"created": created, "updated": updated, "pruned": pruned})
        report["finished"] = _iso(_now())
        if not cfg["ppv_events"]:
            report["message"] = f"PPV disabled — removed {pruned} event channel(s)."
        else:
            report["message"] = (
                f"PPV: {created} events added, {updated} updated, {pruned} ended/removed "
                f"({len(buckets)} live now: {gstats['pairs']} failover pairs, {gstats['single']} single)."
            )
        self._write_status(report)
        logger.info("[Failovarr] %s", report["message"])
        self._ws_done(report["message"])
        send_websocket_update("updates", "update", {"type": "channels_refresh"})
        return report

    # ----------------------------------------------------------- providers/cfg
    def _resolve_providers(self, settings):
        by_name = {}
        dup = set()
        for a in M3UAccount.objects.all():
            if a.name in by_name:
                dup.add(a.name)
            by_name[a.name] = a
        raw = (settings.get("provider_priority") or "").strip()
        names = [x.strip() for x in raw.split(",") if x.strip()]
        if not names:
            names = [a.name for a in M3UAccount.objects.filter(is_active=True, account_type="XC").order_by("name")]
        providers = []
        for nm in names:
            if nm in dup:
                raise ValueError(f"Multiple M3U accounts are named '{nm}'. Rename them uniquely.")
            acct = by_name.get(nm)
            if not acct:
                raise ValueError(f"Provider '{nm}' not found. Check 'Providers' in settings.")
            if acct not in providers:
                providers.append(acct)
        if len(providers) < 1:
            raise ValueError("No providers configured.")
        return providers

    def _region_allow(self, settings):
        raw = (settings.get("region_allowlist") or DEFAULT_REGION_ALLOW)
        return {t.strip().upper() for t in raw.split(",") if t.strip()}

    def _engine_cfg(self, settings, region_allow):
        providers = self._resolve_providers(settings)
        return {
            "nproviders": len(providers),
            "region_allow": region_allow,
            "skip_re": settings.get("skip_groups", DEFAULT_SKIP_GROUPS),
            "adult_re": settings.get("adult_detect", DEFAULT_ADULT_DETECT),
            "include_247": bool(settings.get("include_247", True)),
            "filter_foreign": bool(settings.get("filter_foreign_country", True)),
            "keep_us_market": bool(settings.get("keep_us_market", True)),
            "merge_quality": bool(settings.get("merge_quality_variants", True)),
            "skip_junk": bool(settings.get("skip_junk_names", True)),
            "skip_stale": bool(settings.get("skip_stale", True)),
            "adult_split": bool(settings.get("adult_profile_split", True)),
            "subset_profiles": _parse_subset_profiles(
                settings.get("subset_profiles", ""),
                reserved=(
                    (settings.get("base_profile") or "failovarr").strip(),
                    (settings.get("adult_profile") or "failovarr+18").strip(),
                ),
            ),
            "epg_match": bool(settings.get("epg_match", True)),
            "respect_manual_epg": bool(settings.get("respect_manual_epg", True)),
            "number_start": int(settings.get("channel_number_start", 1) or 0),
            "group_name": (settings.get("channel_group_name") or "Failovarr").strip() or "Failovarr",
            "merge_group_suffixes": bool(settings.get("merge_group_suffixes", True)),
            "group_aliases": _parse_group_aliases(settings.get("group_aliases", _GROUP_ALIAS_DEFAULT)),
            "locals_by_name": bool(settings.get("locals_by_name", True)),
            "ppv_events": bool(settings.get("ppv_events", False)),
            "ppv_min_providers": max(1, int(settings.get("ppv_min_providers", 1) or 1)),
            "ppv_groups": settings.get("ppv_groups", _PPV_DEFAULT_GROUPS),
            "ppv_skip": settings.get("ppv_skip", _PPV_SKIP_DEFAULT),
            "ppv_number_start": int(settings.get("ppv_number_start", 90000) or 90000),
            "ppv_schedule_minutes": int(settings.get("ppv_schedule_minutes", 0) or 0),
        }

    # ----------------------------------------------------------- profiles/group
    def _get_profiles(self, settings):
        base_name = (settings.get("base_profile") or "failovarr").strip()
        adult_name = (settings.get("adult_profile") or "failovarr+18").strip()
        base, _ = ChannelProfile.objects.get_or_create(name=base_name)
        if bool(settings.get("adult_profile_split", True)):
            adult, _ = ChannelProfile.objects.get_or_create(name=adult_name)
        else:
            adult = base
        return base, adult

    def _get_subset_profiles(self, cfg):
        """Resolve configured subset lineups to ``[(ChannelProfile, frozenset(groups)), …]``
        (creating the profiles as needed). Additive HDHR/Plex views; empty when none set.

        A NEW profile is created with ``_start_empty`` so Dispatcharr's post-save signal
        does NOT auto-populate it with every channel — the reconcile then fills it with
        only the channels in the listed groups. (An existing profile is reused as-is; the
        update pass keeps its owned membership in sync with the group list.)"""
        out = []
        for name, groups in cfg.get("subset_profiles", []):
            prof = ChannelProfile.objects.filter(name=name).first()
            if prof is None:
                prof = ChannelProfile(name=name)
                prof._start_empty = True  # skip the auto-add-all-channels signal
                prof.save()
            out.append((prof, groups))
        return out

    def _owned_channel_ids(self, base_prof, adult_prof):
        pids = {base_prof.id, adult_prof.id}
        ids = set(
            ChannelProfileMembership.objects.filter(channel_profile_id__in=pids)
            .values_list("channel_id", flat=True)
        )
        # Exclude PPV-event channels: they live in the same profiles but churn on
        # their own cadence, so they must not drive the stable set's health/seed math.
        return ids - {v["id"] for v in self._read_ppv_keymap().values()}

    def _get_group(self, name):
        group, _ = ChannelGroup.objects.get_or_create(name=name)
        return group

    def _next_number(self, start, keymap):
        nums = [v.get("num", 0) for v in keymap.values() if isinstance(v.get("num", 0), (int, float))]
        base = max(nums) + 1 if nums else start
        return max(base, start)

    # ----------------------------------------------------------- keymap I/O
    def _read_keymap(self):
        try:
            with open(KEYMAP_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh) or {}
        except Exception:
            return {}

    def _write_keymap(self, keymap):
        try:
            tmp = KEYMAP_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(keymap, fh)
            os.replace(tmp, KEYMAP_FILE)
        except Exception:
            logger.exception("[Failovarr] could not write keymap")

    def _read_ppv_keymap(self):
        try:
            with open(PPV_KEYMAP_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh) or {}
        except Exception:
            return {}

    def _write_ppv_keymap(self, keymap):
        try:
            tmp = PPV_KEYMAP_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(keymap, fh)
            os.replace(tmp, PPV_KEYMAP_FILE)
        except Exception:
            logger.exception("[Failovarr] could not write ppv keymap")

    # ----------------------------------------------------------- backups
    def _backup(self, owned_ids, keymap):
        try:
            rows = list(
                ChannelStream.objects.filter(channel_id__in=list(owned_ids))
                .values_list("channel_id", "stream_id", "order")
            )
            chans = list(
                Channel.objects.filter(id__in=list(owned_ids))
                .values("id", "name", "channel_number", "channel_group_id", "epg_data_id", "tvg_id", "is_adult")
            )
            ts = _now().strftime("%Y%m%d-%H%M%S-%f")
            path = os.path.join(DATA_DIR, f"backup_{ts}.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({"channels": chans, "streams": rows, "keymap": keymap}, fh)
            for old in sorted(glob.glob(BACKUP_GLOB))[:-BACKUP_KEEP]:
                try:
                    os.remove(old)
                except Exception:
                    pass
            return path
        except Exception:
            logger.exception("[Failovarr] backup failed (continuing)")
            return None

    # ----------------------------------------------------------- status I/O
    def _write_status(self, data):
        try:
            tmp = STATUS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            os.replace(tmp, STATUS_FILE)
        except Exception:
            logger.debug("could not write status file", exc_info=True)

    def _read_status(self):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except Exception:
            return None

    def _ws_done(self, message, dry_run=False):
        send_websocket_update(
            "updates", "update",
            {"type": "failovarr", "status": "done", "dry_run": dry_run, "message": message},
        )

    # --------------------------------------------------- channel-drop safeguard
    def _read_health(self):
        try:
            with open(HEALTH_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh) or {}
        except Exception:
            return {}

    def _write_health(self, data):
        try:
            tmp = HEALTH_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(tmp, HEALTH_FILE)
        except Exception:
            logger.debug("could not write health file", exc_info=True)

    def _emergency_alert(self, settings, health):
        msg = (
            f"A Failovarr reconcile would have pruned {health.get('to_prune','?')} of "
            f"{health.get('existing','?')} channels (only {health.get('desired','?')} "
            "survived the provider gather).\n\n"
            "This is the signature of a provider outage: an M3U refresh came back "
            "empty/tiny. Pruning was SKIPPED as a precaution — Failovarr will not delete "
            "channels during a suspected collapse.\n\n"
            "To recover: refresh the provider M3U accounts once they're back, then run "
            "Failovarr reconcile. (If this was a deliberate large cleanup, just run it "
            "again — the next run lets the reduction through.)"
        )
        self._gotify_send(settings, "Failovarr ⚠️ CHANNEL DROP", msg, 8)

    # --------------------------------------------------------------- reporting
    def _format_report(self, data):
        s = data.get("stats", {})
        lines = [
            f"[{data.get('status','?')}{' / dry-run' if data.get('dry_run') else ''} / {data.get('mode','?')}] "
            f"{data.get('message','')}",
        ]
        if s:
            lines.append(
                f"streams: {s.get('streams_scanned','?')} scanned "
                f"(skipped {s.get('streams_skipped_event','?')} event, "
                f"{s.get('streams_skipped_foreign','?')} foreign, "
                f"{s.get('streams_skipped_junk','?')} junk)"
            )
            lines.append(
                f"channels: {s.get('keys_total','?')} total "
                f"({s.get('failover_pairs','?')} pairs, {s.get('single_source','?')} single, "
                f"{s.get('adult','?')} adult, {s.get('locals_by_callsign','?')} locals)"
            )
            lines.append(
                f"changes: +{s.get('created', s.get('channels_to_create','?'))} "
                f"~{s.get('updated','?')} -{s.get('pruned', s.get('channels_to_prune','?'))}"
            )
        e = data.get("epg")
        if e:
            srcs = e.get("sources") or ([e.get("source")] if e.get("source") else [])
            lines.append(f"epg: {e.get('matched','?')} mapped (sources: {', '.join(srcs) or '?'})")
        h = data.get("health")
        if h:
            flag = " ⚠️ COLLAPSE — prune blocked" if h.get("collapse") else ""
            lines.append(f"health: {h.get('existing','?')} owned, {h.get('to_prune','?')} would prune{flag}")
        if data.get("backup"):
            lines.append(f"backup: {data['backup']}")
        return "\n".join(lines)

    # --------------------------------------------------------------- gotify
    def _gotify_send(self, settings, title, message, priority):
        server = (settings.get("gotify_server_url") or "").strip().rstrip("/")
        token = (settings.get("gotify_token") or "").strip()
        if server and token:
            url = f"{server}/message?token={urllib.parse.quote(token, safe='')}"
        else:
            url = (settings.get("gotify_url") or "").strip()
        if not url:
            return
        try:
            data = urllib.parse.urlencode({"title": title, "message": message[:1800], "priority": priority}).encode()
            req = urllib.request.Request(url, data=data, method="POST")
            urllib.request.urlopen(req, timeout=10).read()
        except Exception:
            logger.warning("[Failovarr] gotify notify failed", exc_info=True)

    def _notify_gotify(self, settings, ok, message, changed=None):
        mode = (settings.get("gotify_notify") or "off").strip()
        if mode == "off":
            return
        if mode == "on_failure" and ok:
            return
        if mode == "on_change" and ok and changed == 0:
            return  # succeeded with nothing to do — stay quiet
        title = "Failovarr ✅" if ok else "Failovarr ❌ FAILED"
        self._gotify_send(settings, title, message, 3 if ok else 7)

    # ------------------------------------------------------------- scheduler
    def _ensure_scheduler(self):
        for t in threading.enumerate():
            if t.name == "failovarr-sched" and t.is_alive():
                return
        Plugin._sched_stop.clear()
        t = threading.Thread(target=self._scheduler_loop, name="failovarr-sched", daemon=True)
        Plugin._sched_thread = t
        t.start()
        logger.info("[Failovarr] scheduler thread active (checks schedule_time every %ss)", SCHED_TICK_SECS)

    def _scheduler_loop(self):
        os.makedirs(SCHED_DIR, exist_ok=True)
        while not Plugin._sched_stop.wait(SCHED_TICK_SECS):
            try:
                self._scheduler_tick()
            except Exception:
                logger.exception("failovarr scheduler tick failed")

    def _scheduler_tick(self):
        close_old_connections()
        # NEVER run the schedule while the plugin is disabled. The scheduler thread
        # keeps ticking (it can't reliably be torn down on disable), so the gate has
        # to live here — otherwise a disabled plugin would keep firing the daily run
        # and its notifications.
        if not self._plugin_enabled():
            return
        cfg = self._scheduled_settings()
        # PPV events run on their own fast cadence, independent of the daily reconcile.
        self._ppv_tick(cfg)
        target = self._parse_hhmm(cfg.get("schedule_time"))
        if not target:
            return
        now = _now()
        target_today = now.replace(hour=target[0], minute=target[1], second=0, microsecond=0)
        target_dt = target_today if target_today <= now else target_today - datetime.timedelta(days=1)
        if (now - target_dt).total_seconds() > SCHED_WINDOW_SECS:
            return
        datestr = target_dt.strftime("%Y%m%d")
        success_marker = os.path.join(SCHED_DIR, f"success-{datestr}.marker")
        if os.path.exists(success_marker):
            return
        # Hard cap: at most MAX_DAILY_ATTEMPTS runs per target day, spaced by the
        # cooldown. On failure it retries a couple of times then GIVES UP for the day
        # (no indefinite 15-min retry, no notification spam). A success writes the
        # marker above and stops further attempts regardless.
        att = self._read_attempt()
        if att.get("date") != datestr:
            att = {"date": datestr, "count": 0, "last": 0.0}
        if att.get("count", 0) >= MAX_DAILY_ATTEMPTS:
            return
        if att.get("last") and (time.time() - att["last"]) < SCHED_COOLDOWN_SECS:
            return
        # Acquire the lock BEFORE consuming a daily attempt — otherwise lock contention
        # (a manual run, or another worker) would burn all 3 attempts without ever
        # running and skip the day's reconcile.
        if not self._acquire_lock():
            return
        att["count"] = att.get("count", 0) + 1
        att["last"] = time.time()
        self._write_attempt(att)
        self._cancel.clear()
        ok = False
        message = ""
        changed = None
        try:
            logger.info("[Failovarr] scheduled reconcile firing (target %02d:%02d UTC)", *target)
            report = self._run_job(dry_run=False, settings=cfg, mode="reconcile")
            ok = bool(report and report.get("status") == "done")
            message = (report or {}).get("message", "no report")
            st = (report or {}).get("stats", {}) or {}
            changed = (st.get("created", 0) or 0) + (st.get("updated", 0) or 0) + (st.get("pruned", 0) or 0)
            # Refresh PPV events once, right after the daily reconcile — a once-a-day
            # client only sees a daily snapshot anyway, and failover works at playback
            # without a refresh. (Intra-day cadence is opt-in via _ppv_tick.)
            if ok and bool(cfg.get("ppv_events", False)):
                try:
                    prep = self._ppv_job(dry_run=False, settings=cfg)
                    message += f" | {(prep or {}).get('message', 'PPV done')}"
                except Exception:
                    logger.exception("failovarr daily PPV refresh failed")
            if ok:
                _touch(success_marker)
                self._cleanup_markers()
        except Exception as exc:
            logger.exception("failovarr scheduled run failed")
            ok = False
            message = f"{exc}"
        finally:
            close_old_connections()
            self._release_lock()
            self._notify_gotify(cfg, ok, message, changed)

    def _ppv_tick(self, cfg):
        """OPTIONAL intra-day PPV refresh, for clients that pull the playlist more than
        once a day. Off by default (0) — PPV normally refreshes once with the daily
        reconcile, which is all a once-a-day client can see anyway. Shares the run lock."""
        if not bool(cfg.get("ppv_events", False)):
            return
        minutes = int(cfg.get("ppv_schedule_minutes", 0) or 0)
        if minutes <= 0:
            return
        interval = max(5, minutes) * 60
        marker = os.path.join(SCHED_DIR, "ppv_last.ts")
        last = self._read_ts(marker)
        if last and (time.time() - last) < interval:
            return
        if not self._acquire_lock():
            return
        try:
            _atomic_write(marker, str(time.time()))
            self._cancel.clear()
            report = self._ppv_job(dry_run=False, settings=cfg)
            logger.info("[Failovarr] scheduled PPV refresh: %s", (report or {}).get("message", "?"))
        except Exception:
            logger.exception("failovarr ppv tick failed")
        finally:
            close_old_connections()
            self._release_lock()

    def _read_ts(self, path):
        try:
            with open(path) as fh:
                return float(fh.read().strip())
        except Exception:
            return None

    def _scheduled_settings(self):
        from apps.plugins.models import PluginConfig
        cfg = PluginConfig.objects.filter(key=PLUGIN_KEY).values("settings").first()
        saved = (cfg or {}).get("settings", {}) or {}
        merged = dict(_SCHED_DEFAULTS)
        merged.update({k: v for k, v in saved.items() if v is not None})
        return merged

    def _parse_hhmm(self, val):
        val = (val or "").strip()
        if not val:
            return None
        try:
            hh, mm = val.split(":")
            hh, mm = int(hh), int(mm)
            if 0 <= hh < 24 and 0 <= mm < 60:
                return (hh, mm)
        except Exception:
            pass
        return None

    def _read_attempt(self):
        try:
            with open(ATTEMPT_FILE, "r") as fh:
                return json.load(fh) or {}
        except Exception:
            return {}

    def _write_attempt(self, data):
        _atomic_write(ATTEMPT_FILE, json.dumps(data))

    def _plugin_enabled(self):
        """Is this plugin currently enabled? Fail CLOSED (treat unknown as disabled)
        so the schedule never fires for a disabled/unregistered plugin."""
        try:
            from apps.plugins.models import PluginConfig
            row = PluginConfig.objects.filter(key=PLUGIN_KEY).values("enabled").first()
            return bool(row and row["enabled"])
        except Exception:
            logger.debug("could not read plugin enabled state", exc_info=True)
            return False

    def _cleanup_markers(self):
        try:
            cutoff = time.time() - 8 * 86400
            for fn in os.listdir(SCHED_DIR):
                fp = os.path.join(SCHED_DIR, fn)
                if fn.startswith("success-") and os.path.isfile(fp) and os.path.getmtime(fp) < cutoff:
                    os.remove(fp)
        except Exception:
            pass


# Trailing timeshift tokens: an East/West/Pacific feed shares the base channel's
# guide (schedule offset only), so falling back to the base key is safe/deterministic.
_EPG_TIMESHIFT = {"EAST", "WEST", "PACIFIC"}

# ----------------------------------------------------------------- tiny alias table
# Deterministic EPG aliases (channel key -> myepg.top name key). Grown over time.
_EPG_ALIAS = {
    "A AND E": "AE",
    "AE": "A AND E",
    "TBS NETWORK": "TBS",
}


def _compile(pattern):
    if not pattern:
        return None
    try:
        return re.compile(pattern, re.IGNORECASE)
    except re.error:
        logger.warning("[Failovarr] bad regex %r — ignoring", pattern)
        return None


def _atomic_write(path, text):
    try:
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "w") as fh:
            fh.write(text)
        os.replace(tmp, path)
    except Exception:
        pass


def _touch(path):
    _atomic_write(path, _iso(_now()))


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]
