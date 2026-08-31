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

__version__ = "0.1.4"

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
HEALTH_FILE = os.path.join(DATA_DIR, "health.json")
BACKUP_GLOB = os.path.join(DATA_DIR, "backup_*.json")

# Channel-drop safeguard. If the owned channel count collapses (a provider outage
# came back empty and channels got wiped, or the providers returned almost nothing
# this run), skip pruning and fire a high-priority alert instead of deleting.
HEALTH_MIN_BASELINE = 50   # never alarm below this many channels
HEALTH_DROP_FRAC = 0.5     # current <= 50% of baseline = a collapse

# Scheduler tuning.
SCHED_TICK_SECS = 30
SCHED_WINDOW_SECS = 6 * 3600
SCHED_COOLDOWN_SECS = 15 * 60
MAX_DAILY_ATTEMPTS = 3        # hard cap on scheduled runs per day (stops retry/notify spam)
LOCK_STALE_SECS = 30 * 60
BACKUP_KEEP = 7

# ---------------------------------------------------------------- defaults
DEFAULT_SKIP_GROUPS = (
    r"PPV|EVENT|EXCLUSIVE|8K\b|ESPN\s*\+|ESPN PLUS|FLO |NFHS|MILB|DAZN|NCAAB|NBA TEAM|BTN"
)
DEFAULT_ADULT_DETECT = r"18[|:+]|\bADULT\b|\bXXX\b|FOR ADULTS"
DEFAULT_REGION_ALLOW = "US,EN"

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
}

# Foreign COUNTRY prefixes to filter (NOT languages — US-market channels are kept
# regardless of language). Matched as the leading ``XX|`` / ``XX:`` token.
_FOREIGN = {
    "CL", "NL", "AR", "MX", "DE", "FR", "ES", "IT", "PT", "GR", "TR", "RU", "PL",
    "RO", "JP", "CN", "KU", "IL", "IR", "AL", "BG", "PK", "AF", "SO", "BE", "MT",
    "IN", "QC", "LA",
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


def _consolidation_key(name, region_allow, drop_quality):
    """Group-by key: strip region prefix (US/EN), keep other prefixes, drop quality."""
    s = _fold(name)
    ptoks, body = _prefix_tokens(s)
    if ptoks and ptoks[0] in region_allow:
        s = body  # region prefix stripped; a non-region prefix (GO/PRIME) stays in
    toks = _tokens(s)
    if drop_quality:
        toks = [t for t in toks if t not in _QUALITY]
    return " ".join(toks)


def _epg_key(name):
    """EPG-lookup key: strip ALL prefixes + quality (myepg.top is keyed by bare names)."""
    s = _fold(name)
    ptoks, body = _prefix_tokens(s)
    while ptoks:
        s = body
        ptoks, body = _prefix_tokens(s)
    return " ".join(t for t in _tokens(s) if t not in _QUALITY)


def _callsign(name):
    s = _fold(name).upper()
    m = _CALL_PAREN.search(s) or _CALL_BARE.search(s)
    return m.group(1) if m else None


def _local_network(group_name):
    m = _LOCAL_RE.search(_fold(group_name or "").upper())
    return m.group(1) if m else None


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
    if re.search(r"#{3,}", n):  # labelled divider e.g. "##### FOX WISCONSIN #####"
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


def _group_display(gname):
    """Clean a provider group name into a channel-group label for the IPTV client:
    strip ANY leading prefix (``US|``/``US:``/``18|``/``MU|`` …) so trex's ``US| PRIME``
    and strong's ``US: PRIME`` land in ONE group, fold superscripts, collapse spaces.
    Groups are the client's category navigation, so this preserves the provider's own
    taxonomy (News/Sports/Locals/24-7/…) instead of one giant bucket."""
    s = _fold(gname or "").replace("\xa0", " ")
    ptoks, body = _prefix_tokens(s)
    if ptoks:  # strip whatever prefix the group carries (region OR provider tag)
        s = body
    # drop pure-quality words (RAW, 60FPS, "HD/RAW", …) — decoration, not a category
    kept = []
    for w in s.split():
        subs = [t for t in re.split(r"[^0-9A-Za-z]+", w) if t]
        if subs and all(t.upper() in _QUALITY for t in subs):
            continue
        kept.append(w)
    return " ".join(kept).strip() or "Failovarr"


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
                "label": "Merge quality variants (HD + 4K → one channel)",
                "type": "boolean",
                "default": True,
                "help_text": "On: drop quality tags from the key so HD/4K/FHD of the same channel become one (more failover). Off: keep them separate.",
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
                "label": "Channel group for created channels",
                "type": "string",
                "default": "Failovarr",
                "help_text": "All created channels are placed in this channel group.",
            },
            {
                "id": "skip_stale",
                "label": "Skip dead/stale streams",
                "type": "boolean",
                "default": True,
                "help_text": "Ignore streams Dispatcharr has flagged stale, so you never attach a dead failover.",
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
                    {"value": "on_completion", "label": "On every completion"},
                ],
                "default": "off",
                "help_text": "Notify a Gotify endpoint after the daily scheduled run.",
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
                    "If the owned channel count suddenly collapses (a provider outage "
                    "wiped channels), skip pruning and send a HIGH-priority Gotify "
                    "alert even if notifications are Off."
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
        health = self._eval_health(len(owned_before), record=(not dry_run))

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
        prune_blocked = bool(health and health.get("cliff") and mode != "seed")

        backup_path = self._backup(owned_before, keymap)

        base_prof, adult_prof = self._get_profiles(settings)
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
                to_create, buckets, gcache, base_prof, adult_prof, cfg, keymap, next_num
            )
            report["stats"]["created"] = created

            # UPDATE existing channels (stream order + membership + adult flag + group).
            updated = 0
            if common:
                updated = self._update_channels(
                    common, buckets, keymap, base_prof, adult_prof, cfg, gcache
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
        if health and health.get("alert") and bool(settings.get("health_alert", True)):
            self._emergency_alert(settings, health, len(owned_before))
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

                # Local station? key on callsign (network + callsign).
                key = None
                callsign = None
                net = _local_network(gname)
                if net:
                    callsign = _callsign(name)
                    if callsign:
                        key = f"{net} {callsign}"
                if key is None:
                    key = _consolidation_key(name, region_allow, cfg["merge_quality"])
                if not key:
                    stats["skip_junk"] += 1
                    continue

                b = buckets.get(key)
                if b is None:
                    b = buckets[key] = {
                        "display": _display_name(name, region_allow),
                        "is_adult": is_adult,
                        "epg_key": _epg_key(name),
                        "callsign": callsign,
                        # group from the FIRST (highest-priority provider) stream seen
                        # for this key = the primary's category.
                        "group": _group_display(gname),
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

    def _create_channels(self, keys, buckets, gcache, base_prof, adult_prof, cfg, keymap, next_num):
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
            keymap[k] = {"id": ch.id, "num": cnum}
            for o, spk in enumerate(streams):
                cs_rows.append(ChannelStream(channel_id=ch.id, stream_id=spk, order=o))
            # Membership: adult profile always; base only for non-adult.
            mem_rows.append(ChannelProfileMembership(channel_profile=adult_prof, channel_id=ch.id, enabled=True))
            if not (split and is_adult):
                mem_rows.append(ChannelProfileMembership(channel_profile=base_prof, channel_id=ch.id, enabled=True))

        for chunk in _chunks(cs_rows, 2000):
            ChannelStream.objects.bulk_create(chunk, ignore_conflicts=True)
        for chunk in _chunks(mem_rows, 2000):
            ChannelProfileMembership.objects.bulk_create(chunk, ignore_conflicts=True)
        return len(created_objs)

    # -------------------------------------------------------------- update
    def _update_channels(self, keys, buckets, keymap, base_prof, adult_prof, cfg, gcache):
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

        # current adult flag + group per channel
        info_now = {
            cid: (ad, gid)
            for cid, ad, gid in Channel.objects.filter(id__in=ch_ids)
            .values_list("id", "is_adult", "channel_group_id")
        }
        mem_now = set()
        for pid, cid in ChannelProfileMembership.objects.filter(
            channel_id__in=ch_ids, channel_profile_id__in=[base_prof.id, adult_prof.id]
        ).values_list("channel_profile_id", "channel_id"):
            mem_now.add((pid, cid))

        to_create_cs, to_update_cs, to_delete_cs = [], [], []
        adult_flip = []
        group_flip = []
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

            cur_adult, cur_gid = info_now.get(ch_id, (None, None))
            if cur_adult != b["is_adult"]:
                adult_flip.append((ch_id, b["is_adult"]))
                local_changed = True
            desired_g = self._group_obj(b.get("group"), gcache, cfg)
            if cur_gid != desired_g.id:
                group_flip.append((ch_id, desired_g.id))
                local_changed = True

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

    def _epg_source(self):
        """The EPG source with the most real (non-dummy) entries."""
        from apps.epg.models import EPGSource, EPGData
        best, best_n = None, -1
        for src in EPGSource.objects.all():
            n = EPGData.objects.filter(epg_source=src).exclude(tvg_id__startswith="dummy").count()
            if n > best_n:
                best, best_n = src, n
        return best

    def _build_epg_index(self, source):
        """name-key -> (epg_data_id, tvg_id). Real entries only; prefer .us on collision."""
        from apps.epg.models import EPGData
        index = {}
        for eid, tvg_id, ename in (
            EPGData.objects.filter(epg_source=source)
            .exclude(tvg_id__startswith="dummy")
            .values_list("id", "tvg_id", "name").iterator()
        ):
            key = _epg_key(ename)
            if not key:
                continue
            prev = index.get(key)
            if prev is None:
                index[key] = (eid, tvg_id)
            elif str(tvg_id or "").lower().endswith(".us") and not str(prev[1] or "").lower().endswith(".us"):
                index[key] = (eid, tvg_id)
        return index

    def _epg_match(self, buckets, keymap, settings, cfg):
        source = self._epg_source()
        if source is None:
            return {"matched": 0, "message": "no EPG source found"}
        index = self._build_epg_index(source)
        respect_manual = cfg["respect_manual_epg"]

        ch_ids = [keymap[k]["id"] for k in buckets if k in keymap]
        already = dict(
            Channel.objects.filter(id__in=ch_ids)
            .values_list("id", "epg_data_id")
        )

        updates = []
        matched = 0
        for k, b in buckets.items():
            v = keymap.get(k)
            if not v:
                continue
            ch_id = v["id"]
            if respect_manual and already.get(ch_id):
                continue
            eid_tvg = self._lookup_epg(b, index)
            if not eid_tvg:
                continue
            eid, tvg = eid_tvg
            updates.append((ch_id, eid, tvg))
            matched += 1

        for ch_id, eid, tvg in updates:
            Channel.objects.filter(id=ch_id).update(epg_data_id=eid, tvg_id=tvg or "")

        refreshed = False
        if updates:
            try:
                from apps.epg.tasks import refresh_epg_data
                refresh_epg_data(source.id)
                refreshed = True
            except Exception:
                logger.warning("[Failovarr] refresh_epg_data failed", exc_info=True)

        return {"matched": matched, "source": source.name, "refresh_triggered": refreshed}

    def _lookup_epg(self, bucket, index):
        # 1) EPG-key exact
        hit = index.get(bucket["epg_key"])
        if hit:
            return hit
        # 2) suffix drop (NETWORK / CHANNEL / TV) + tiny alias table
        toks = bucket["epg_key"].split()
        if len(toks) > 1 and toks[-1] in ("NETWORK", "CHANNEL", "TV"):
            hit = index.get(" ".join(toks[:-1]))
            if hit:
                return hit
        alias = _EPG_ALIAS.get(bucket["epg_key"])
        if alias:
            hit = index.get(alias)
            if hit:
                return hit
        # 3) callsign for locals
        if bucket.get("callsign"):
            hit = index.get(bucket["callsign"])
            if hit:
                return hit
        return None

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
            "epg_match": bool(settings.get("epg_match", True)),
            "respect_manual_epg": bool(settings.get("respect_manual_epg", True)),
            "number_start": int(settings.get("channel_number_start", 1) or 0),
            "group_name": (settings.get("channel_group_name") or "Failovarr").strip() or "Failovarr",
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

    def _owned_channel_ids(self, base_prof, adult_prof):
        pids = {base_prof.id, adult_prof.id}
        return set(
            ChannelProfileMembership.objects.filter(channel_profile_id__in=pids)
            .values_list("channel_id", flat=True)
        )

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

    def _eval_health(self, current, record):
        state = self._read_health()
        baseline = int(state.get("baseline", 0) or 0)
        alerted = bool(state.get("alerted", False))
        cliff = baseline >= HEALTH_MIN_BASELINE and current <= baseline * HEALTH_DROP_FRAC
        new_alert = bool(cliff and not alerted and record)
        dropped = max(0, baseline - current)
        pct = int(round(100 * dropped / baseline)) if baseline else 0
        if record:
            if new_alert:
                self._write_health({"baseline": baseline, "alerted": True, "current": current, "updated": _iso(_now())})
            else:
                self._write_health({"baseline": max(current, 0), "alerted": False, "current": current, "updated": _iso(_now())})
        return {"current": current, "baseline": baseline, "cliff": cliff, "dropped": dropped, "pct": pct, "alert": new_alert}

    def _emergency_alert(self, settings, health, current):
        msg = (
            f"Failovarr owned channel count dropped from {health.get('baseline','?')} "
            f"to {current} (-{health.get('pct','?')}%).\n\n"
            "This is the signature of a provider outage: an M3U refresh came back "
            "empty. Pruning was SKIPPED as a precaution — Failovarr will not delete "
            "channels during a suspected collapse.\n\n"
            "To recover: refresh the provider M3U accounts once they're back, then run "
            "Failovarr reconcile."
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
            lines.append(f"epg: {e.get('matched','?')} mapped (source {e.get('source','?')})")
        h = data.get("health")
        if h:
            flag = " ⚠️ COLLAPSE" if h.get("cliff") else ""
            lines.append(f"health: {h.get('current','?')} owned (baseline {h.get('baseline','?')}){flag}")
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

    def _notify_gotify(self, settings, ok, message):
        mode = (settings.get("gotify_notify") or "off").strip()
        if mode == "off":
            return
        if mode == "on_failure" and ok:
            return
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
        att["count"] = att.get("count", 0) + 1
        att["last"] = time.time()
        self._write_attempt(att)

        if not self._acquire_lock():
            return
        self._cancel.clear()
        ok = False
        message = ""
        try:
            logger.info("[Failovarr] scheduled reconcile firing (target %02d:%02d UTC)", *target)
            report = self._run_job(dry_run=False, settings=cfg, mode="reconcile")
            ok = bool(report and report.get("status") == "done")
            message = (report or {}).get("message", "no report")
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
            self._notify_gotify(cfg, ok, message)

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
