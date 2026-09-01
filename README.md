<p align="center">
  <img src="logo.png" alt="Failovarr" width="96" height="96" />
</p>

<h1 align="center">Failovarr</h1>

<p align="center">
  <strong>Cross-provider channel consolidation with failover for Dispatcharr.</strong>
</p>

<p align="center">
  <a href="#what-it-does">What it does</a> · <a href="#how-matching-works">Matching</a> · <a href="#channel-groups">Groups</a> · <a href="#epg--logos">EPG</a> · <a href="#ppv--live-event-failover">PPV</a> · <a href="#install">Install</a> · <a href="#settings">Settings</a> · <a href="#actions">Actions</a>
</p>

<p align="center">
  <img alt="License" src="https://img.shields.io/badge/license-MIT-blue" />
  <img alt="Python" src="https://img.shields.io/badge/python-3-3776AB?logo=python&logoColor=white" />
  <img alt="Dispatcharr" src="https://img.shields.io/badge/dispatcharr-plugin-1d9bf0?logo=plex&logoColor=white" />
  <img alt="Manifest" src="https://img.shields.io/badge/manifest-GPG--signed-brightgreen" />
</p>

---

## What it does

If you run two IPTV subscriptions as mutual backups — say **Trex** and **Strong** —
each one auto-syncs its own full channel list, so you end up with two parallel
universes of the same channels. **Failovarr unions them into one channel set with
cross-provider failover:**

- **One channel per real channel.** Both providers that carry it get their streams
  stacked as failover (priority order), so a connection outage or an in-use
  connection on one provider transparently falls over to the other.
- **Nothing is lost.** A channel only one provider carries is still kept
  (single-source), never dropped.
- **Category groups preserved.** Channels keep the provider's own categories
  (News / Sports / Locals / Entertainment / 24-7 / …) so your client stays navigable.
- **Locals merged by callsign.** US ABC/NBC/CBS/FOX affiliates match on their
  callsign (`WSB`, `KOMO`), the only stable cross-provider identifier — even when the
  same affiliate is packaged under two different provider groups.
- **Adult split.** Adult channels route to a `+18` profile only; everything else
  lands in both the base and `+18` profiles.
- **Free EPG + logos.** Each channel is mapped to a real guide entry by name (across
  every EPG source you've added) and a refresh pulls schedules and applies channel
  logos in one pass.
- **Optional PPV / live-event failover.** Surface live PPV events and merge the *same*
  event across both providers into one failover channel, so it doesn't drop mid-event.

It reconciles **in place** — a daily run adds new channels, refreshes failover
ordering, re-groups, and prunes only channels gone from *every* provider, so your
channel numbers, EPG mappings and manual tweaks are preserved. It **never
wipe-and-rebuilds** (a channel-collapse safeguard skips destructive work if a
provider outage suddenly empties your list), so it's safe to run unattended daily.

## How matching works

Providers name the same channel almost identically — they differ only by the
separator and quality decoration:

```
Trex:    US| CNN HD          Strong:  US: CNN HD
Trex:    US| DISCOVERY WEST HD    Strong:  US: DISCOVERY WEST HD
```

So the matcher is a **normalize-and-group**, not fuzzy scoring:

- Strip the region prefix (`US|` / `EN|`), NFKD-fold superscripts (`ᴴᴰ` → HD,
  `ᴿᴬᵂ` → RAW), drop quality tags (`4K/UHD/FHD/HD/…`), collapse to an uppercase key.
- **Keep** non-region prefixes (`GO|` / `PRIME|` / `MU|`) in the key so distinct feeds
  don't over-merge, along with East/West and trailing numbers. A trailing `+` is kept
  too, so `AMC` and `AMC+` stay separate channels.
- **Local stations** key on callsign; city labels are inconsistent and sometimes
  wrong, so they're never used for matching.
- **PPV / event groups are skipped** by the normal consolidation (see
  [PPV](#ppv--live-event-failover)) — their dated one-off names would spawn thousands
  of stale channels.
- **Foreign channels are filtered** — by country prefix *and* non-Latin script — so a
  German or Arabic feed never pollutes the list, while US-market channels (including
  Spanish-language networks like Telemundo/Univision) are kept.

The union of every distinct key from any provider becomes the channel set. It's a
dictionary group-by: it runs in **seconds**, is safe to run daily, and never produces
the confident-but-wrong matches fuzzy scoring does.

## Channel groups

Each channel takes the (cleaned) category group of its **primary** provider's stream,
so your client keeps the provider's own taxonomy instead of one giant list. The
region prefix is stripped so trex's `US| PRIME` and strong's `US: PRIME` merge into
one `PRIME` group, superscripts are folded, and — with **Tidy group names** on
(default) — a trailing `NETWORK`/`CHANNEL`/`TV` is dropped so `ABC NETWORK` and `ABC`
become one `ABC` group. Local affiliates group under their network (`ABC`, `NBC`, …).

## EPG + logos

With **Match EPG guide + logos** on, each channel is matched by name to a real guide
entry (dummy placeholders excluded, `.us` preferred for North America) across **every
EPG source you've added in Dispatcharr** — then a single refresh pulls schedules
**and** applies each channel's logo. Timeshift feeds (`CINEMAX EAST`) fall back to the
base guide. It's deterministic (exact matches only — never fuzzy), and it respects
manually-fixed guides.

> **Coverage tip — US locals.** A generic guide like myepg.top covers major
> cable/national channels well but is thin on US **local affiliates** (and has nothing
> for 24/7 loops — those have no schedule). To fill in your locals, add a **US-locals
> XMLTV** as a second EPG source in Dispatcharr. Failovarr has first-class support for
> these: US-locals feeds name each entry by station **callsign** (`KCEN-DT`, `WHDC-LD`),
> and Failovarr matches your local channels by callsign automatically — guide *and*
> logos, no plugin settings to change (it matches across all your EPG sources at once).
>
> A popular free option is **EPGShare**'s US-locals file:
>
> ```
> https://epgshare01.online/epgshare01/epg_ripper_US_LOCALS1.xml.gz
> ```
>
> They also publish `epg_ripper_US2.xml.gz` (national/cable) and `epg_ripper_US_SPORTS1.xml.gz`;
> EPGShare rotates filenames occasionally, so grab the current ones from
> <https://epgshare01.online/epgshare01/>. For the most robust US/Canada local coverage,
> **Schedules Direct** (paid) is the gold standard.

## PPV / live-event failover

*Experimental, opt-in — off by default.* Turn on **PPV events** to surface live PPV
events (normally skipped) as channels, and merge the **same event across both
providers** into one failover channel so it doesn't drop mid-event.

- Parses each provider's event name into an order-independent key — a `A vs B` matchup
  or a normalized title for races/single events (`ITALY: RACE`) — so trex and strong,
  which resell the same upstream with near-identical names, pair on the same event.
- Idle (`NO EVENT`) slots and finished events are filtered out.
- **Completely separate from your stable channels:** its own channel-number range
  (default `90000+`), grouped per package, and excluded from the stable set's health
  accounting — so PPV's fast churn can never disturb your main lineup.
- **Curated:** a light default filter drops only clear junk (high-school, per-team
  duplicate feeds, dead/foreign); racing, college and minor-league are kept. Curate
  further by disabling PPV groups in Dispatcharr (Failovarr only mines *enabled*
  groups) or by editing the skip regex.
- **Refreshes once daily** with the main reconcile by default — your client pulls the
  playlist about once a day anyway, and failover works during *playback* with no
  refresh. Set an intra-day interval only if your client refreshes more often.

**Refresh PPV events** is a clean on/off switch: with PPV events **on** it builds the
event channels; with it **off** it removes them all.

## Install

**Via plugin repo (recommended):** In Dispatcharr go to **Plugins → Repos → Add
repo** and paste:

```
https://raw.githubusercontent.com/roydufek/failovarr/main/manifest.json
```

Then find Failovarr in the available plugins and click install. Updates show up
automatically when a new version is published.

The release manifests are **GPG-signed**. To get the ✓ *Verified Signature* badge in
Dispatcharr, paste the public key below into the repo's **public key** field when you
add it. This is optional — installs work fine unsigned.

<details>
<summary><strong>Failovarr signing public key</strong></summary>

```
-----BEGIN PGP PUBLIC KEY BLOCK-----

mDMEapXFDRYJKwYBBAHaRw8BAQdAWfTghA3bApjxc6xXx4KHyy7XyvxQgHUzvW3E
gLTHPYa0LkZhaWxvdmFyciBTaWduaW5nIEtleSA8ZmFpbG92YXJyQHJveWR1ZmVr
LmNvbT6IkAQTFgoAOBYhBNtMr9OvVES3jVGdmGLm/HE84yd+BQJqlcUNAhsDBQsJ
CAcCBhUKCQgLAgQWAgMBAh4BAheAAAoJEGLm/HE84yd+ln4A/3p4UYwBwv9DqPsa
lNo21V1sZz767cb2EseyyB8RgzqUAPwJAp2kp14jEUcwUBRJtymQrF3fjwcf1XPb
Q+hTvLOrAQ==
=UkuY
-----END PGP PUBLIC KEY BLOCK-----
```
</details>

**Manual zip upload:** Download a release zip (or run `bash build.sh`) and use
**Plugins → Import** to upload it.

**Manual copy:** Copy this folder to `…/dispatcharr/data/plugins/failovarr/`, then
**Plugins → reload**.

## Settings

After installing, enable Failovarr and configure:

**Providers & scope**
- **Providers** — comma-separated M3U account names in failover priority order
  (e.g. `trex,strong`). The first is primary (order 0). Only streams in each account's
  **enabled groups** are considered.
- **Skip these groups (regex)** — PPV/event groups to leave to the PPV feature or
  native auto-sync.
- **Include 24/7 channels** · **Filter foreign-country channels** / **Always keep
  US-market channels** · **Region prefixes to strip** · **Merge quality variants**
  (HD + 4K → one) · **Skip junk/divider names**.

**Grouping, locals & profiles**
- **Tidy group names** — merge `ABC NETWORK` + `ABC`, drop resolution-only names.
- **Merge equivalent group names** — fold categories that mean the same thing into one
  guide group. One `FROM = TO` pair per line (or comma-separated), case-insensitive on
  the left, applied to the cleaned label. Ships with `SPORT = SPORTS` so a provider's
  singular `US| SPORT` shelf lands in the same **SPORTS** group as the plural one; add
  your own pairs, or clear the box to keep every group exactly as each provider names
  it. Only the pairs you list are merged — nothing is guessed.
- **Detect locals by name** — merge the same affiliate packaged under different groups.
- **Adult split** + **base / +18 profile names** + **adult detection**.
- **First channel number** · **Fallback channel group name**.

**EPG**
- **Match EPG guide + logos** · **Respect manual EPG mappings**.

**Schedule & notifications**
- **Daily reconcile time (HH:MM, UTC)** — blank disables. Reconcile only, never wipes.
- **Gotify** — off / on-failure / **on-change** / on-completion; set server URL + token.
- **Channel-drop safeguard** — high-priority alert + skip pruning on a provider-outage
  collapse.

**PPV (experimental, off by default)**
- **PPV events** — master toggle · **PPV categories to skip (regex)** · **minimum
  providers per event** · **PPV / event group match** · **Extra PPV refresh interval
  (0 = daily only)** · **First PPV channel number**.

## Actions

- **Preview (dry-run)** — reports exactly what the reconcile would change, writes
  nothing.
- **Reconcile now** — consolidate in place (add / update / prune by normalized name).
- **Match EPG + logos** — map channels to guide entries and refresh, on its own.
- **Preview PPV events** / **Refresh PPV events** — dry-run / build live PPV channels
  (or remove them all when PPV events is off).
- **Seed / reset** — delete all Failovarr-owned channels and rebuild from scratch (for
  the initial build or a deliberate clean re-seed only).
- **View last results** · **Clear operation lock**.

## Notes

- The container runs in UTC; schedule times are UTC.
- No external dependencies (pure stdlib + Django ORM).
- Failovarr **owns** its channels (the base/`+18` profile members) and reconciles them
  in place; it never mass-deletes as a side effect. The one destructive action, **Seed
  / reset**, is explicit, confirmed, and backed up first. PPV channels are tracked and
  numbered separately and never affect the stable set.

## Publishing a new version

1. Bump `__version__` in `plugin.py` and `version` in `plugin.json`, add a
   `CHANGELOG.md` entry, commit.
2. `git push github main` then `git tag vX.Y.Z && git push github vX.Y.Z`.
3. The `release` workflow builds `failovarr-X.Y.Z.zip`, computes its sha256, refreshes
   `manifest.json` + `plugin-manifest.json`, GPG-signs them, and creates the GitHub
   Release. Dispatcharr instances see the update on their next repo refresh.

The tag (`vX.Y.Z`) must match the version in `plugin.json`, or the workflow fails.
