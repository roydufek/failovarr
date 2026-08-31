<p align="center">
  <img src="logo.png" alt="Failovarr" width="96" height="96" />
</p>

<h1 align="center">Failovarr</h1>

<p align="center">
  <strong>Cross-provider channel consolidation with failover for Dispatcharr.</strong>
</p>

<p align="center">
  <a href="#what-it-does">What it does</a> · <a href="#install">Install</a> · <a href="#how-matching-works">Matching</a> · <a href="#settings">Settings</a> · <a href="#actions">Actions</a> · <a href="#publishing-a-new-version">Publishing</a>
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
  stacked as failover (priority order), so a connection outage on one provider
  transparently falls over to the other.
- **Nothing is lost.** A channel only one provider carries is still kept
  (single-source), never dropped.
- **Locals merged by callsign.** US ABC/NBC/CBS/FOX affiliates match on their
  callsign (`KABC`), the only stable cross-provider identifier.
- **Adult split.** Adult channels route to a `+18` profile only; everything else
  lands in both the base and `+18` profiles.
- **Free EPG + logos.** Each channel is mapped to a real guide entry by name and a
  refresh pulls schedules and applies channel logos in one pass.

It reconciles **in place** — a daily run adds new channels, refreshes failover
ordering, and prunes only channels gone from *every* provider, so your channel
numbers, EPG mappings and manual tweaks are preserved.

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
- **Keep** non-region prefixes (`GO|` / `PRIME|` / `MU|`) in the key so distinct
  feeds don't over-merge, along with East/West and trailing numbers.
- **Local stations** key on callsign; city labels are inconsistent and sometimes
  wrong, so they're never used for matching.
- **PPV / event groups are skipped** — their dated one-off names would spawn
  thousands of stale channels. Hand those to Dispatcharr's native auto-channel-sync.
- **Foreign-country prefixes are filtered** (on country prefix, *not* language — US
  Spanish-language networks like Telemundo stay).

The union of every distinct key from any provider becomes the channel set. It's a
dictionary group-by: it runs in **seconds**, is safe to run daily, and never
produces the confident-but-wrong matches fuzzy scoring does.

## Install

**Via plugin repo (recommended):** In Dispatcharr go to **Plugins → Repos → Add
repo** and paste:

```
https://raw.githubusercontent.com/roydufek/failovarr/main/manifest.json
```

Then find Failovarr in the available plugins and click install. Updates show up
automatically when a new version is published.

The release manifests are **GPG-signed**. To get the ✓ *Verified Signature* badge
in Dispatcharr, paste the public key below into the repo's **public key** field when
you add it. This is optional — installs work fine unsigned.

<!-- FAILOVARR_PUBKEY: inserted at first signed release -->

**Manual zip upload:** Download a release zip (or run `bash build.sh`) and use
**Plugins → Import** to upload it.

**Manual copy:** Copy this folder to `…/dispatcharr/data/plugins/failovarr/`, then
**Plugins → reload**.

## Settings

After installing, enable Failovarr and configure:

- **Providers** — comma-separated M3U account names in failover priority order
  (e.g. `trex,strong`). The first is primary (order 0). Only streams in each
  account's **enabled groups** are considered.
- **Skip these groups (regex)** — PPV/event groups to skip; default covers
  `PPV|EVENT|ESPN+|FLO|NFHS|DAZN|…`.
- **Include 24/7 channels** — keep 24/7 loop channels (default on).
- **Filter foreign-country channels** / **Always keep US-market channels** — drop
  foreign-country prefixes while protecting US channels regardless of language.
- **Merge quality variants** — HD + 4K of the same channel become one (more
  failover) vs kept separate.
- **Adult split** + **base / +18 profile names** — route adult channels to the
  `+18` profile only.
- **Match EPG guide + logos** / **Respect manual EPG mappings** — deterministic
  name-based guide matching; don't overwrite hand-fixed guides.
- **First channel number** / **Channel group** — numbering and grouping for created
  channels.
- **Daily reconcile time (HH:MM, UTC)** — blank disables. Reconcile only, never
  seeds/wipes.
- **Gotify** — optional notifications for scheduled runs.
- **Channel-drop safeguard** — if the owned channel count collapses (provider
  outage), skip pruning and send a high-priority alert.

## Actions

- **Preview (dry-run)** — reports exactly what the reconcile would change, writes
  nothing.
- **Reconcile now** — consolidate in place (add / update / prune by normalized name).
- **Match EPG + logos** — map channels to guide entries and refresh, on its own.
- **Seed / reset** — delete all Failovarr-owned channels and rebuild from scratch.
  For the initial build or a deliberate clean re-seed only.
- **View last results** — the report from the most recent run.
- **Clear operation lock** — recover from an interrupted run.

## Notes

- The container runs in UTC; the schedule time is UTC.
- No external dependencies (pure stdlib + Django ORM).
- Failovarr **owns** its channels (the base/`+18` profile members) and reconciles
  them in place; it never mass-deletes as a side effect. The one destructive action,
  **Seed / reset**, is explicit, confirmed, and backed up first.

## Publishing a new version

1. Bump `__version__` in `plugin.py` and `version` in `plugin.json`, add a
   `CHANGELOG.md` entry, commit.
2. `git push github main` then `git tag vX.Y.Z && git push github vX.Y.Z`.
3. The `release` workflow builds `failovarr-X.Y.Z.zip`, computes its sha256,
   refreshes `manifest.json` + `plugin-manifest.json`, GPG-signs them, and creates
   the GitHub Release. Dispatcharr instances see the update on their next repo
   refresh.

The tag (`vX.Y.Z`) must match the version in `plugin.json`, or the workflow fails.
