# Changelog

## v0.1.8

- **`+` brands stay distinct.** `AMC` and `AMC+` (and `Paramount`/`Paramount+`, etc.)
  are no longer collapsed into one channel — a trailing `+` is preserved in the
  matching key.
- **EPG matches across all sources.** Guide matching now uses every EPG source that
  has real entries, not just the largest — so adding a second guide (e.g. a US-locals
  XMLTV) raises coverage with no code change, and each involved source is refreshed.
- **Timeshift feeds share the base guide.** An `East`/`West`/`Pacific` feed falls back
  to the base channel's guide (e.g. `CINEMAX EAST` → `CINEMAX`) — a safe, deterministic
  match (same schedule, offset only), never fuzzy.

## v0.1.7

- **Locals group under their network.** A local station now lands in its network
  group (ABC/NBC/CBS/FOX) rather than whichever package feed was seen first, so an
  affiliate merged from an `ABC NETWORK` feed and a `DirecTV city` feed sits under
  `ABC` with the rest, not in a `DirecTV city` bucket.

## v0.1.6

- **Clean 4K group names.** Groups a provider names purely by resolution (e.g.
  `4K| UHD 3840P`) now become a tidy `4K` category instead of a stray `3840P` token.
- **Detect locals by name** (`locals_by_name`, default on). A channel whose name
  carries a network + callsign is treated as that local regardless of its group, so
  the same affiliate packaged under different groups (e.g. an `ABC NETWORK` feed and a
  `DirecTV city` feed of WSB Atlanta) merges into one channel by callsign.
- **Notify on change** (`gotify_notify` → "On change"). Pings you after a scheduled run
  only when it actually added, pruned or re-grouped channels — e.g. a provider added a
  new group — or if it failed. Quiet on no-op runs.

## v0.1.5

- **Tidy group names toggle** (`merge_group_suffixes`, default on). Drops a trailing
  generic word (NETWORK / CHANNEL / TV) from group labels, so categories that providers
  name differently merge — e.g. one provider's `ABC NETWORK` and another's `ABC` become
  a single `ABC` group; `NBC NETWORK` → `NBC`, etc. Turn off to keep each provider's
  exact wording.

## v0.1.4

- **Per-category channel groups.** Each consolidated channel now takes the channel
  group of its primary stream (cleaned: the `US|`/`US:` prefix stripped so trex and
  strong categories merge, superscripts folded), instead of everything landing in one
  flat "Failovarr" group. Restores the provider's category navigation
  (News/Sports/Locals/24-7/…) in the IPTV client. The reconcile re-groups existing
  channels in place.

## v0.1.3

Scheduler hardening (before enabling the daily reconcile):

- **Never runs while the plugin is disabled.** The scheduler tick now gates on the
  plugin's enabled state, so a disabled plugin can't keep firing the daily run or its
  notifications.
- **Hard cap of 3 attempts per day.** On failure the schedule retries a couple of
  times (spaced by the cooldown) then gives up until the next day — no indefinite
  15-minute retry loop and no notification spam. A success still stops further
  attempts for the day.

## v0.1.0

Initial release.

- **Cross-provider consolidation with failover.** Unions N IPTV providers (e.g.
  trex + strong) into single channels by deterministic normalized-name matching —
  no fuzzy scoring. A channel enabled on *either* provider is never lost; when both
  carry it, each provider's stream is stacked as failover in priority order.
- **Deterministic normalize-and-group.** Providers name the same channel
  near-identically (`US| CNN HD` vs `US: CNN HD`); the matcher strips the region
  prefix, folds superscripts, drops quality tags, and groups. Locals key on
  **callsign** (never city). PPV/event groups are skipped. Foreign-country prefixes
  are filtered (keeping US-market channels regardless of language).
- **Adult split.** Channels in adult groups route to the +18 profile only;
  everything else lands in both the base and +18 profiles.
- **Deterministic EPG matching + free logos.** Maps each channel to a real
  myepg.top guide entry by name (two-key strategy), sets `epg_data`/`tvg_id`, and
  triggers a refresh that pulls schedules and applies logos in one pass.
- **In-place reconcile — never wipe-and-rebuild.** Upserts by normalized key so a
  daily run preserves channel numbers, EPG mappings, and manual tweaks. A one-time
  **Seed / reset** action is provided for the initial build.
- **Safety machinery** carried over from streammirrarr: pre-run backups to a
  persistent directory, a channel-collapse health guard (skips destructive work +
  alerts on a provider-outage wipe), Gotify notifications, a cross-process run lock,
  and a daily scheduler.
