# Changelog

## v0.2.2

- **"Refresh PPV events" is now a clean on/off switch.** With **PPV events** ON it
  builds/updates the event channels as before; with it **OFF** a refresh removes every
  PPV channel. So turning PPV off and hitting Refresh cleanly deletes them (previously
  the button built them regardless of the toggle). The stable channel set is untouched
  either way.

## v0.2.1

- **PPV refreshes once daily by default, not every 30 min.** IPTV clients pull the
  playlist about once a day, so frequent PPV churn was mostly invisible to the client
  (and could create/expire an event entirely between client pulls). PPV now refreshes
  once, folded into the daily reconcile — and cross-provider failover works during
  playback with no refresh anyway. The intra-day cadence (`ppv_schedule_minutes`) is
  now opt-in (default 0), for clients that pull the playlist several times a day.

## v0.2.0

**PPV / live-event failover (experimental, opt-in).** The headline feature: surface
live PPV events — normally skipped — as channels, merging the SAME event across
providers into one failover channel so it doesn't drop mid-event.

- Parses provider event names into an order-independent key (a `A vs B` matchup, or a
  normalized title for races/single events like `ITALY: RACE`), so trex and strong —
  which resell the same upstream with near-identical names — pair on the same event.
- Idle (`NO EVENT`) slots and finished events are filtered; junk dividers rejected.
- **Kept entirely separate from your stable channels:** its own keymap, its own
  channel-number range (`ppv_number_start`, default 90000), and excluded from the
  stable set's health/seed accounting — PPV churn can never disturb the main lineup.
- **Light default filter:** a `ppv_skip` list drops only clear junk (high-school,
  per-team-duplicate feeds, 24/7, a dead service, obviously-foreign) — racing, college
  and minor-league are kept, since they have real audiences. Also respects Dispatcharr's
  enabled-group toggles, so you can curate at either layer to taste. `ppv_min_providers`
  (default 1) can restrict to cross-provider pairs only.
- **Own fast cadence:** refreshes every `ppv_schedule_minutes` (default 30) so live
  events appear/disappear promptly, independent of the daily reconcile.
- New actions: **Preview PPV events** (dry-run) and **Refresh PPV events now**.
- Off by default — enable **PPV events** in settings to try it.

## v0.1.9

- **Auto-names refresh when the channel changes — manual renames respected.** When a
  key's contents shift (e.g. the AMC/AMC+ split) the channel's display name is
  recomputed, but only if you haven't hand-renamed it: the keymap tracks the last
  auto-name, so a name you set yourself is never overwritten. Fixes channels left
  showing a stale name after a matching change.

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
