# zwift-mcp — Claude Code Notes

## Project overview

Two-component system:
- **`zwift_downloader.py`** — cron job that fetches data from the Zwift game
  API and from ZwiftPower, parses the original FIT files, and stores
  everything in SQLite
- **`mcp_server.py`** — stateless streamable-HTTP MCP server **plus** a REST
  API, both exposing the same SQLite data

## Key files

| File | Purpose |
|------|---------|
| `zwift_downloader.py` | `ZwiftClient` (OAuth + API), `ZwiftPowerClient` (SSO + JSON), FIT parsing, `ZwiftDownloader` (sync + DB) |
| `mcp_server.py` | FastMCP resources/tools, REST routes, HTTP transport |
| `schema/schema_zwift.sql` | Full SQLite schema (14 tables, 5 views) |
| `probe_zwift_api.py` | Read-only endpoint prober; ground truth for field names |
| `.env` | Credentials and config |
| `.zwift_token.json` | Cached OAuth tokens (mode 600, gitignored) |
| `.zwiftpower_session.json` | Cached phpBB cookies (mode 600, gitignored) |
| `deploy/zwift-mcp.service` | systemd unit for production |

`ZwiftClient` is imported by `mcp_server.py` so the write tools reach Zwift
without duplicating authentication logic.

## Running locally

```bash
# Install dependencies (once)
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# Check the API still behaves as the code expects
.venv/bin/python probe_zwift_api.py

# Sync
.venv/bin/python zwift_downloader.py --days 30
.venv/bin/python zwift_downloader.py                 # incremental
.venv/bin/python zwift_downloader.py --backfill-detail
.venv/bin/python zwift_downloader.py --with-samples
.venv/bin/python zwift_downloader.py --redo-detail   # re-parse cached FITs
.venv/bin/python zwift_downloader.py --skip-fit      # summary + ZwiftPower only
.venv/bin/python zwift_downloader.py --zp-only
.venv/bin/python zwift_downloader.py --summary

# Start server (HTTP, default)
.venv/bin/python mcp_server.py

# Start server (stdio override)
.venv/bin/python mcp_server.py --transport stdio
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `ZWIFT_USER` | Zwift account email |
| `ZWIFT_PASS` | Zwift account password |
| `ZWIFT_ATHLETE_ID` | Optional; skips the profile lookup |
| `ZWIFTPOWER_USER` / `ZWIFTPOWER_PASS` | Only if ZwiftPower differs from Zwift |
| `ZWIFT_DB_PATH` | SQLite path (default `./zwift_activities.db`) |
| `ZWIFT_FIT_CACHE` | Where FIT files are cached (default `./fits`) |
| `ZWIFT_START_DATE` | Earliest date for a first sync (default 2 years back) |
| `ZWIFT_FTP_OVERRIDE` | FTP for all derived metrics, overriding the profile |
| `ZWIFT_RUN_FTP` | Separate threshold for runs; without it they use the bike FTP |
| `ZWIFT_MCP_TRANSPORT` | `http` (default) or `stdio` |
| `ZWIFT_MCP_AUTH_TOKEN` | Bearer token — guards **both** MCP and REST |
| `ZWIFT_MCP_HTTP_HOST` | Bind address (default `0.0.0.0`) |
| `ZWIFT_MCP_HTTP_PORT` | Port (default `8081`; coros-mcp holds 8080) |

## Zwift API notes

The private game API at `us-or-rly101.zwift.com`. Everything below was
verified against a live account with `probe_zwift_api.py` on 2026-08-17.

- **Auth is real OAuth.** A password grant against Keycloak at
  `secure.zwift.com/auth/realms/zwift/…/token` with `client_id=Zwift_Mobile_Link`.
  There **is** a refresh grant, so `authenticate()` refreshes silently and a
  full login only happens when the refresh is rejected. Unlike COROS,
  multiple sessions coexist — the game, the app and this client can all be
  logged in at once.
- **`expires_in` is in milliseconds** (`86400000`), not seconds as OAuth
  specifies. Taken at face value it parks the refresh a thousand days out and
  every call ends up recovering from a 401 instead. `_token_request()`
  divides anything over 1e6 by 1000.
- **Ask for JSON.** Several endpoints answer with protobuf unless
  `Accept: application/json` is set. `request()` always sets it, and raises a
  clear error if a non-JSON body comes back anyway.
- **The list and the detail endpoint return different fields**, and the
  difference matters:
  - The **list** (`/api/profiles/{id}/activities`) has no heart rate, no
    cadence, no speed and no max power. It is enumeration plus `avgWatts`.
  - The **detail** (`/api/activities/{id}`) adds `avgHeartRate`,
    `maxHeartRate`, `avg/maxCadenceInRotationsPerMinute`,
    `avg/maxSpeedInMetersPerSecond`, `maxWatts` — and two values worth more
    than they look: `profileFtp`, the FTP in force when the ride happened,
    and `profileMaxHeartRate`, which `/api/profiles/me` does **not** return.
  That is why `enrich_activity_details()` always runs and is not behind a
  flag: without it there is no heart rate anywhere in the database.
- **`duration` is whole minutes**, not seconds and not a display string. Use
  the timestamps; `_activity_row()` only falls back to it last.
- **`utcOffsetMinutes` is how you get the local day.** Stored as
  `utc_offset_minutes` and used for `activities.date` and for the
  `daily_metrics` grouping, so a 00:30 ride counts against the night it
  happened rather than the UTC day after.
- **Laps, streams and NP still come from the FIT.** The API has no lap or
  sample data at all — see `parse_fit()`. An activity with no `fitFileKey`
  can never have those.
- **FIT downloads are plain S3 objects**, not API calls: no Authorization
  header, and they do not go through `request()`.
- **A FIT without a power meter still has a power channel — all zeros.**
  `download_activity_detail()` checks `has_power` before computing NP, work,
  max power, the curve or power zones; without that check every Zwift run
  reports NP 0 and "100% of the ride in Z1". Zwift's estimated running power
  lives on the API summary, not in the file, so `avg_power` survives.
- **Response shapes drift.** The activity list has returned both a bare list
  and an envelope; `get_activities()` accepts either. Field names have moved
  too (`id`/`id_str`/`activityId`, `name`/`activityName`), which is why
  `_activity_row()` uses fallbacks and always keeps `raw_json`.
- **Weight is grams and height is millimetres** in the profile;
  `download_profile()` normalises both.
- **`start=` paging works** — verified, not assumed. Page size 30.
- **Write support is thin.** There is no partial update: `rename_activity()`
  re-reads the whole activity and PUTs it back with only the title changed.
- **`give_ride_on()` exists on the client but is deliberately not an MCP
  tool.** It posts to another athlete's activity — a social action taken in
  your name. Adding it means deciding that a bearer token is enough
  authorisation for that; decide deliberately before doing so.

## ZwiftPower notes

- **No API and no token.** ZwiftPower is a phpBB site behind Zwift SSO
  (`client_id=zwiftpower-public`). Signing in means following
  `ucp.php?mode=login&login=external&…` into Keycloak, scraping the login
  form's `action` (it carries the execution state, so it cannot be
  constructed), posting the credentials, and letting the redirect chain set
  phpBB cookies. Those cookies are cached in `.zwiftpower_session.json` and
  reused for 20 hours.
- **The login form has no `id="kc-form-login"`.** That is the id every
  Keycloak example uses, and Zwift's theme does not set it. The fallback
  pattern — any form posting to `secure.zwift.com` — is the one that
  actually fires; keep both.
- **There is no cheap "am I signed in" endpoint.** `api3.php?do=profile_list`
  answers `200` with a zero-byte body whether or not you are authenticated,
  so calling `.json()` on it throws and makes a good session look dead. This
  cost an hour: `_verify_session()` reads phpBB's own `phpbb3_*_u` cookie
  instead (user id 1 is the guest).
- **A logged-out data request returns HTML, not a 401.** That is why
  `_json()` treats an unparseable body as an expired session and retries once
  after re-authenticating.
- **The account must be linked once in a browser.** A Zwift account that has
  never visited zwiftpower.com has no profile there and every endpoint comes
  back empty — which looks exactly like a broken scraper.
- **Values arrive as `[value, formatting_flag]`**, not scalars. Everything
  goes through `_zp()` / `_zp_num()`, which also try several candidate key
  names because they differ between the `cache3` JSON and `api3.php`.
  Empty cells are `['', 0]`, so "missing" and "zero" are distinguishable
  only after `_num()` rejects the empty string.
- **`age` is a band, not a number** — "Vet", "Veteran", "Snr". Both `age`
  columns are TEXT for that reason.
- **The profile header must be read by label, not by CSS class.** The
  in-game level badge is also `label-cat-E`, sits above the real category,
  and will answer in its place — it agreed with the true category by
  coincidence on the first account tested, which is exactly how that kind of
  bug survives. `_download_zp_profile()` anchors on
  "Category (Pace Group)" and reads every other value from the `<td>` beside
  its own `<th>`. `&nbsp;` becomes `\xa0` after unescaping and defeats
  `.strip()`/`.split()` unless normalised.
- **`zFTP` and `Zwift Racing Score` are often `---`.** ZwiftPower shows a
  dash when it has no value, which is a legitimate empty result, not a
  scrape failure.
- **The critical-power endpoint returns `{"info": [], "efforts": []}`** — an
  empty *list* — for a rider with no recent races, and the curve is built
  from races only. All four parameter spellings behave identically, and
  `cache3/profile/{id}_cp.json` does not exist (403). An empty curve is
  normal; only a dict of duration → points carries data.
- **ZwiftPower ids are not Zwift ids.** `zp_results.event_id` is a
  ZwiftPower race (`zid`). The event-results rows carry an `actid` field
  that looks like the missing join key but is empty in practice, so
  `link_zp_results()` matches on start time within an hour and writes
  `activities.zp_event_id`. On real data every match was within three
  minutes — the activity starts slightly *before* the event, because the
  clock starts in the pen. Unlinked results are expected, not a bug.
- **A ZwiftPower failure must never fail the sync.** The site has outages,
  and its long-term future has been uncertain for a while. Errors are caught,
  recorded in `sync_state` and skipped — the game API is the primary source.
- **ZwiftPower's CP curve only sees races**, so it routinely disagrees with
  the locally derived `power_curve`. They are stored separately on purpose;
  `get_power_curve(source='both')` shows the two side by side.

## Database schema

```
athletes                   — profile, FTP, weight, lifetime totals
worlds                     — world id lookup (seeded in the schema)
activities                 — one row per activity, summary + FIT-derived detail
activity_laps              — from the FIT lap messages
activity_samples           — per-second streams (only with --with-samples)
activity_zone_distribution — time in zone, computed locally
power_curve                — best mean power per duration, per activity
segment_results            — segment efforts from the Zwift API
zp_profile                 — ZwiftPower category, zFTP, racing score
zp_results                 — one row per race
zp_event_results           — full finishing fields (--with-zp-fields)
zp_critical_power          — ZwiftPower's own CP curve
daily_metrics              — derived TSS, CTL, ATL, TSB per day
sync_state                 — per-dataset watermarks

Views:
  activity_summary  — km, km/h, w/kg, TSS conversions
  monthly_stats     — by month and sport
  weekly_load       — weekly volume and TSS
  power_curve_best  — all-time best per duration
  race_results      — ZwiftPower results joined to activities
```

All stored values are SI; conversions live in the views.

`schema/schema_zwift.sql` is the single source of truth — `init_database()`
executes the file directly, so the DDL exists in exactly one place. Every DDL
statement is `IF NOT EXISTS`, so re-running is safe. **Views are dropped and
recreated** on each run, because `CREATE VIEW IF NOT EXISTS` would keep a
stale definition in an existing database and view fixes would never take
effect.

**There are no migrations.** Every DDL statement is `IF NOT EXISTS`, so a
column added to the schema file does not appear in a database that already
exists, and the next sync fails with `no such column`. The database is a
cache: delete it and re-sync. That is cheap because `fits/` is kept — the
FIT files are not re-downloaded, only re-parsed.

Writes use `INSERT OR REPLACE`. `_merge_activity()` carries forward the
FIT-derived columns, `zp_event_id` and the local annotations, since
`INSERT OR REPLACE` rewrites the whole row and a routine re-sync would
otherwise wipe everything the list endpoint cannot see.

## Derived metrics

Zwift has no training-load service, so these are computed here:

- **NP** — 30-second rolling average power, fourth power, mean, fourth root.
- **IF / TSS** — scaled by the ride's own `profile_ftp` where the detail sync
  captured it, falling back to `ftp()`, which prefers `ZWIFT_FTP_OVERRIDE`,
  then the in-game FTP, then ZwiftPower's zFTP. Using today's FTP for a ride
  from three years ago rewrites its intensity permanently, which is why the
  per-ride value wins.
- **`tss_source` says how a TSS was reached**: `np` from a parsed FIT, or
  `avg_power` estimated from the summary. The estimate understates a
  variable ride. `recompute_training_load()` writes the estimate onto the
  activity rather than only into the daily total — otherwise
  `activities.tss` and `daily_metrics.tss` answer the same question with
  different numbers depending on which one a query happens to read.
- **Running is scored against `ZWIFT_RUN_FTP` when set.** Without it, runs
  fall back to the cycling FTP, which mixes two different quantities: Zwift's
  running power is not comparable to a bike FTP, so those TSS values are
  indicative only.
- **CTL / ATL / TSB** — exponentially weighted averages of daily TSS with 42
  and 7 day time constants. Every day depends on the one before, including
  rest days, so `recompute_training_load()` rebuilds the whole table rather
  than appending. It is cheap; do not "optimise" it into an incremental
  update without handling gaps.
- **Power curve** — `power_series()` builds a real one-sample-per-second
  array indexed by elapsed time, filling gaps with zeros. Do not compute best
  efforts over the raw record list: a ride with dropped samples would report
  a 20-minute best that covers more than 20 minutes of clock time.

## Safety invariants

Keep these when editing:

- **`order_by` is interpolated into SQL**, so it must be validated against
  `ALLOWED_ORDER_COLUMNS`. Never pass caller input into an ORDER BY unchecked.
- **`execute_sql` is read-only**: it rejects non-SELECT, rejects stacked
  statements, and sets `PRAGMA query_only = ON` as defence in depth.
- **The REST API is bearer-guarded** by the `guard()` wrapper. Only
  `/api/v1/health` is intentionally unauthenticated.
- **Both `/mcp` and `/mcp/` must work.** `_accept_bare_mcp_path` rewrites the
  bare path *outside* the router; a Starlette `Mount` would otherwise answer
  bare `/mcp` with a 307 redirect that some clients drop auth headers on.
- Write tools push to Zwift **first**, then update the local row, so the
  local database never claims a change that Zwift rejected.
- **Nothing here deletes an activity.** Zwift has no import path to undo one,
  so unlike coros-mcp there is no safe two-step delete to offer. Do not add
  one without a recovery story.
- **Credential caches are mode 600** and gitignored: `.zwift_token.json`
  holds a live refresh token, `.zwiftpower_session.json` a live session.
- **`probe_zwift.json` is gitignored** — it contains profile and activity
  data verbatim.

## Modeled after

`../coros-mcp` — same two-component pattern, same HTTP stack, same REST-on-
the-same-port-and-token design. Differences all follow from the source:
OAuth with refresh instead of region probing, FIT parsing instead of a detail
endpoint, locally computed training load instead of EvoLab, and a second
upstream (ZwiftPower) with its own session and its own ids.
