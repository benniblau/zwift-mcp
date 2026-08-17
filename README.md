# zwift-mcp

Your Zwift training history in a local SQLite database, exposed to MCP
clients (Claude, and anything else that speaks MCP) and over a REST API.

Two components:

- **`zwift_downloader.py`** — a cron job that pulls from the Zwift game API
  and from ZwiftPower, parses the original FIT files, and stores everything
  in SQLite
- **`mcp_server.py`** — a stateless streamable-HTTP MCP server plus a REST
  API, both reading the same database behind one bearer token

## Why two sources

Zwift and ZwiftPower know different things, and neither is complete:

| | Zwift API | ZwiftPower |
|---|---|---|
| Every ride, including solo and workouts | ✅ | ❌ races only |
| Heart rate, cadence, speed, max power | ✅ detail endpoint | ✅ per race |
| Laps and per-second streams | only inside the FIT file | ❌ |
| Race position, category, field | ❌ | ✅ |
| Critical-power curve | ❌ | ✅ (races only) |
| Training load, CTL/ATL/TSB | ❌ | ❌ |

So the downloader lists activities from Zwift, calls the detail endpoint for
the summary fields the list omits, downloads and parses each FIT for laps and
streams, pulls race results from ZwiftPower, and computes training load
locally. Results from the two sites are linked by start time, since they
share no identifier — on real data that matches within about three minutes,
the time you spend in the pen before the flag drops.

## Setup

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env       # fill in ZWIFT_USER / ZWIFT_PASS
```

Generate a bearer token for the server:

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

Put it in `.env` as `ZWIFT_MCP_AUTH_TOKEN`.

Before the first sync, confirm the endpoints still look the way this code
expects — none of them are documented or stable:

```bash
.venv/bin/python probe_zwift_api.py
```

It writes `probe_zwift.json` (gitignored) with the raw payloads and prints a
summary. If a section reports an error, fix the mapping before syncing
rather than filling the database with NULLs.

## Syncing

```bash
.venv/bin/python zwift_downloader.py                 # incremental
.venv/bin/python zwift_downloader.py --days 30
.venv/bin/python zwift_downloader.py --since 2024-01-01
.venv/bin/python zwift_downloader.py --full          # re-fetch everything
.venv/bin/python zwift_downloader.py --with-samples  # + per-second streams
.venv/bin/python zwift_downloader.py --backfill-detail
.venv/bin/python zwift_downloader.py --redo-detail   # re-parse cached FIT files
.venv/bin/python zwift_downloader.py --skip-fit      # no FIT pass (fast)
.venv/bin/python zwift_downloader.py --zp-only       # ZwiftPower only
.venv/bin/python zwift_downloader.py --skip-zp       # game API only
.venv/bin/python zwift_downloader.py --with-zp-fields  # + full race fields
.venv/bin/python zwift_downloader.py --summary       # print stats, sync nothing
```

A nightly cron entry:

```cron
30 4 * * * cd /opt/zwift-mcp && .venv/bin/python zwift_downloader.py >> sync.log 2>&1
```

The first run is the slow one: it downloads a FIT file per activity. Later
runs only fetch what is new, and cached FITs are never re-downloaded.

## Running the server

```bash
.venv/bin/python mcp_server.py                    # HTTP (default), port 8081
.venv/bin/python mcp_server.py --transport stdio  # local Claude Desktop
```

In HTTP mode:

```
/mcp  and  /mcp/     MCP streamable HTTP endpoint (both spellings work)
/api/v1/...          REST API, same bearer token
/api/v1/health       liveness probe, unauthenticated
```

Connect an MCP client with the URL and `Authorization: Bearer <token>`.

## Production

`deploy/zwift-mcp.service` is a hardened systemd unit for an install under
`/opt/zwift-mcp`. Running it from a home directory instead means dropping
`ProtectHome` — it would hide the service's own working directory — and
pointing `ReadWritePaths` at the install path.

```bash
sudo cp deploy/zwift-mcp.service /etc/systemd/system/
sudo systemctl enable --now zwift-mcp
journalctl -u zwift-mcp -f
```

Pick a port that nothing else is using (`ss -tlnp`) and set it in `.env`;
one MCP server per port.

A nightly sync, staggered against whatever else runs on the box:

```cron
15 9 * * * cd /opt/zwift-mcp && .venv/bin/python zwift_downloader.py --days 10 > download.log 2>&1
```

To update a running deployment:

```bash
cd /opt/zwift-mcp && ./deploy/update.sh
```

That pulls, installs any new dependencies, restarts the unit and checks the
health endpoint — and refuses to run if tracked files have local edits.

`.env`, the database and `fits/` are gitignored, so a pull never touches
them. If the pull changes `schema/schema_zwift.sql`, note that there are no
migrations — delete the database and re-sync (cached FITs make it cheap).

The database is in WAL mode so the nightly sync and the running server do not
block each other. Keep it that way: under the default rollback journal, a
long recompute hands live queries "database is locked".

## MCP surface

**Resources** — `zwift://athlete`, `zwift://activities`,
`zwift://activities/recent`, `zwift://stats/summary`, `zwift://stats/monthly`,
`zwift://training/daily`, `zwift://power/curve`, `zwift://racing/results`

**Read tools**

| Tool | What it does |
|---|---|
| `query_activities` | Filter by sport, world, date, distance, duration, power, races |
| `get_activity_details` | Summary, laps, time in zone, that ride's curve, race result |
| `get_training_load` | Daily TSS with CTL / ATL / TSB |
| `get_training_trends` | Weekly or monthly volume |
| `get_training_zones` | Thresholds and the power/HR zones derived from them |
| `get_activity_stats` | Totals overall, by sport and by world |
| `get_power_curve` | Best mean power per duration, local vs ZwiftPower |
| `get_race_results` | ZwiftPower results with category and position |
| `get_race_details` | One race, plus the finishing field if synced |
| `get_athlete_profile` | Zwift profile, ZwiftPower profile, current form |
| `execute_sql` | Read-only SELECT against the whole database |

**Write tools** — `rename_activity` (pushes to Zwift, then updates locally),
`set_local_annotation` (local tags and notes, never sent anywhere), and
`get_activity_fit_file` (where the original FIT lives).

## REST API

```
GET    /api/v1/health                          unauthenticated
GET    /api/v1/athlete
GET    /api/v1/activities?sport=&world_id=&start_date=&races_only=&limit=
GET    /api/v1/activities/{id}?include_samples=
PATCH  /api/v1/activities/{id}                 {"name": …, "local_notes": …}
GET    /api/v1/activities/{id}/laps
GET    /api/v1/activities/{id}/samples?limit=&offset=
GET    /api/v1/stats/summary
GET    /api/v1/stats/monthly
GET    /api/v1/daily-metrics?start_date=&end_date=&limit=
GET    /api/v1/power-curve?source=local|zwiftpower|both
GET    /api/v1/races?start_date=&title_contains=&limit=
GET    /api/v1/races/{event_id}
GET    /api/v1/zwiftpower/profile
GET    /api/v1/sync-state
```

```bash
curl -H "Authorization: Bearer $ZWIFT_MCP_AUTH_TOKEN" \
     "http://localhost:8081/api/v1/activities?races_only=true&limit=5"
```

## Database

```
athletes                   — profile, FTP, weight, lifetime totals
worlds                     — world id lookup (seeded)
activities                 — one row per ride or run
activity_laps              — from the FIT lap messages
activity_samples           — per-second stream (only with --with-samples)
activity_zone_distribution — time in zone, computed from samples + FTP
power_curve                — best mean power per duration, per activity
segment_results            — segment efforts from the Zwift API
zp_profile                 — ZwiftPower category, zFTP, racing score
zp_results                 — one row per race
zp_event_results           — full finishing fields (--with-zp-fields)
zp_critical_power          — ZwiftPower's own CP curve
daily_metrics              — derived TSS, CTL, ATL, TSB per day
sync_state                 — per-dataset watermarks

Views:
  activity_summary  — km, km/h, w/kg, TSS
  monthly_stats     — by month and sport
  weekly_load       — weekly volume and TSS
  power_curve_best  — all-time best per duration, with the ride that set it
  race_results      — ZwiftPower results joined to the local activity
```

All stored values are SI: metres, seconds, watts, bpm, m/s. Conversions live
in the views.

## Things worth knowing

- **Training load is computed here, not fetched.** Each ride is scaled
  against the FTP Zwift held at the time (`profileFtp`), falling back to the
  current one. `ZWIFT_FTP_OVERRIDE` replaces a stale value.
- **`tss_source` tells you how a TSS was reached** — `np` from a parsed FIT,
  or `avg_power` estimated from the summary. The estimate understates a
  ride with big surges, so it is labelled rather than hidden.
- **Runs are scored against the bike FTP** unless `ZWIFT_RUN_FTP` is set.
  Zwift's running power is not the same quantity, so treat those TSS values
  as indicative.
- **The FIT file is the detail.** Without it there are no laps, no streams,
  no normalised power and no power curve.
- **Runs recorded without a power meter get no power curve or power zones.**
  Their FIT carries an all-zero power channel, which is absence, not data.
- **ZwiftPower's critical-power curve covers races only**, and only recent
  ones — an empty curve is a normal answer, not a failure.
- **The database has no migrations.** If a schema column changes, delete
  `zwift_activities.db` and re-sync; cached FITs in `fits/` mean nothing is
  re-downloaded.
- **ZwiftPower is a separate account link.** If the API returns nothing, open
  zwiftpower.com in a browser once and sign in with Zwift; the profile has to
  exist there before anything is queryable.
- **A ZwiftPower failure never fails the sync.** Race data is secondary to
  the game data, so an outage is logged in `sync_state` and skipped.
- **Neither API is public.** Field names and endpoints change without notice.
  `probe_zwift_api.py` exists to tell you which one broke.
