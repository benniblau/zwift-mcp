-- Zwift Activities Database Schema
-- Two upstream sources, kept in clearly separated tables:
--   * the private Zwift game API (us-or-rly101.zwift.com) — activities,
--     profile, and the original FIT files that hold all per-second data
--   * ZwiftPower (zwiftpower.com) — race results, category, zFTP and the
--     critical-power curve. Everything sourced there is prefixed `zp_`.
--
-- UNIT CONVENTIONS
--   Every stored value is SI: metres, seconds, watts, bpm, rpm, m/s.
--   The Zwift API reports distance in metres but durations in milliseconds,
--   and the FIT file reports speed in m/s and coordinates in semicircles.
--   The downloader normalises all of that before insert. Conversions to km,
--   km/h, pace and hours happen in the views at the bottom of this file.
--
--   Derived training metrics (NP, IF, TSS, CTL, ATL, TSB) are computed
--   locally — Zwift has no equivalent of Garmin's or COROS's training-load
--   service — so they are only as good as the FTP in `athletes.ftp`.

-- ============================================================
-- Account
-- ============================================================

-- Zwift profile. One row per athlete; normally just you, but the schema
-- allows storing others so ZwiftPower race fields can name their riders.
CREATE TABLE IF NOT EXISTS athletes (
    zwift_id            TEXT PRIMARY KEY,
    first_name          TEXT,
    last_name           TEXT,
    display_name        TEXT,
    male                INTEGER,
    age                 INTEGER,
    country_code        TEXT,
    ftp                 REAL,          -- watts, as set in the game
    weight              REAL,          -- kg  (API reports grams)
    height              REAL,          -- cm  (API reports millimetres)
    max_hr              INTEGER,       -- bpm, from the game settings if present
    rest_hr             INTEGER,
    level               INTEGER,       -- achievementLevel / 100
    run_level           INTEGER,
    total_distance      REAL,          -- metres, lifetime
    total_climbed       REAL,          -- metres, lifetime
    total_time_s        REAL,          -- seconds, lifetime
    total_xp            INTEGER,
    is_self             INTEGER DEFAULT 0,   -- 1 for the authenticated athlete
    raw_json            TEXT,
    synced_at           TEXT
);

-- ============================================================
-- Lookups
-- ============================================================

-- Zwift world ids. Seeded below; unknown ids still store fine and simply
-- render as NULL in the joins.
CREATE TABLE IF NOT EXISTS worlds (
    world_id            INTEGER PRIMARY KEY,
    name                TEXT NOT NULL
);

-- ============================================================
-- Activities
-- ============================================================

-- One row per activity, assembled from three sources:
--
--   the activity LIST endpoint  — enumeration and the basic summary
--   the activity DETAIL endpoint — heart rate, cadence, speed, max power,
--                                  and the FTP in force at the time
--   the original FIT file        — laps, per-second streams, NP, work, and
--                                  everything derived from them
--
-- The list alone has no heart rate at all, so a summary-only sync leaves
-- those columns NULL until enrich_activity_details() runs.
CREATE TABLE IF NOT EXISTS activities (
    activity_id         TEXT PRIMARY KEY,
    profile_id          TEXT,
    name                TEXT,
    description         TEXT,
    sport               TEXT,              -- CYCLING, RUNNING, ...
    world_id            INTEGER,
    start_time          INTEGER,           -- unix epoch seconds, UTC
    end_time            INTEGER,
    date                INTEGER,           -- YYYYMMDD in the rider's local time
    utc_offset_minutes  INTEGER,           -- local offset the ride was saved with
    duration_s          REAL,              -- elapsed
    moving_time_s       REAL,
    distance            REAL,              -- metres
    elevation_gain      REAL,              -- metres
    calories            REAL,              -- kcal

    avg_power           REAL,              -- watts
    max_power           REAL,              -- from the detail endpoint
    np                  REAL,              -- normalised power, from FIT
    intensity_factor    REAL,              -- np / ftp, computed
    tss                 REAL,              -- training stress score, computed
    -- How that TSS was arrived at: 'np' from the FIT file's power stream,
    -- or 'avg_power' estimated from the summary because no FIT was parsed.
    -- An estimate understates a variable ride; keep the two distinguishable.
    tss_source          TEXT,
    work_kj             REAL,              -- from FIT
    avg_wkg             REAL,              -- avg_power / weight at the time
    profile_ftp         REAL,              -- the FTP Zwift had at ride time

    avg_hr              INTEGER,           -- from the detail endpoint
    max_hr              INTEGER,
    avg_cadence         INTEGER,
    max_cadence         INTEGER,
    avg_speed           REAL,              -- m/s
    max_speed           REAL,              -- m/s

    privacy             TEXT,
    ride_on_count       INTEGER,
    comment_count       INTEGER,

    -- The FIT file is the detail source. Bucket and key come from the API;
    -- fit_path points at the local cache once downloaded.
    fit_bucket          TEXT,
    fit_key             TEXT,
    fit_path            TEXT,
    fit_synced_at       TEXT,
    api_detail_synced_at TEXT,             -- set when the detail endpoint ran
    detail_synced_at    TEXT,              -- set when laps/curve were derived
    sample_count        INTEGER DEFAULT 0,

    -- Link to the ZwiftPower result for the same race, resolved by
    -- link_zp_results() on start time and title. NULL for non-race rides.
    zp_event_id         TEXT,

    -- Local-only annotations. Never sent anywhere.
    local_tags          TEXT,
    local_notes         TEXT,

    raw_json            TEXT,
    synced_at           TEXT
);

CREATE INDEX IF NOT EXISTS idx_activities_profile ON activities(profile_id);
CREATE INDEX IF NOT EXISTS idx_activities_date ON activities(date);
CREATE INDEX IF NOT EXISTS idx_activities_start ON activities(start_time);
CREATE INDEX IF NOT EXISTS idx_activities_sport ON activities(sport);
CREATE INDEX IF NOT EXISTS idx_activities_world ON activities(world_id);
CREATE INDEX IF NOT EXISTS idx_activities_detail ON activities(detail_synced_at);
CREATE INDEX IF NOT EXISTS idx_activities_zp ON activities(zp_event_id);

-- Laps, from the FIT lap messages. Zwift writes one lap per manual lap press
-- and one per workout interval, so lap_index is the only ordering key.
CREATE TABLE IF NOT EXISTS activity_laps (
    activity_id         TEXT NOT NULL,
    lap_index           INTEGER NOT NULL,
    start_time          INTEGER,
    elapsed_time_s      REAL,
    timer_time_s        REAL,
    distance            REAL,
    avg_power           REAL,
    max_power           REAL,
    np                  REAL,
    avg_hr              INTEGER,
    max_hr              INTEGER,
    avg_cadence         INTEGER,
    max_cadence         INTEGER,
    avg_speed           REAL,
    max_speed           REAL,
    ascent              REAL,
    descent             REAL,
    calories            REAL,
    PRIMARY KEY (activity_id, lap_index)
);

CREATE INDEX IF NOT EXISTS idx_laps_activity ON activity_laps(activity_id);

-- Per-second sample stream from the FIT record messages. Large — a single
-- two-hour ride is ~7200 rows — so it is only populated with --with-samples.
CREATE TABLE IF NOT EXISTS activity_samples (
    activity_id         TEXT NOT NULL,
    sample_index        INTEGER NOT NULL,  -- seconds from the first record
    timestamp           INTEGER,           -- unix epoch seconds
    distance            REAL,              -- metres, cumulative
    altitude            REAL,              -- metres
    speed               REAL,              -- m/s
    power               REAL,              -- watts
    heart_rate          INTEGER,
    cadence             INTEGER,
    grade               REAL,              -- percent, where recorded
    latitude            REAL,              -- virtual world coordinates
    longitude           REAL,
    PRIMARY KEY (activity_id, sample_index)
);

CREATE INDEX IF NOT EXISTS idx_samples_activity ON activity_samples(activity_id);

-- Time in zone, computed locally from the samples and the athlete's
-- thresholds. metric_type is 'power' or 'hr'; zone_index is 1-based.
CREATE TABLE IF NOT EXISTS activity_zone_distribution (
    activity_id         TEXT NOT NULL,
    metric_type         TEXT NOT NULL,
    zone_index          INTEGER NOT NULL,
    zone_name           TEXT,
    lower_bound         REAL,
    upper_bound         REAL,              -- NULL on the open-ended top zone
    seconds             REAL,
    percent             REAL,
    PRIMARY KEY (activity_id, metric_type, zone_index)
);

CREATE INDEX IF NOT EXISTS idx_zones_activity ON activity_zone_distribution(activity_id);

-- Best mean power over fixed durations, per activity. Derived from the
-- samples during detail sync; the all-time curve is the view below.
CREATE TABLE IF NOT EXISTS power_curve (
    activity_id         TEXT NOT NULL,
    duration_s          INTEGER NOT NULL,
    watts               REAL,
    wkg                 REAL,
    PRIMARY KEY (activity_id, duration_s)
);

CREATE INDEX IF NOT EXISTS idx_power_curve_duration ON power_curve(duration_s);

-- Segment efforts (Alpe d'Huez, Epic KOM, sprints) from the Zwift
-- segment-results endpoint. Ranks are as reported at fetch time.
CREATE TABLE IF NOT EXISTS segment_results (
    result_id           TEXT PRIMARY KEY,
    segment_id          TEXT,
    segment_name        TEXT,
    world_id            INTEGER,
    activity_id         TEXT,
    zwift_id            TEXT,
    event_id            TEXT,
    start_time          INTEGER,
    elapsed_time_s      REAL,
    avg_power           REAL,
    avg_wkg             REAL,
    avg_hr              INTEGER,
    rank                INTEGER,
    raw_json            TEXT,
    synced_at           TEXT
);

CREATE INDEX IF NOT EXISTS idx_segment_results_segment ON segment_results(segment_id);
CREATE INDEX IF NOT EXISTS idx_segment_results_activity ON segment_results(activity_id);

-- ============================================================
-- ZwiftPower
--
-- ZwiftPower is a separate site with its own session (Zwift SSO) and its own
-- ids: `event_id` here is a ZwiftPower race id (`zid`), NOT a Zwift activity
-- id. The two are joined heuristically by link_zp_results(), which writes
-- activities.zp_event_id — treat an unlinked result as normal, not as a bug.
-- ============================================================

-- ZwiftPower's view of a rider: category, zFTP, racing score, current team.
CREATE TABLE IF NOT EXISTS zp_profile (
    zwift_id            TEXT PRIMARY KEY,
    name                TEXT,
    category            TEXT,              -- A / B / C / D / E
    racing_score        REAL,
    zftp                REAL,              -- watts
    zmap                REAL,              -- watts
    weight              REAL,              -- kg
    height              REAL,              -- cm
    age                 TEXT,              -- an age band ("Vet"), not a number
    country             TEXT,
    team                TEXT,
    div                 INTEGER,
    races_count         INTEGER,
    raw_json            TEXT,
    synced_at           TEXT
);

-- One row per race the athlete finished, as ZwiftPower scores it.
CREATE TABLE IF NOT EXISTS zp_results (
    event_id            TEXT NOT NULL,
    zwift_id            TEXT NOT NULL,
    activity_id         TEXT,              -- resolved by link_zp_results()
    event_title         TEXT,
    event_date          INTEGER,           -- unix epoch seconds
    category            TEXT,
    position            INTEGER,           -- overall
    position_in_cat     INTEGER,
    finishers_in_cat    INTEGER,
    time_s              REAL,
    time_gap_s          REAL,              -- behind the category winner
    avg_power           REAL,
    np                  REAL,
    avg_wkg             REAL,
    zp_ftp              REAL,              -- ZwiftPower's wFTP for this ride
    avg_hr              INTEGER,
    max_hr              INTEGER,
    weight              REAL,
    height              REAL,
    age                 TEXT,              -- an age band ("Vet"), not a number
    -- Best w/kg over the standard ZwiftPower durations for this race.
    wkg_5s              REAL,
    wkg_15s             REAL,
    wkg_30s             REAL,
    wkg_60s             REAL,
    wkg_5m              REAL,
    wkg_20m             REAL,
    w_5s                REAL,
    w_15s               REAL,
    w_30s               REAL,
    w_60s               REAL,
    w_5m                REAL,
    w_20m               REAL,
    flagged             INTEGER,           -- ZwiftPower "zada"/upgrade flags
    raw_json            TEXT,
    synced_at           TEXT,
    PRIMARY KEY (event_id, zwift_id)
);

CREATE INDEX IF NOT EXISTS idx_zp_results_date ON zp_results(event_date);
CREATE INDEX IF NOT EXISTS idx_zp_results_activity ON zp_results(activity_id);

-- The rest of the field in a race the athlete rode. Only populated with
-- --with-zp-fields, since it is one extra request per race.
CREATE TABLE IF NOT EXISTS zp_event_results (
    event_id            TEXT NOT NULL,
    zwift_id            TEXT NOT NULL,
    name                TEXT,
    team                TEXT,
    category            TEXT,
    position            INTEGER,
    position_in_cat     INTEGER,
    time_s              REAL,
    avg_power           REAL,
    avg_wkg             REAL,
    weight              REAL,
    raw_json            TEXT,
    synced_at           TEXT,
    PRIMARY KEY (event_id, zwift_id)
);

CREATE INDEX IF NOT EXISTS idx_zp_event_results_event ON zp_event_results(event_id);

-- ZwiftPower's own critical-power curve for the athlete. Kept separate from
-- `power_curve` because it is computed by ZwiftPower over race data only,
-- and therefore routinely disagrees with the locally derived curve.
CREATE TABLE IF NOT EXISTS zp_critical_power (
    zwift_id            TEXT NOT NULL,
    duration_s          INTEGER NOT NULL,
    watts               REAL,
    wkg                 REAL,
    effort_date         INTEGER,           -- when the best effort was set
    event_id            TEXT,
    synced_at           TEXT,
    PRIMARY KEY (zwift_id, duration_s)
);

-- ============================================================
-- Derived training metrics
--
-- Computed by recompute_training_load() from the activities table, not
-- fetched. Re-derived in full on every run, so the whole table is a cache.
-- ============================================================

CREATE TABLE IF NOT EXISTS daily_metrics (
    calendar_date       TEXT PRIMARY KEY,  -- YYYY-MM-DD
    activity_count      INTEGER,
    duration_s          REAL,
    distance            REAL,
    elevation_gain      REAL,
    tss                 REAL,              -- total for the day
    ctl                 REAL,              -- chronic training load, 42d
    atl                 REAL,              -- acute training load, 7d
    tsb                 REAL,              -- ctl - atl (form)
    ftp_used            REAL,              -- FTP the TSS was computed against
    updated_at          TEXT
);

-- ============================================================
-- Sync bookkeeping
-- ============================================================

-- Watermark per dataset so incremental runs know where to resume without
-- re-deriving it from MAX() over the data every time.
CREATE TABLE IF NOT EXISTS sync_state (
    dataset             TEXT PRIMARY KEY,
    last_cursor         TEXT,
    last_synced_at      TEXT,
    records             INTEGER,
    status              TEXT,
    message             TEXT
);

-- ============================================================
-- Views
--
-- Views are derived objects, so they are dropped and recreated on every run.
-- CREATE VIEW IF NOT EXISTS would silently keep a stale definition in any
-- database that already exists, meaning view fixes never take effect.
-- ============================================================

DROP VIEW IF EXISTS activity_summary;
CREATE VIEW activity_summary AS
SELECT
    a.activity_id,
    a.name,
    a.sport,
    COALESCE(w.name, 'World ' || a.world_id) AS world,
    date(a.start_time, 'unixepoch')          AS activity_date,
    datetime(a.start_time, 'unixepoch')      AS start_time_utc,
    ROUND(a.distance / 1000.0, 2)            AS distance_km,
    ROUND(a.duration_s / 60.0, 1)            AS duration_min,
    ROUND(a.moving_time_s / 60.0, 1)         AS moving_min,
    ROUND(a.elevation_gain, 0)               AS elevation_m,
    ROUND(a.avg_speed * 3.6, 2)              AS avg_speed_kmh,
    ROUND(a.max_speed * 3.6, 2)              AS max_speed_kmh,
    ROUND(a.avg_power, 0)                    AS avg_watts,
    ROUND(a.max_power, 0)                    AS max_watts,
    ROUND(a.np, 0)                           AS np_watts,
    ROUND(a.avg_wkg, 2)                      AS avg_wkg,
    ROUND(a.intensity_factor, 3)             AS intensity_factor,
    ROUND(a.tss, 1)                          AS tss,
    a.tss_source,
    ROUND(a.work_kj, 0)                      AS work_kj,
    a.avg_hr,
    a.max_hr,
    a.avg_cadence,
    ROUND(a.calories, 0)                     AS calories,
    a.zp_event_id IS NOT NULL                AS is_race,
    a.detail_synced_at IS NOT NULL           AS has_detail,
    a.sample_count,
    a.local_tags,
    a.local_notes
FROM activities a
LEFT JOIN worlds w ON a.world_id = w.world_id;

DROP VIEW IF EXISTS monthly_stats;
CREATE VIEW monthly_stats AS
SELECT
    strftime('%Y-%m', a.start_time, 'unixepoch') AS month,
    a.sport,
    COUNT(*)                                     AS activity_count,
    ROUND(SUM(a.distance) / 1000.0, 1)           AS total_km,
    ROUND(SUM(a.duration_s) / 3600.0, 2)         AS total_hours,
    ROUND(SUM(a.elevation_gain), 0)              AS total_elevation_m,
    ROUND(SUM(a.work_kj), 0)                     AS total_work_kj,
    ROUND(SUM(a.tss), 0)                         AS total_tss,
    ROUND(AVG(NULLIF(a.avg_power, 0)), 0)        AS avg_watts,
    ROUND(AVG(NULLIF(a.avg_hr, 0)), 0)           AS avg_heartrate
FROM activities a
WHERE a.start_time IS NOT NULL
GROUP BY month, a.sport
ORDER BY month DESC;

DROP VIEW IF EXISTS weekly_load;
CREATE VIEW weekly_load AS
SELECT
    strftime('%Y-W%W', a.start_time, 'unixepoch') AS week,
    COUNT(*)                                      AS activity_count,
    ROUND(SUM(a.distance) / 1000.0, 1)            AS total_km,
    ROUND(SUM(a.duration_s) / 3600.0, 2)          AS total_hours,
    ROUND(SUM(a.elevation_gain), 0)               AS total_elevation_m,
    ROUND(SUM(a.tss), 0)                          AS total_tss,
    ROUND(AVG(NULLIF(a.np, 0)), 0)                AS avg_np
FROM activities a
WHERE a.start_time IS NOT NULL
GROUP BY week
ORDER BY week DESC;

-- All-time best mean power per duration, with the ride that set it.
DROP VIEW IF EXISTS power_curve_best;
CREATE VIEW power_curve_best AS
SELECT
    pc.duration_s,
    ROUND(MAX(pc.watts), 0)  AS best_watts,
    ROUND((SELECT p2.wkg FROM power_curve p2
           WHERE p2.duration_s = pc.duration_s
           ORDER BY p2.watts DESC LIMIT 1), 2) AS wkg,
    (SELECT a2.activity_id FROM power_curve p2
      JOIN activities a2 ON a2.activity_id = p2.activity_id
     WHERE p2.duration_s = pc.duration_s
     ORDER BY p2.watts DESC LIMIT 1) AS activity_id,
    (SELECT a2.name FROM power_curve p2
      JOIN activities a2 ON a2.activity_id = p2.activity_id
     WHERE p2.duration_s = pc.duration_s
     ORDER BY p2.watts DESC LIMIT 1) AS activity_name,
    (SELECT date(a2.start_time, 'unixepoch') FROM power_curve p2
      JOIN activities a2 ON a2.activity_id = p2.activity_id
     WHERE p2.duration_s = pc.duration_s
     ORDER BY p2.watts DESC LIMIT 1) AS set_on
FROM power_curve pc
GROUP BY pc.duration_s
ORDER BY pc.duration_s;

-- Races as ZwiftPower scores them, joined to the local ride where linked.
DROP VIEW IF EXISTS race_results;
CREATE VIEW race_results AS
SELECT
    r.event_id,
    date(r.event_date, 'unixepoch')      AS race_date,
    r.event_title,
    r.category,
    r.position,
    r.position_in_cat,
    r.finishers_in_cat,
    ROUND(r.time_s / 60.0, 2)            AS time_min,
    ROUND(r.time_gap_s, 1)               AS gap_s,
    ROUND(r.avg_power, 0)                AS avg_watts,
    ROUND(r.np, 0)                       AS np_watts,
    ROUND(r.avg_wkg, 2)                  AS avg_wkg,
    r.avg_hr,
    r.max_hr,
    ROUND(r.zp_ftp, 0)                   AS zp_ftp,
    r.wkg_5s, r.wkg_60s, r.wkg_5m, r.wkg_20m,
    r.activity_id,
    a.name                               AS activity_name,
    ROUND(a.distance / 1000.0, 2)        AS distance_km,
    ROUND(a.tss, 1)                      AS tss
FROM zp_results r
LEFT JOIN activities a ON a.activity_id = r.activity_id
ORDER BY r.event_date DESC;

-- ============================================================
-- Seed data
--
-- World ids as observed in activity payloads. Unverified ids are harmless:
-- an unknown world renders as "World <id>" in activity_summary.
-- ============================================================

INSERT OR IGNORE INTO worlds (world_id, name) VALUES
    (1,  'Watopia'),
    (2,  'Richmond'),
    (3,  'London'),
    (4,  'New York'),
    (5,  'Innsbruck'),
    (6,  'Bologna'),
    (7,  'Yorkshire'),
    (8,  'Crit City'),
    (9,  'Makuri Islands'),
    (10, 'France'),
    (11, 'Paris'),
    (12, 'Gravel Mountain'),
    (13, 'Scotland');
