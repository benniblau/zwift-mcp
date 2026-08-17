#!/usr/bin/env python3
"""
MCP Server for the Zwift Activities Database

Exposes the local Zwift + ZwiftPower SQLite database to MCP clients via
resources and tools, and the same data over a conventional REST API. Read
tools query the local database; the one write tool pushes to Zwift first and
only then updates the local copy.

Transports:
    stdio                — for local Claude Desktop
    streamable HTTP      — stateless, bearer-authenticated, for remote clients

Usage:
    python mcp_server.py                       # HTTP (default) on ZWIFT_MCP_HTTP_PORT
    python mcp_server.py --transport stdio     # stdio

In HTTP mode the server serves:
    /mcp  and  /mcp/     — the MCP streamable HTTP endpoint (both spellings)
    /api/v1/...          — the REST API, same bearer token
    /api/v1/health       — liveness probe, unauthenticated
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

from mcp.server.fastmcp import FastMCP

from zwift_downloader import HR_ZONES, POWER_ZONES

# ── Logging to stderr only (keep stdout clean for STDIO MCP transport) ──────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# ── Server ───────────────────────────────────────────────────────────────────
mcp = FastMCP("zwift-activities")

DEFAULT_DB = os.path.join(os.path.dirname(__file__), "zwift_activities.db")
DB_PATH = os.getenv("ZWIFT_DB_PATH", DEFAULT_DB)

# Mount point for the streamable HTTP transport (no trailing slash).
MCP_PATH = "/mcp"
API_PREFIX = "/api/v1"

# Columns that may be interpolated into an ORDER BY clause. Anything reaching
# an ORDER BY must come from this set — it cannot be parameterised.
ALLOWED_ORDER_COLUMNS = {
    "date", "start_time", "distance", "duration_s", "moving_time_s",
    "elevation_gain", "avg_power", "max_power", "np", "tss",
    "intensity_factor", "work_kj", "avg_wkg", "avg_hr", "max_hr",
    "avg_cadence", "avg_speed", "calories", "name",
}

MAX_LIMIT = 500


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # The downloader writes this file from cron while the server is serving.
    # The database is in WAL mode so readers are not blocked, but a wait is
    # still better than failing a query outright during a checkpoint.
    conn.execute("PRAGMA busy_timeout = 5000")
    try:
        yield conn
    finally:
        conn.close()


def _row(row) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return dict(row)


def _rows(rows) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


def _json(payload: Any) -> str:
    return json.dumps(payload, indent=2, default=str)


# ── Zwift client, created lazily so read-only use never needs credentials ────

_client = None


def get_client():
    """
    Return a shared authenticated Zwift client.

    Created on first use so that a purely read-only server (or one whose
    database is already populated) never needs credentials in the environment.
    """
    global _client
    if _client is None:
        from zwift_downloader import ZwiftClient
        _client = ZwiftClient()
        _client.authenticate()
    return _client


def _build_activity_filters(
    sport: Optional[str] = None,
    world_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_distance_km: Optional[float] = None,
    max_distance_km: Optional[float] = None,
    min_duration_min: Optional[float] = None,
    min_avg_watts: Optional[float] = None,
    races_only: Optional[bool] = None,
    name_contains: Optional[str] = None,
    has_power_data: Optional[bool] = None,
    has_hr_data: Optional[bool] = None,
) -> tuple:
    """Shared WHERE-clause builder for the MCP tools and the REST API."""
    conditions: List[str] = []
    params: List[Any] = []

    if sport:
        conditions.append("UPPER(a.sport) = UPPER(?)")
        params.append(sport)
    if world_id is not None:
        conditions.append("a.world_id = ?")
        params.append(world_id)
    if start_date:
        conditions.append("a.date >= ?")
        params.append(int(start_date.replace("-", "")))
    if end_date:
        conditions.append("a.date <= ?")
        params.append(int(end_date.replace("-", "")))
    if min_distance_km is not None:
        conditions.append("a.distance >= ?")
        params.append(min_distance_km * 1000)
    if max_distance_km is not None:
        conditions.append("a.distance <= ?")
        params.append(max_distance_km * 1000)
    if min_duration_min is not None:
        conditions.append("a.duration_s >= ?")
        params.append(min_duration_min * 60)
    if min_avg_watts is not None:
        conditions.append("a.avg_power >= ?")
        params.append(min_avg_watts)
    if races_only is True:
        conditions.append("a.zp_event_id IS NOT NULL")
    elif races_only is False:
        conditions.append("a.zp_event_id IS NULL")
    if name_contains:
        conditions.append("a.name LIKE ?")
        params.append(f"%{name_contains}%")
    if has_power_data is True:
        conditions.append("a.avg_power IS NOT NULL AND a.avg_power > 0")
    elif has_power_data is False:
        conditions.append("(a.avg_power IS NULL OR a.avg_power = 0)")
    if has_hr_data is True:
        conditions.append("a.avg_hr IS NOT NULL AND a.avg_hr > 0")
    elif has_hr_data is False:
        conditions.append("(a.avg_hr IS NULL OR a.avg_hr = 0)")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return where, params


def _query_activities(where: str, params: List[Any], order_by: str,
                      order_desc: bool, limit: int, offset: int = 0) -> Dict[str, Any]:
    """Run an activity query and return results plus the total match count."""
    if order_by not in ALLOWED_ORDER_COLUMNS:
        raise ValueError(
            f"Invalid order_by column '{order_by}'. "
            f"Allowed: {', '.join(sorted(ALLOWED_ORDER_COLUMNS))}"
        )
    limit = max(1, min(limit, MAX_LIMIT))
    direction = "DESC" if order_desc else "ASC"

    sql = f"""
        SELECT a.activity_id, a.name, a.sport,
               COALESCE(w.name, 'World ' || a.world_id) AS world,
               a.date, datetime(a.start_time, 'unixepoch') AS start_time_utc,
               ROUND(a.distance / 1000.0, 2) AS distance_km,
               ROUND(a.duration_s / 60.0, 1) AS duration_min,
               ROUND(a.moving_time_s / 60.0, 1) AS moving_min,
               ROUND(a.elevation_gain, 0) AS elevation_m,
               ROUND(a.avg_speed * 3.6, 1) AS avg_speed_kmh,
               ROUND(a.avg_power, 0) AS avg_watts,
               ROUND(a.max_power, 0) AS max_watts,
               ROUND(a.np, 0) AS np_watts,
               ROUND(a.avg_wkg, 2) AS avg_wkg,
               ROUND(a.intensity_factor, 3) AS intensity_factor,
               ROUND(a.tss, 1) AS tss, a.tss_source,
               ROUND(a.work_kj, 0) AS work_kj,
               a.avg_hr, a.max_hr, a.avg_cadence,
               ROUND(a.calories, 0) AS calories,
               a.zp_event_id, a.zp_event_id IS NOT NULL AS is_race,
               a.local_tags, a.local_notes,
               a.detail_synced_at IS NOT NULL AS has_detail
        FROM activities a
        LEFT JOIN worlds w ON a.world_id = w.world_id
        {where}
        ORDER BY a.{order_by} {direction}
        LIMIT ? OFFSET ?
    """
    count_sql = f"""
        SELECT COUNT(*) FROM activities a
        LEFT JOIN worlds w ON a.world_id = w.world_id
        {where}
    """

    with get_db() as conn:
        rows = conn.execute(sql, params + [limit, offset]).fetchall()
        total = conn.execute(count_sql, params).fetchone()[0]

    return {
        "total_matching": total,
        "returned": len(rows),
        "offset": offset,
        "activities": _rows(rows),
    }


def _activity_detail(activity_id: str, include_samples: bool = False) -> Dict[str, Any]:
    """Assemble the full local record for one activity."""
    with get_db() as conn:
        activity = _row(conn.execute(
            """SELECT a.*, COALESCE(w.name, 'World ' || a.world_id) AS world
               FROM activities a
               LEFT JOIN worlds w ON a.world_id = w.world_id
               WHERE a.activity_id = ?""",
            (activity_id,),
        ).fetchone())
        if not activity:
            return {"error": f"Activity {activity_id} not found"}

        # The stored blob is an implementation detail; expose it parsed.
        raw = activity.pop("raw_json", None)
        if raw:
            try:
                activity["zwift_payload"] = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                pass

        laps = _rows(conn.execute(
            "SELECT * FROM activity_laps WHERE activity_id = ? ORDER BY lap_index",
            (activity_id,),
        ).fetchall())

        zones = _rows(conn.execute(
            """SELECT metric_type, zone_index, zone_name, lower_bound,
                      upper_bound, seconds, percent
               FROM activity_zone_distribution WHERE activity_id = ?
               ORDER BY metric_type, zone_index""",
            (activity_id,),
        ).fetchall())

        curve = _rows(conn.execute(
            "SELECT duration_s, watts, wkg FROM power_curve "
            "WHERE activity_id = ? ORDER BY duration_s",
            (activity_id,),
        ).fetchall())

        race = _row(conn.execute(
            "SELECT * FROM race_results WHERE activity_id = ?", (activity_id,)
        ).fetchone())

        result = {
            "activity": activity,
            "laps": laps,
            "zone_distribution": zones,
            "power_curve": curve,
            "race_result": race,
        }

        if include_samples:
            result["samples"] = _rows(conn.execute(
                "SELECT * FROM activity_samples WHERE activity_id = ? "
                "ORDER BY sample_index",
                (activity_id,),
            ).fetchall())
        else:
            result["sample_count"] = conn.execute(
                "SELECT COUNT(*) FROM activity_samples WHERE activity_id = ?",
                (activity_id,),
            ).fetchone()[0]

    return result


def _stats_summary() -> Dict[str, Any]:
    with get_db() as conn:
        overall = _row(conn.execute("""
            SELECT COUNT(*) AS total_activities,
                   COUNT(DISTINCT sport) AS unique_sports,
                   MIN(date) AS earliest,
                   MAX(date) AS latest,
                   ROUND(SUM(distance) / 1000.0, 1) AS total_km,
                   ROUND(SUM(duration_s) / 3600.0, 1) AS total_hours,
                   ROUND(SUM(elevation_gain), 0) AS total_elevation_m,
                   ROUND(SUM(work_kj), 0) AS total_work_kj,
                   ROUND(SUM(tss), 0) AS total_tss,
                   ROUND(AVG(NULLIF(avg_power, 0)), 0) AS avg_watts,
                   ROUND(AVG(NULLIF(avg_hr, 0)), 0) AS avg_heartrate,
                   ROUND(SUM(calories), 0) AS total_calories,
                   COUNT(CASE WHEN zp_event_id IS NOT NULL THEN 1 END) AS races,
                   COUNT(CASE WHEN detail_synced_at IS NOT NULL THEN 1 END) AS with_detail
            FROM activities
        """).fetchone())

        by_sport = _rows(conn.execute("""
            SELECT COALESCE(sport, 'UNKNOWN') AS sport,
                   COUNT(*) AS count,
                   ROUND(SUM(distance) / 1000.0, 1) AS total_km,
                   ROUND(SUM(duration_s) / 3600.0, 1) AS total_hours,
                   ROUND(SUM(elevation_gain), 0) AS total_elevation_m,
                   ROUND(AVG(NULLIF(avg_power, 0)), 0) AS avg_watts,
                   ROUND(AVG(NULLIF(avg_hr, 0)), 0) AS avg_heartrate
            FROM activities GROUP BY sport ORDER BY count DESC
        """).fetchall())

        by_world = _rows(conn.execute("""
            SELECT COALESCE(w.name, 'World ' || a.world_id) AS world,
                   COUNT(*) AS count,
                   ROUND(SUM(a.distance) / 1000.0, 1) AS total_km,
                   ROUND(SUM(a.elevation_gain), 0) AS total_elevation_m
            FROM activities a
            LEFT JOIN worlds w ON a.world_id = w.world_id
            GROUP BY a.world_id ORDER BY count DESC LIMIT 20
        """).fetchall())

    return {"summary": overall, "by_sport": by_sport, "by_world": by_world}


def _power_curve(source: str = "local") -> Dict[str, Any]:
    """
    Best mean power per duration.

    'local' is derived from the FIT files of every ride; 'zwiftpower' is
    ZwiftPower's own curve, which only sees races and therefore usually reads
    lower. 'both' returns them side by side.
    """
    out: Dict[str, Any] = {"source": source}
    with get_db() as conn:
        if source in ("local", "both"):
            out["local"] = _rows(conn.execute(
                "SELECT * FROM power_curve_best ORDER BY duration_s"
            ).fetchall())
        if source in ("zwiftpower", "both"):
            out["zwiftpower"] = _rows(conn.execute(
                "SELECT duration_s, watts, wkg, "
                "date(effort_date, 'unixepoch') AS set_on, event_id "
                "FROM zp_critical_power ORDER BY duration_s"
            ).fetchall())
    return out


def _race_results(limit: int = 50, start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  title_contains: Optional[str] = None) -> Dict[str, Any]:
    conditions, params = [], []
    if start_date:
        conditions.append("race_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("race_date <= ?")
        params.append(end_date)
    if title_contains:
        conditions.append("event_title LIKE ?")
        params.append(f"%{title_contains}%")
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM race_results {where} ORDER BY race_date DESC LIMIT ?",
            params + [max(1, min(limit, MAX_LIMIT))],
        ).fetchall()
    return {"count": len(rows), "races": _rows(rows)}


# ─────────────────────────────────────────────────────────────────────────────
# Resources
# ─────────────────────────────────────────────────────────────────────────────

@mcp.resource(
    "zwift://athlete",
    name="Athlete Profile",
    description="Zwift profile with FTP, weight and lifetime totals, plus the "
                "ZwiftPower category and zFTP",
    mime_type="application/json",
)
def resource_athlete() -> str:
    return _json(_athlete_payload())


@mcp.resource(
    "zwift://activities",
    name="All Activities",
    description="All Zwift activities ordered by date descending",
    mime_type="application/json",
)
def resource_activities() -> str:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM activity_summary ORDER BY activity_date DESC"
        ).fetchall()
    return _json(_rows(rows))


@mcp.resource(
    "zwift://activities/recent",
    name="Recent Activities",
    description="Activities from the last 30 days",
    mime_type="application/json",
)
def resource_recent() -> str:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM activity_summary WHERE activity_date >= ? "
            "ORDER BY activity_date DESC",
            (cutoff,),
        ).fetchall()
    return _json(_rows(rows))


@mcp.resource(
    "zwift://stats/summary",
    name="Activity Statistics",
    description="Aggregate statistics across all activities, by sport and world",
    mime_type="application/json",
)
def resource_stats_summary() -> str:
    return _json(_stats_summary())


@mcp.resource(
    "zwift://stats/monthly",
    name="Monthly Statistics",
    description="Activity statistics aggregated by month and sport",
    mime_type="application/json",
)
def resource_stats_monthly() -> str:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM monthly_stats LIMIT 200").fetchall()
    return _json(_rows(rows))


@mcp.resource(
    "zwift://training/daily",
    name="Daily Training Load",
    description="Per-day TSS with CTL, ATL and TSB, computed locally from FTP",
    mime_type="application/json",
)
def resource_daily_metrics() -> str:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM daily_metrics ORDER BY calendar_date DESC LIMIT 365"
        ).fetchall()
    return _json(_rows(rows))


@mcp.resource(
    "zwift://power/curve",
    name="Power Curve",
    description="All-time best mean power per duration, local and ZwiftPower",
    mime_type="application/json",
)
def resource_power_curve() -> str:
    return _json(_power_curve("both"))


@mcp.resource(
    "zwift://racing/results",
    name="Race Results",
    description="ZwiftPower race results with category, position and power",
    mime_type="application/json",
)
def resource_race_results() -> str:
    return _json(_race_results(limit=MAX_LIMIT))


def _athlete_payload() -> Dict[str, Any]:
    with get_db() as conn:
        athlete = _row(conn.execute(
            "SELECT * FROM athletes WHERE is_self = 1 "
            "ORDER BY synced_at DESC LIMIT 1"
        ).fetchone())
        if athlete:
            athlete.pop("raw_json", None)
        zp = _row(conn.execute(
            "SELECT * FROM zp_profile ORDER BY synced_at DESC LIMIT 1"
        ).fetchone())
        if zp:
            zp.pop("raw_json", None)
        form = _row(conn.execute(
            "SELECT * FROM daily_metrics ORDER BY calendar_date DESC LIMIT 1"
        ).fetchone())
    return {"athlete": athlete, "zwiftpower": zp, "current_form": form}


# ─────────────────────────────────────────────────────────────────────────────
# Read tools
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def query_activities(
    sport: Optional[str] = None,
    world_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    min_distance_km: Optional[float] = None,
    max_distance_km: Optional[float] = None,
    min_duration_min: Optional[float] = None,
    min_avg_watts: Optional[float] = None,
    races_only: Optional[bool] = None,
    name_contains: Optional[str] = None,
    has_power_data: Optional[bool] = None,
    has_hr_data: Optional[bool] = None,
    limit: int = 50,
    offset: int = 0,
    order_by: str = "date",
    order_desc: bool = True,
) -> str:
    """
    Query Zwift activities with flexible filters.

    Args:
        sport: 'CYCLING' or 'RUNNING'.
        world_id: Zwift world (1 Watopia, 3 London, 9 Makuri Islands, ...).
        start_date: Earliest activity date (YYYY-MM-DD).
        end_date: Latest activity date (YYYY-MM-DD).
        min_distance_km: Minimum distance in km.
        max_distance_km: Maximum distance in km.
        min_duration_min: Minimum elapsed time in minutes.
        min_avg_watts: Minimum average power.
        races_only: True for rides matched to a ZwiftPower race result,
            False to exclude them.
        name_contains: Substring match on the activity title.
        has_power_data: If True, only activities that recorded power.
        has_hr_data: If True, only activities that recorded heart rate.
        limit: Maximum results (default 50, max 500).
        offset: Number of results to skip, for paging.
        order_by: Sort column (default 'date').
        order_desc: Sort descending if True.
    """
    where, params = _build_activity_filters(
        sport, world_id, start_date, end_date, min_distance_km, max_distance_km,
        min_duration_min, min_avg_watts, races_only, name_contains,
        has_power_data, has_hr_data,
    )
    try:
        return _json(_query_activities(where, params, order_by, order_desc, limit, offset))
    except ValueError as e:
        return _json({"error": str(e)})


@mcp.tool()
def get_activity_details(activity_id: str, include_samples: bool = False) -> str:
    """
    Get the full local record for one activity: summary metrics, laps,
    time-in-zone distribution, that ride's power curve and the matching
    ZwiftPower race result if there is one.

    Laps, zones and the curve only exist for activities whose FIT file has
    been parsed — see `has_detail` in query_activities.

    Args:
        activity_id: Zwift activity id.
        include_samples: Include the per-second sample stream. This can be
            tens of thousands of rows, so it is off by default.
    """
    return _json(_activity_detail(activity_id, include_samples))


@mcp.tool()
def get_training_load(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 90,
) -> str:
    """
    Daily training load: TSS per day with CTL (fitness), ATL (fatigue) and
    TSB (form).

    These are computed locally from power and the FTP in the athlete profile,
    not supplied by Zwift, so a stale FTP shifts every value.

    Args:
        start_date: Earliest date (YYYY-MM-DD).
        end_date: Latest date (YYYY-MM-DD).
        limit: Maximum days to return (default 90).
    """
    conditions, params = [], []
    if start_date:
        conditions.append("calendar_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("calendar_date <= ?")
        params.append(end_date)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_db() as conn:
        rows = conn.execute(
            f"""SELECT calendar_date, activity_count,
                       ROUND(duration_s / 60.0, 1) AS duration_min,
                       ROUND(distance / 1000.0, 2) AS distance_km,
                       elevation_gain, tss, ctl, atl, tsb, ftp_used
                FROM daily_metrics {where}
                ORDER BY calendar_date DESC LIMIT ?""",
            params + [max(1, min(limit, 400))],
        ).fetchall()
    return _json({"count": len(rows), "days": _rows(rows)})


@mcp.tool()
def get_training_trends(period: str = "month", limit: int = 24) -> str:
    """
    Training volume aggregated by week or month.

    Args:
        period: 'week' or 'month'.
        limit: Number of periods to return, most recent first.
    """
    view = "weekly_load" if period == "week" else "monthly_stats"
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM {view} LIMIT ?", (max(1, min(limit, 200)),)
        ).fetchall()
    return _json({"period": period, "trends": _rows(rows)})


@mcp.tool()
def get_training_zones() -> str:
    """
    Current thresholds and the training zones derived from them.

    Power zones are Coggan percentages of FTP; heart-rate zones are
    percentages of max HR. Zwift stores neither — both are computed here from
    the profile, which is also exactly how the stored time-in-zone data was
    calculated.
    """
    with get_db() as conn:
        athlete = _row(conn.execute(
            "SELECT display_name, ftp, weight, height, max_hr, rest_hr, level "
            "FROM athletes WHERE is_self = 1 ORDER BY synced_at DESC LIMIT 1"
        ).fetchone())
        zp = _row(conn.execute(
            "SELECT category, racing_score, zftp, zmap FROM zp_profile "
            "ORDER BY synced_at DESC LIMIT 1"
        ).fetchone())

    override = os.getenv("ZWIFT_FTP_OVERRIDE")
    ftp = float(override) if override else (athlete or {}).get("ftp")
    max_hr = (athlete or {}).get("max_hr")

    def build(zones, threshold):
        if not threshold:
            return []
        return [
            {
                "index": i + 1,
                "name": name,
                "lower": round(low * threshold),
                "upper": round(high * threshold) if high is not None else None,
                "percent_of_threshold": f"{int(low * 100)}–"
                                        f"{int(high * 100) if high else '∞'}%",
            }
            for i, (name, low, high) in enumerate(zones)
        ]

    return _json({
        "thresholds": {
            "ftp_watts": ftp,
            "ftp_source": "ZWIFT_FTP_OVERRIDE" if override else "zwift profile",
            "weight_kg": (athlete or {}).get("weight"),
            "wkg_at_ftp": round(ftp / athlete["weight"], 2)
                          if ftp and (athlete or {}).get("weight") else None,
            "max_hr": max_hr,
            "zwiftpower": zp,
        },
        "power_zones": build(POWER_ZONES, ftp),
        "hr_zones": build(HR_ZONES, max_hr),
        "units": {"power": "watts", "hr": "bpm"},
    })


@mcp.tool()
def get_activity_stats() -> str:
    """Aggregate statistics across all activities, overall, per sport and per world."""
    return _json(_stats_summary())


@mcp.tool()
def get_power_curve(source: str = "both") -> str:
    """
    All-time best mean power for each standard duration.

    Args:
        source: 'local' (derived from every ride's FIT file), 'zwiftpower'
            (ZwiftPower's own curve, races only), or 'both' to compare them.
            The two routinely disagree — ZwiftPower never sees training rides.
    """
    if source not in ("local", "zwiftpower", "both"):
        return _json({"error": "source must be 'local', 'zwiftpower' or 'both'"})
    return _json(_power_curve(source))


@mcp.tool()
def get_race_results(
    limit: int = 50,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    title_contains: Optional[str] = None,
) -> str:
    """
    ZwiftPower race results: category, position, time, power and the linked
    local activity where one was matched.

    Args:
        limit: Maximum races to return (default 50).
        start_date: Earliest race date (YYYY-MM-DD).
        end_date: Latest race date (YYYY-MM-DD).
        title_contains: Substring match on the event title.
    """
    return _json(_race_results(limit, start_date, end_date, title_contains))


@mcp.tool()
def get_race_details(event_id: str) -> str:
    """
    One race in full: your result, and the finishing field if it was synced
    with --with-zp-fields.

    Args:
        event_id: ZwiftPower event id (the `zid`, not a Zwift activity id).
    """
    with get_db() as conn:
        mine = _row(conn.execute(
            "SELECT * FROM race_results WHERE event_id = ?", (event_id,)
        ).fetchone())
        if not mine:
            return _json({"error": f"No ZwiftPower result stored for event {event_id}"})
        field = _rows(conn.execute(
            "SELECT name, team, category, position, position_in_cat, "
            "ROUND(time_s / 60.0, 2) AS time_min, avg_power, avg_wkg, weight "
            "FROM zp_event_results WHERE event_id = ? "
            "ORDER BY position LIMIT 200",
            (event_id,),
        ).fetchall())
    return _json({
        "result": mine,
        "field_size": len(field),
        "field": field,
        "note": None if field else
                "Finishing field not synced. Run the downloader with "
                "--with-zp-fields to fetch it.",
    })


@mcp.tool()
def get_athlete_profile() -> str:
    """
    The Zwift profile (FTP, weight, level, lifetime totals), the ZwiftPower
    profile (category, zFTP, racing score) and today's form.
    """
    return _json(_athlete_payload())


@mcp.tool()
def execute_sql(query: str, limit: int = 100) -> str:
    """
    Run a custom read-only SELECT query against the Zwift database.

    Tables: activities, athletes, worlds, activity_laps, activity_samples,
    activity_zone_distribution, power_curve, segment_results, daily_metrics,
    zp_profile, zp_results, zp_event_results, zp_critical_power, sync_state.
    Views: activity_summary, monthly_stats, weekly_load, power_curve_best,
    race_results.

    Units are SI: metres, seconds, watts, bpm, m/s. Timestamps are unix epoch
    seconds — wrap them in datetime(col, 'unixepoch').

    ZwiftPower ids are not Zwift ids: zp_results.event_id is a ZwiftPower
    race, joined to activities through activities.zp_event_id.

    Only SELECT statements are permitted.

    Args:
        query: SQL SELECT query.
        limit: Maximum rows (default 100, max 1000).
    """
    stripped = query.strip().upper()
    if not stripped.startswith("SELECT"):
        return _json({"error": "Only SELECT queries are permitted"})
    # Block multi-statement payloads smuggled in behind a semicolon.
    if ";" in query.strip().rstrip(";"):
        return _json({"error": "Multiple SQL statements are not permitted"})

    sql = query.strip().rstrip(";")
    if "LIMIT" not in stripped:
        sql += f" LIMIT {max(1, min(limit, 1000))}"

    try:
        with get_db() as conn:
            # Defence in depth: reject writes even if they slip past the checks.
            conn.execute("PRAGMA query_only = ON")
            rows = conn.execute(sql).fetchall()
        return _json({"count": len(rows), "rows": _rows(rows)})
    except sqlite3.Error as e:
        return _json({"error": str(e)})


# ─────────────────────────────────────────────────────────────────────────────
# Write tools — push to Zwift first, then update the local database
# ─────────────────────────────────────────────────────────────────────────────

def _rename_activity(activity_id: str, name: str) -> Dict[str, Any]:
    if not name.strip():
        raise ValueError("Activity name cannot be empty")
    get_client().rename_activity(activity_id, name)
    with get_db() as conn:
        conn.execute("UPDATE activities SET name = ? WHERE activity_id = ?",
                     (name, activity_id))
        conn.commit()
    return {"status": "ok", "activity_id": activity_id, "name": name}


@mcp.tool()
def rename_activity(activity_id: str, name: str) -> str:
    """
    Rename an activity on Zwift and update the local database.

    This writes to your Zwift account. Zwift replaces the whole activity on
    update, so the client re-reads it first and changes only the title.

    Args:
        activity_id: Zwift activity id.
        name: The new activity title.
    """
    try:
        return _json(_rename_activity(activity_id, name))
    except Exception as e:
        return _json({"error": str(e)})


@mcp.tool()
def set_local_annotation(
    activity_id: str,
    tags: Optional[str] = None,
    notes: Optional[str] = None,
) -> str:
    """
    Store tags and notes against an activity in the LOCAL database only.

    Nothing is sent to Zwift or ZwiftPower. Use this for organisation neither
    site supports, such as training-block labels or how a workout felt.

    Args:
        activity_id: Zwift activity id.
        tags: Comma-separated tags.
        notes: Free-text note kept locally.
    """
    with get_db() as conn:
        exists = conn.execute(
            "SELECT 1 FROM activities WHERE activity_id = ?", (activity_id,)
        ).fetchone()
        if not exists:
            return _json({"error": f"Activity {activity_id} not found"})
        if tags is not None:
            conn.execute("UPDATE activities SET local_tags = ? WHERE activity_id = ?",
                         (tags, activity_id))
        if notes is not None:
            conn.execute("UPDATE activities SET local_notes = ? WHERE activity_id = ?",
                         (notes, activity_id))
        conn.commit()
    return _json({"status": "ok", "activity_id": activity_id,
                  "tags": tags, "notes": notes})


@mcp.tool()
def get_activity_fit_file(activity_id: str) -> str:
    """
    Where the original FIT file for an activity lives: the local cache path if
    the downloader has fetched it, and the S3 URL either way.

    Args:
        activity_id: Zwift activity id.
    """
    with get_db() as conn:
        row = _row(conn.execute(
            "SELECT fit_bucket, fit_key, fit_path, fit_synced_at "
            "FROM activities WHERE activity_id = ?", (activity_id,)
        ).fetchone())
    if not row:
        return _json({"error": f"Activity {activity_id} not found"})
    if not row["fit_key"]:
        return _json({"error": f"Activity {activity_id} has no FIT file recorded"})
    return _json({
        "activity_id": activity_id,
        "local_path": row["fit_path"],
        "downloaded_at": row["fit_synced_at"],
        "url": f"https://{row['fit_bucket']}.s3.amazonaws.com/{row['fit_key']}",
    })


# ─────────────────────────────────────────────────────────────────────────────
# REST API
# ─────────────────────────────────────────────────────────────────────────────

def build_rest_routes():
    """Conventional REST endpoints backed by the same database and client."""
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    def ok(payload, status: int = 200):
        return JSONResponse(json.loads(_json(payload)), status_code=status)

    def err(message: str, status: int = 400):
        return JSONResponse({"error": message}, status_code=status)

    def authorized(request) -> bool:
        return getattr(request.user, "is_authenticated", False)

    def guard(handler):
        """Reject unauthenticated REST calls before touching the database."""
        async def wrapped(request):
            if not authorized(request):
                return err("Unauthorized", 401)
            try:
                return await handler(request)
            except ValueError as e:
                return err(str(e), 400)
            except Exception as e:                       # noqa: BLE001
                logger.exception("REST handler failed")
                return err(str(e), 500)
        return wrapped

    async def health(request):
        """Liveness probe — intentionally unauthenticated."""
        try:
            with get_db() as conn:
                n = conn.execute("SELECT COUNT(*) FROM activities").fetchone()[0]
                races = conn.execute("SELECT COUNT(*) FROM zp_results").fetchone()[0]
            return ok({"status": "ok", "database": DB_PATH,
                       "activities": n, "race_results": races})
        except sqlite3.Error as e:
            return err(f"database unavailable: {e}", 503)

    @guard
    async def list_activities(request):
        q = request.query_params

        def num(name, cast=float):
            v = q.get(name)
            return cast(v) if v not in (None, "") else None

        def flag(name):
            v = q.get(name)
            if v is None:
                return None
            return v.lower() in ("1", "true", "yes")

        where, params = _build_activity_filters(
            sport=q.get("sport"),
            world_id=num("world_id", int),
            start_date=q.get("start_date"),
            end_date=q.get("end_date"),
            min_distance_km=num("min_distance_km"),
            max_distance_km=num("max_distance_km"),
            min_duration_min=num("min_duration_min"),
            min_avg_watts=num("min_avg_watts"),
            races_only=flag("races_only"),
            name_contains=q.get("name_contains"),
            has_power_data=flag("has_power_data"),
            has_hr_data=flag("has_hr_data"),
        )
        return ok(_query_activities(
            where, params,
            order_by=q.get("order_by", "date"),
            order_desc=q.get("order", "desc").lower() != "asc",
            limit=int(q.get("limit", 50)),
            offset=int(q.get("offset", 0)),
        ))

    @guard
    async def get_activity(request):
        activity_id = request.path_params["activity_id"]
        include = request.query_params.get("include_samples", "").lower() in ("1", "true")
        result = _activity_detail(activity_id, include)
        return ok(result, 404 if "error" in result else 200)

    @guard
    async def update_activity(request):
        """PATCH an activity: name, or the local-only annotations."""
        activity_id = request.path_params["activity_id"]
        try:
            body = await request.json()
        except Exception:
            return err("Request body must be JSON")

        applied = {}
        if "name" in body:
            applied["rename"] = _rename_activity(activity_id, body["name"])
        if "local_tags" in body or "local_notes" in body:
            with get_db() as conn:
                if "local_tags" in body:
                    conn.execute(
                        "UPDATE activities SET local_tags = ? WHERE activity_id = ?",
                        (body["local_tags"], activity_id))
                if "local_notes" in body:
                    conn.execute(
                        "UPDATE activities SET local_notes = ? WHERE activity_id = ?",
                        (body["local_notes"], activity_id))
                conn.commit()
            applied["local"] = {"status": "ok"}

        if not applied:
            return err("No supported fields in body. Accepted: name, "
                       "local_tags, local_notes")
        return ok({"activity_id": activity_id, "applied": applied})

    @guard
    async def activity_laps(request):
        activity_id = request.path_params["activity_id"]
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM activity_laps WHERE activity_id = ? ORDER BY lap_index",
                (activity_id,),
            ).fetchall()
        return ok({"activity_id": activity_id, "laps": _rows(rows)})

    @guard
    async def activity_samples(request):
        activity_id = request.path_params["activity_id"]
        q = request.query_params
        limit = max(1, min(int(q.get("limit", 5000)), 50000))
        offset = int(q.get("offset", 0))
        with get_db() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM activity_samples WHERE activity_id = ?",
                (activity_id,),
            ).fetchone()[0]
            rows = conn.execute(
                "SELECT * FROM activity_samples WHERE activity_id = ? "
                "ORDER BY sample_index LIMIT ? OFFSET ?",
                (activity_id, limit, offset),
            ).fetchall()
        return ok({"activity_id": activity_id, "total": total,
                   "offset": offset, "samples": _rows(rows)})

    @guard
    async def athlete(request):
        payload = _athlete_payload()
        if not payload["athlete"]:
            return err("No athlete profile synced yet", 404)
        return ok(payload)

    @guard
    async def stats_summary(request):
        return ok(_stats_summary())

    @guard
    async def stats_monthly(request):
        with get_db() as conn:
            rows = conn.execute("SELECT * FROM monthly_stats LIMIT 200").fetchall()
        return ok({"months": _rows(rows)})

    @guard
    async def daily_metrics(request):
        q = request.query_params
        conditions, params = [], []
        if q.get("start_date"):
            conditions.append("calendar_date >= ?")
            params.append(q["start_date"])
        if q.get("end_date"):
            conditions.append("calendar_date <= ?")
            params.append(q["end_date"])
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        limit = max(1, min(int(q.get("limit", 90)), 400))
        with get_db() as conn:
            rows = conn.execute(
                f"SELECT * FROM daily_metrics {where} "
                f"ORDER BY calendar_date DESC LIMIT ?",
                params + [limit],
            ).fetchall()
        return ok({"count": len(rows), "days": _rows(rows)})

    @guard
    async def power_curve(request):
        source = request.query_params.get("source", "both")
        if source not in ("local", "zwiftpower", "both"):
            return err("source must be 'local', 'zwiftpower' or 'both'")
        return ok(_power_curve(source))

    @guard
    async def races(request):
        q = request.query_params
        return ok(_race_results(
            limit=int(q.get("limit", 50)),
            start_date=q.get("start_date"),
            end_date=q.get("end_date"),
            title_contains=q.get("title_contains"),
        ))

    @guard
    async def race(request):
        event_id = request.path_params["event_id"]
        with get_db() as conn:
            mine = _row(conn.execute(
                "SELECT * FROM race_results WHERE event_id = ?", (event_id,)
            ).fetchone())
            if not mine:
                return err(f"No result stored for event {event_id}", 404)
            field = _rows(conn.execute(
                "SELECT * FROM zp_event_results WHERE event_id = ? ORDER BY position",
                (event_id,),
            ).fetchall())
        return ok({"result": mine, "field": field})

    @guard
    async def zwiftpower_profile(request):
        with get_db() as conn:
            row = _row(conn.execute(
                "SELECT * FROM zp_profile ORDER BY synced_at DESC LIMIT 1"
            ).fetchone())
            curve = _rows(conn.execute(
                "SELECT * FROM zp_critical_power ORDER BY duration_s"
            ).fetchall())
        if not row:
            return err("No ZwiftPower profile synced yet", 404)
        return ok({"profile": row, "critical_power": curve})

    @guard
    async def sync_state(request):
        with get_db() as conn:
            rows = conn.execute("SELECT * FROM sync_state").fetchall()
        return ok({"datasets": _rows(rows)})

    p = API_PREFIX
    return [
        Route(f"{p}/health", health, methods=["GET"]),
        Route(f"{p}/athlete", athlete, methods=["GET"]),
        Route(f"{p}/activities", list_activities, methods=["GET"]),
        Route(f"{p}/activities/{{activity_id}}", get_activity, methods=["GET"]),
        Route(f"{p}/activities/{{activity_id}}", update_activity, methods=["PATCH"]),
        Route(f"{p}/activities/{{activity_id}}/laps", activity_laps, methods=["GET"]),
        Route(f"{p}/activities/{{activity_id}}/samples", activity_samples, methods=["GET"]),
        Route(f"{p}/stats/summary", stats_summary, methods=["GET"]),
        Route(f"{p}/stats/monthly", stats_monthly, methods=["GET"]),
        Route(f"{p}/daily-metrics", daily_metrics, methods=["GET"]),
        Route(f"{p}/power-curve", power_curve, methods=["GET"]),
        Route(f"{p}/races", races, methods=["GET"]),
        Route(f"{p}/races/{{event_id}}", race, methods=["GET"]),
        Route(f"{p}/zwiftpower/profile", zwiftpower_profile, methods=["GET"]),
        Route(f"{p}/sync-state", sync_state, methods=["GET"]),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Transport
# ─────────────────────────────────────────────────────────────────────────────

def run_stdio() -> None:
    if not os.path.exists(DB_PATH):
        logger.error(f"Database not found: {DB_PATH}")
        logger.error("Run zwift_downloader.py first to populate the database.")
        sys.exit(1)
    mcp.run()


def main_http() -> None:
    """Run the MCP streamable HTTP transport and the REST API together."""
    import uvicorn
    from starlette.applications import Starlette
    from starlette.middleware import Middleware
    from starlette.middleware.authentication import AuthenticationMiddleware
    from starlette.routing import Mount

    from mcp.server.auth.middleware.bearer_auth import BearerAuthBackend, RequireAuthMiddleware
    from mcp.server.auth.provider import AccessToken
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

    if not os.path.exists(DB_PATH):
        logger.error(f"Database not found: {DB_PATH}")
        logger.error("Run zwift_downloader.py first to populate the database.")
        sys.exit(1)

    auth_token = os.getenv("ZWIFT_MCP_AUTH_TOKEN")
    if not auth_token:
        logger.error("ZWIFT_MCP_AUTH_TOKEN is required for HTTP transport")
        sys.exit(1)

    host = os.getenv("ZWIFT_MCP_HTTP_HOST", "0.0.0.0")
    port = int(os.getenv("ZWIFT_MCP_HTTP_PORT", "8081"))

    class StaticTokenVerifier:
        def __init__(self, expected: str):
            self.expected = expected

        async def verify_token(self, token: str) -> Optional[AccessToken]:
            if token == self.expected:
                return AccessToken(
                    token=token,
                    client_id="static",
                    scopes=["mcp:access"],
                    expires_at=None,
                )
            return None

    verifier = StaticTokenVerifier(auth_token)
    # Stateless: no session is retained between requests, so any instance can
    # serve any request and the server can be restarted without breaking clients.
    session_manager = StreamableHTTPSessionManager(app=mcp._mcp_server, stateless=True)

    @asynccontextmanager
    async def lifespan(app):
        async with session_manager.run():
            yield

    def _normalize_path(inner):
        """Give the session manager a non-empty path when mounted at /mcp."""
        async def wrapped(scope, receive, send):
            if scope["type"] == "http" and not scope.get("path"):
                scope = {**scope, "path": "/", "raw_path": b"/"}
            await inner(scope, receive, send)
        return wrapped

    mcp_app = RequireAuthMiddleware(
        _normalize_path(session_manager.handle_request),
        required_scopes=["mcp:access"],
    )

    app = Starlette(
        routes=[Mount(MCP_PATH, app=mcp_app)] + build_rest_routes(),
        middleware=[
            Middleware(AuthenticationMiddleware, backend=BearerAuthBackend(verifier)),
        ],
        lifespan=lifespan,
    )

    def _accept_bare_mcp_path(inner):
        """
        Make `/mcp` and `/mcp/` behave identically.

        Starlette compiles Mount("/mcp") to the regex `^/mcp/(?P<path>.*)$`, so
        a request to bare `/mcp` does not match and the router answers with a
        307 redirect to `/mcp/`. Many MCP clients do not follow redirects, and
        some drop the Authorization header when they do. Rewriting the path
        here — outside the router — means both spellings are served directly.
        """
        async def wrapped(scope, receive, send):
            if scope["type"] in ("http", "websocket") and scope.get("path") == MCP_PATH:
                scope = {
                    **scope,
                    "path": MCP_PATH + "/",
                    "raw_path": (MCP_PATH + "/").encode("ascii"),
                }
            await inner(scope, receive, send)
        return wrapped

    logger.info(f"Starting Zwift MCP server on {host}:{port}")
    logger.info(f"  MCP  : http://{host}:{port}{MCP_PATH}  (and {MCP_PATH}/)")
    logger.info(f"  REST : http://{host}:{port}{API_PREFIX}/")
    uvicorn.run(_accept_bare_mcp_path(app), host=host, port=port, log_level="info")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zwift MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default=os.getenv("ZWIFT_MCP_TRANSPORT", "http"),
        help="Transport mode (default: http, or set ZWIFT_MCP_TRANSPORT)",
    )
    args = parser.parse_args()

    if args.transport == "http":
        main_http()
    else:
        run_stdio()
