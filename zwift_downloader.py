#!/usr/bin/env python3
"""
Zwift data downloader.

Pulls two sources into one SQLite database:

  * the private Zwift game API — profile, activities, and the original FIT
    files, which are the only place per-second data, laps and heart-rate or
    cadence averages exist
  * ZwiftPower — race results, category, zFTP and the critical-power curve

Training load (NP, IF, TSS, CTL, ATL, TSB) has no upstream equivalent and is
computed locally from the samples and the athlete's FTP.

Usage:
    python zwift_downloader.py                     # incremental
    python zwift_downloader.py --days 30
    python zwift_downloader.py --since 2024-01-01
    python zwift_downloader.py --full              # re-fetch everything
    python zwift_downloader.py --with-samples      # + per-second streams
    python zwift_downloader.py --backfill-detail   # FITs for rides missing them
    python zwift_downloader.py --zp-only           # only ZwiftPower
    python zwift_downloader.py --skip-zp           # only the game API
"""

import argparse
import html
import json
import math
import os
import re
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ZWIFT_TOKEN_URL = (
    "https://secure.zwift.com/auth/realms/zwift/protocol/openid-connect/token"
)
ZWIFT_API = os.getenv("ZWIFT_API_HOST", "https://us-or-rly101.zwift.com")
ZWIFT_CLIENT_ID = "Zwift_Mobile_Link"

ZP_BASE = "https://zwiftpower.com"
ZP_SSO_URL = f"{ZP_BASE}/ucp.php?mode=login&login=external&oauth_service=oauthzpsso"

# Zwift rejects some generic agents outright; present as the mobile app.
USER_AGENT = os.getenv(
    "ZWIFT_USER_AGENT",
    "Zwift/115 CFNetwork/1335.0.3 Darwin/21.6.0",
)
ZP_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
)

DEFAULT_DB = os.path.join(os.path.dirname(__file__), "zwift_activities.db")
SCHEMA_PATH = Path(__file__).with_name("schema") / "schema_zwift.sql"

TOKEN_CACHE_PATH = Path(__file__).with_name(".zwift_token.json")
ZP_SESSION_PATH = Path(__file__).with_name(".zwiftpower_session.json")
FIT_CACHE = Path(os.getenv("ZWIFT_FIT_CACHE", Path(__file__).with_name("fits")))

# The activity list endpoint pages with start/limit. Zwift silently caps the
# page size; 30 is what the mobile app asks for and is always honoured.
PAGE_SIZE = 30

# Durations the local power curve is computed over, in seconds.
CURVE_DURATIONS = (1, 5, 15, 30, 60, 120, 300, 600, 1200, 1800, 3600)

# Coggan power zones as a fraction of FTP, and HR zones as a fraction of max
# HR. Upper bound None means the zone is open-ended.
POWER_ZONES = [
    ("Z1 Active Recovery", 0.00, 0.55),
    ("Z2 Endurance",       0.55, 0.75),
    ("Z3 Tempo",           0.75, 0.90),
    ("Z4 Threshold",       0.90, 1.05),
    ("Z5 VO2max",          1.05, 1.20),
    ("Z6 Anaerobic",       1.20, 1.50),
    ("Z7 Neuromuscular",   1.50, None),
]
HR_ZONES = [
    ("Z1 Recovery",  0.00, 0.60),
    ("Z2 Endurance", 0.60, 0.70),
    ("Z3 Tempo",     0.70, 0.80),
    ("Z4 Threshold", 0.80, 0.90),
    ("Z5 Maximal",   0.90, None),
]

CTL_TIME_CONSTANT = 42
ATL_TIME_CONSTANT = 7


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _upsert(conn: sqlite3.Connection, table: str, data: Dict[str, Any]) -> None:
    """INSERT OR REPLACE a dict as a row, ignoring None-only payloads."""
    if not data:
        return
    columns = ", ".join(data)
    placeholders = ", ".join("?" for _ in data)
    conn.execute(
        f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})",
        list(data.values()),
    )


def _num(value: Any) -> Optional[float]:
    """Coerce to float, tolerating strings, empties and ZwiftPower's junk."""
    if value is None or value == "" or value is False:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if not math.isnan(float(value)) else None
    try:
        return float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> Optional[int]:
    v = _num(value)
    return int(round(v)) if v is not None else None


def _epoch(value: Any) -> Optional[int]:
    """
    Parse the several date shapes Zwift uses into unix epoch seconds.

    The API mixes ISO 8601 with and without a timezone, and epoch
    milliseconds; ZwiftPower uses epoch seconds.
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        # Anything past ~2001 in milliseconds is far beyond a plausible
        # seconds-since-epoch date, so magnitude is a safe discriminator.
        return int(v / 1000) if v > 1e11 else int(v)
    text = str(value).strip().replace("Z", "+00:00")
    for fmt in (None, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.fromisoformat(text) if fmt is None else datetime.strptime(text, fmt)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    return None


def _yyyymmdd(epoch: Optional[int]) -> Optional[int]:
    if not epoch:
        return None
    return int(datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y%m%d"))


def _iso_day(epoch: Optional[int]) -> Optional[str]:
    if not epoch:
        return None
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%d")


def _ms_to_s(value: Any) -> Optional[float]:
    v = _num(value)
    return v / 1000.0 if v is not None else None


def _semicircles(value: Any) -> Optional[float]:
    """FIT stores coordinates in semicircles; convert to degrees."""
    v = _num(value)
    if v is None:
        return None
    return v * (180.0 / 2 ** 31)


def _zp(value: Any) -> Any:
    """
    Unwrap a ZwiftPower cell.

    Most numeric fields arrive as [value, formatting_flag] rather than a
    scalar, and a few arrive as a single-element list. Anything else is
    returned untouched.
    """
    if isinstance(value, (list, tuple)):
        return value[0] if value else None
    return value


def _zp_num(row: Dict[str, Any], *keys: str) -> Optional[float]:
    """First numeric value among several candidate ZwiftPower keys."""
    for key in keys:
        if key in row:
            v = _num(_zp(row[key]))
            if v is not None:
                return v
    return None


def _zp_str(row: Dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        if key in row:
            v = _zp(row[key])
            if v not in (None, ""):
                return str(v).strip()
    return None


# ---------------------------------------------------------------------------
# Zwift game API client
# ---------------------------------------------------------------------------

class ZwiftError(RuntimeError):
    """Raised for Zwift API errors the caller should surface."""


class ZwiftClient:
    """
    Authenticated client for the private Zwift API.

    Unlike most of these reverse-engineered endpoints, Zwift's auth is a
    standard OAuth password grant against Keycloak, and it issues a refresh
    token. Access tokens last about six hours; `_ensure_token()` refreshes
    silently and only falls back to a full login when the refresh is
    rejected, so a long-running server never needs to re-read the password.

    The MCP server imports this class directly so its tools reach Zwift
    without duplicating auth logic.
    """

    def __init__(self, user: Optional[str] = None, password: Optional[str] = None,
                 token_cache: Path = TOKEN_CACHE_PATH):
        self.user = user or os.getenv("ZWIFT_USER")
        self.password = password or os.getenv("ZWIFT_PASS") or os.getenv("ZWIFT_PASSWORD")
        self.token_cache = Path(token_cache)
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.expires_at: float = 0.0
        self.player_id: Optional[str] = None
        self.profile: Dict[str, Any] = {}
        self.session = requests.Session()

        if not self.user or not self.password:
            raise ZwiftError("ZWIFT_USER and ZWIFT_PASS must be set in .env")

        self._load_cached_token()

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _load_cached_token(self) -> None:
        if not self.token_cache.exists():
            return
        try:
            cached = json.loads(self.token_cache.read_text())
        except (OSError, json.JSONDecodeError):
            return
        if cached.get("user") != self.user:
            return
        self.access_token = cached.get("access_token")
        self.refresh_token = cached.get("refresh_token")
        self.expires_at = float(cached.get("expires_at") or 0)
        self.player_id = cached.get("player_id")

    def _save_cached_token(self) -> None:
        payload = {
            "user": self.user,
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_at": self.expires_at,
            "player_id": self.player_id,
        }
        try:
            self.token_cache.write_text(json.dumps(payload))
            # The file holds a live credential — keep it owner-readable only.
            os.chmod(self.token_cache, 0o600)
        except OSError as e:
            print(f"  ⚠️  Could not cache token: {e}")

    def _token_request(self, grant: Dict[str, str], label: str) -> bool:
        try:
            resp = self.session.post(
                ZWIFT_TOKEN_URL,
                data={"client_id": ZWIFT_CLIENT_ID, **grant},
                headers={
                    "User-Agent": USER_AGENT,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                timeout=30,
            )
        except requests.RequestException as e:
            raise ZwiftError(f"Could not reach the Zwift auth server: {e}") from e

        if resp.status_code >= 400:
            detail = ""
            try:
                body = resp.json()
                detail = body.get("error_description") or body.get("error") or ""
            except ValueError:
                detail = resp.text[:200]
            if label == "refresh":
                # Expected once the refresh token ages out; the caller falls
                # back to a password grant.
                return False
            if resp.status_code in (400, 401):
                raise ZwiftError(
                    f"Zwift rejected the credentials ({resp.status_code}): {detail}\n"
                    "Check ZWIFT_USER and ZWIFT_PASS in .env — these are the "
                    "same details you use to sign in to the Zwift companion app."
                )
            raise ZwiftError(f"Zwift auth failed ({resp.status_code}): {detail}")

        body = resp.json()
        self.access_token = body.get("access_token")
        self.refresh_token = body.get("refresh_token") or self.refresh_token
        # Zwift reports expires_in in MILLISECONDS (86400000), not seconds as
        # OAuth specifies. Taking it at face value would park the refresh
        # 1000 days out and leave every call to recover from a 401 instead.
        lifetime = float(body.get("expires_in") or 3600)
        if lifetime > 1e6:
            lifetime /= 1000.0
        self.expires_at = time.time() + lifetime
        if not self.access_token:
            raise ZwiftError("Zwift auth returned no access token")
        self._save_cached_token()
        return True

    def authenticate(self, force: bool = False) -> None:
        """Obtain a usable access token, refreshing or logging in as needed."""
        if not force and self.access_token and time.time() < self.expires_at - 60:
            return

        if not force and self.refresh_token:
            if self._token_request(
                {"grant_type": "refresh_token", "refresh_token": self.refresh_token},
                "refresh",
            ):
                return
            print("  🔑 Refresh token rejected, logging in again…")
            self.refresh_token = None

        print("🔑  Logging in to Zwift…")
        self._token_request(
            {"grant_type": "password", "username": self.user, "password": self.password},
            "password",
        )
        print("✅  Authenticated with Zwift")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            # Several endpoints return protobuf unless JSON is requested.
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def request(self, method: str, path: str, params: Optional[Dict] = None,
                json_body: Optional[Any] = None) -> Any:
        """
        Single choke point for Zwift API calls.

        Owns token refresh on a 401, back-off on 5xx and rate limiting, and
        translation of error responses into ZwiftError.
        """
        self.authenticate()
        url = path if path.startswith("http") else f"{ZWIFT_API}{path}"
        last_status = None

        for attempt in range(3):
            try:
                resp = self.session.request(
                    method, url, params=params, json=json_body,
                    headers=self._headers(), timeout=60,
                )
            except requests.Timeout:
                print(f"    ⏳ Timeout on {path}, retrying…")
                time.sleep(5 * (attempt + 1))
                continue
            except requests.ConnectionError as e:
                print(f"    ⏳ Connection error on {path}: {e}")
                time.sleep(5 * (attempt + 1))
                continue

            last_status = resp.status_code

            if resp.status_code == 401 and attempt == 0:
                print("    🔑 Access token rejected, refreshing…")
                self.authenticate(force=True)
                continue

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                print(f"    ⏳ Rate limited, waiting {wait}s…")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                print(f"    ⏳ Zwift returned {resp.status_code}, backing off…")
                time.sleep(10 * (attempt + 1))
                continue

            if resp.status_code == 404:
                raise ZwiftError(f"Zwift has no resource at {path} (404)")

            if resp.status_code >= 400:
                raise ZwiftError(
                    f"Zwift error on {path}: {resp.status_code} {resp.text[:200]}"
                )

            if not resp.content:
                return {}
            try:
                return resp.json()
            except ValueError:
                raise ZwiftError(
                    f"Zwift returned a non-JSON response for {path} "
                    f"(content-type {resp.headers.get('Content-Type')}). "
                    "This endpoint may only speak protobuf."
                )

        raise ZwiftError(
            f"Failed to call {path} after 3 attempts (last HTTP status: {last_status})"
        )

    def get(self, path: str, params: Optional[Dict] = None) -> Any:
        return self.request("GET", path, params=params)

    # ------------------------------------------------------------------
    # API surface
    # ------------------------------------------------------------------

    def get_profile(self, player_id: str = "me") -> Dict[str, Any]:
        data = self.get(f"/api/profiles/{player_id}")
        if player_id == "me" and data.get("id"):
            self.player_id = str(data["id"])
            self.profile = data
            self._save_cached_token()
        return data

    def resolve_player_id(self) -> str:
        """The numeric athlete id, from cache, .env or the profile endpoint."""
        if self.player_id:
            return self.player_id
        env_id = os.getenv("ZWIFT_ATHLETE_ID")
        if env_id:
            self.player_id = str(env_id)
            return self.player_id
        self.get_profile("me")
        if not self.player_id:
            raise ZwiftError("Could not determine the Zwift athlete id")
        return self.player_id

    def get_activities(self, player_id: Optional[str] = None, start: int = 0,
                       limit: int = PAGE_SIZE) -> List[Dict[str, Any]]:
        pid = player_id or self.resolve_player_id()
        data = self.get(f"/api/profiles/{pid}/activities",
                        params={"start": start, "limit": limit})
        # The endpoint has returned both a bare list and an envelope over
        # time; accept either rather than depending on the current shape.
        if isinstance(data, dict):
            data = data.get("activities") or data.get("data") or []
        return data if isinstance(data, list) else []

    def get_activity(self, activity_id: str) -> Dict[str, Any]:
        return self.get(f"/api/activities/{activity_id}")

    def download_fit(self, bucket: str, key: str) -> bytes:
        """
        Fetch the original FIT file.

        This is a plain S3 object, not an API call: it takes no Authorization
        header and does not go through request().
        """
        url = f"https://{bucket}.s3.amazonaws.com/{key}"
        resp = self.session.get(url, timeout=120,
                                headers={"User-Agent": USER_AGENT})
        if resp.status_code != 200:
            raise ZwiftError(
                f"Could not download FIT ({resp.status_code}) from {url}"
            )
        return resp.content

    def get_segment_results(self, segment_id: str, world_id: int = 1,
                            player_id: Optional[str] = None,
                            from_epoch: Optional[int] = None,
                            to_epoch: Optional[int] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "world_id": world_id,
            "segment_id": segment_id,
            "only-signed": "true",
        }
        if player_id:
            params["player_id"] = player_id
        if from_epoch:
            params["from"] = from_epoch * 1000
        if to_epoch:
            params["to"] = to_epoch * 1000
        data = self.get("/api/segment-results", params=params)
        if isinstance(data, dict):
            data = data.get("segmentResults") or data.get("data") or []
        return data if isinstance(data, list) else []

    # ------------------------------------------------------------------
    # Writes
    #
    # Zwift's write surface is undocumented and thinner than the read side.
    # Both methods below are used by the MCP write tools, which push here
    # first and only then update the local row.
    # ------------------------------------------------------------------

    def rename_activity(self, activity_id: str, name: str) -> Dict[str, Any]:
        """
        Rename an activity.

        Zwift has no partial-update endpoint: the PUT replaces the whole
        resource, so the current activity is fetched and echoed back with
        only `name` changed.
        """
        activity = self.get_activity(activity_id)
        activity["name"] = name
        return self.request("PUT", f"/api/activities/{activity_id}",
                            json_body=activity) or {}

    def give_ride_on(self, activity_id: str, profile_id: str) -> Dict[str, Any]:
        """Give a ride on to someone else's activity."""
        me = self.resolve_player_id()
        return self.request(
            "POST",
            f"/api/profiles/{profile_id}/activities/{activity_id}/rideon",
            json_body={"profileId": int(me)},
        ) or {}


# ---------------------------------------------------------------------------
# ZwiftPower client
# ---------------------------------------------------------------------------

class ZwiftPowerError(RuntimeError):
    """Raised for ZwiftPower errors the caller should surface."""


class ZwiftPowerClient:
    """
    Session-cookie client for ZwiftPower.

    ZwiftPower is a phpBB site that authenticates through Zwift's SSO. There
    is no token and no API key: signing in means following the OAuth redirect
    into Keycloak, posting the login form, and letting the redirect chain set
    phpBB session cookies. Those cookies are cached and reused, because the
    login dance is slow and Zwift throttles it.

    Everything the site's own tables load is JSON behind that cookie, so no
    HTML scraping is needed for results — only for the profile header, where
    category and zFTP live.
    """

    def __init__(self, user: Optional[str] = None, password: Optional[str] = None,
                 session_cache: Path = ZP_SESSION_PATH):
        # ZwiftPower authenticates through Zwift SSO, so the same credentials
        # work unless the user deliberately separates them.
        self.user = user or os.getenv("ZWIFTPOWER_USER") or os.getenv("ZWIFT_USER")
        self.password = (password or os.getenv("ZWIFTPOWER_PASS")
                         or os.getenv("ZWIFT_PASS") or os.getenv("ZWIFT_PASSWORD"))
        self.session_cache = Path(session_cache)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": ZP_USER_AGENT})
        self.authenticated = False

        if not self.user or not self.password:
            raise ZwiftPowerError(
                "ZwiftPower needs credentials: set ZWIFT_USER/ZWIFT_PASS, or "
                "ZWIFTPOWER_USER/ZWIFTPOWER_PASS if they differ."
            )

    # ------------------------------------------------------------------
    # Session handling
    # ------------------------------------------------------------------

    def _load_cached_session(self) -> bool:
        if not self.session_cache.exists():
            return False
        try:
            cached = json.loads(self.session_cache.read_text())
        except (OSError, json.JSONDecodeError):
            return False
        if cached.get("user") != self.user:
            return False
        # phpBB sessions are long-lived but not forever; re-login daily.
        if time.time() - float(cached.get("issued_at") or 0) > 20 * 3600:
            return False
        for name, value in (cached.get("cookies") or {}).items():
            self.session.cookies.set(name, value, domain=".zwiftpower.com")
        return True

    def _save_cached_session(self) -> None:
        # Only the site's own cookies are worth keeping — the Keycloak ones
        # picked up in the redirect chain belong to a different domain.
        payload = {
            "user": self.user,
            "issued_at": time.time(),
            "cookies": {c.name: c.value for c in self.session.cookies
                        if "zwiftpower.com" in (c.domain or "")},
        }
        try:
            self.session_cache.write_text(json.dumps(payload))
            os.chmod(self.session_cache, 0o600)
        except OSError as e:
            print(f"  ⚠️  Could not cache ZwiftPower session: {e}")

    def authenticate(self, force: bool = False) -> None:
        if self.authenticated and not force:
            return
        if not force and self._load_cached_session() and self._verify_session():
            print("✅  Reusing cached ZwiftPower session")
            self.authenticated = True
            return

        print("🔑  Logging in to ZwiftPower via Zwift SSO…")
        self.session.cookies.clear()

        # 1. Follow the site's SSO entry point to Keycloak's login form.
        try:
            resp = self.session.get(ZP_SSO_URL, timeout=60, allow_redirects=True)
        except requests.RequestException as e:
            raise ZwiftPowerError(f"Could not reach ZwiftPower: {e}") from e

        if "zwiftpower.com" in resp.url and "secure.zwift.com" not in resp.url:
            # Already signed in — the redirect never left the site.
            if self._verify_session():
                self.authenticated = True
                self._save_cached_session()
                return

        # 2. The login form's action carries the Keycloak execution state,
        #    so it has to be read out of the page rather than constructed.
        action = self._extract_login_action(resp.text)
        if not action:
            raise ZwiftPowerError(
                "Could not find the Zwift SSO login form. The sign-in flow has "
                "probably changed — re-capture it with probe_zwift_api.py."
            )

        # 3. Post the credentials and follow the chain back to ZwiftPower.
        resp = self.session.post(
            action,
            data={"username": self.user, "password": self.password,
                  "rememberMe": "on"},
            headers={"Content-Type": "application/x-www-form-urlencoded",
                     "Referer": resp.url},
            timeout=60,
            allow_redirects=True,
        )

        if "secure.zwift.com" in resp.url or "Invalid username or password" in resp.text:
            raise ZwiftPowerError(
                "Zwift SSO rejected the credentials for ZwiftPower. Note that a "
                "Zwift account must be linked to ZwiftPower once, in a browser, "
                "before the API returns anything."
            )

        if not self._verify_session():
            raise ZwiftPowerError(
                "Signed in to Zwift SSO but ZwiftPower still refuses the "
                "session. If this account has never opened zwiftpower.com, "
                "do that once in a browser to create the profile."
            )

        self.authenticated = True
        self._save_cached_session()
        print("✅  Authenticated with ZwiftPower")

    @staticmethod
    def _extract_login_action(page: str) -> Optional[str]:
        """Pull the Keycloak form action (with its execution token) out of the HTML."""
        match = re.search(
            r'<form[^>]*id="kc-form-login"[^>]*action="([^"]+)"', page, re.I
        ) or re.search(r'<form[^>]*action="(https://secure\.zwift\.com[^"]+)"', page, re.I)
        return html.unescape(match.group(1)) if match else None

    def _verify_session(self) -> bool:
        """
        Is there a signed-in phpBB session?

        There is no cheap "am I logged in" endpoint — `api3.php` answers 200
        with an empty body whatever the session state, so probing it proves
        nothing. phpBB's own user cookie is the signal: it holds the board
        user id, and 1 is the anonymous guest. The board's cookie prefix
        contains an installation hash (`phpbb3_lswlk_u` today), so the name
        is matched by shape rather than spelled out.

        A cookie that is present but stale is caught later by `_json()`,
        which re-authenticates once when a JSON call comes back as HTML.
        """
        for cookie in self.session.cookies:
            if re.fullmatch(r"phpbb3_\w+_u", cookie.name):
                return bool(cookie.value) and cookie.value not in ("1", "")
        return False

    # ------------------------------------------------------------------
    # Data
    # ------------------------------------------------------------------

    def _json(self, url: str) -> Any:
        self.authenticate()
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
            except requests.RequestException as e:
                print(f"    ⏳ ZwiftPower request failed: {e}")
                time.sleep(5 * (attempt + 1))
                continue

            if resp.status_code in (401, 403) and attempt == 0:
                print("    🔑 ZwiftPower session expired, logging in again…")
                self.authenticate(force=True)
                continue
            if resp.status_code == 404:
                return None
            if resp.status_code >= 500:
                time.sleep(10 * (attempt + 1))
                continue
            if resp.status_code != 200:
                raise ZwiftPowerError(
                    f"ZwiftPower error {resp.status_code} for {url}"
                )
            try:
                return resp.json()
            except ValueError:
                # An HTML body here means the session silently lapsed.
                if attempt == 0:
                    self.authenticate(force=True)
                    continue
                raise ZwiftPowerError(
                    f"ZwiftPower returned HTML rather than JSON for {url}"
                )
        raise ZwiftPowerError(f"Gave up on {url} after 3 attempts")

    def profile_results(self, zwift_id: str) -> List[Dict[str, Any]]:
        """Every race ZwiftPower has scored for this rider."""
        data = (self._json(f"{ZP_BASE}/cache3/profile/{zwift_id}_all.json")
                or self._json(f"{ZP_BASE}/api3.php?do=profile_results&z={zwift_id}&type=all"))
        return (data or {}).get("data") or []

    def critical_power(self, zwift_id: str, kind: str = "watts") -> Dict[str, Any]:
        """The rider's critical-power curve as ZwiftPower computes it."""
        return self._json(
            f"{ZP_BASE}/api3.php?do=critical_power_profile"
            f"&zwift_id={zwift_id}&type={kind}"
        ) or {}

    def event_results(self, event_id: str) -> List[Dict[str, Any]]:
        """The full finishing field of one race."""
        data = (self._json(f"{ZP_BASE}/cache3/results/{event_id}_view.json")
                or self._json(f"{ZP_BASE}/api3.php?do=event_results&zid={event_id}"))
        return (data or {}).get("data") or []

    def profile_page(self, zwift_id: str) -> str:
        """The HTML profile, for the header values that have no JSON endpoint."""
        self.authenticate()
        resp = self.session.get(f"{ZP_BASE}/profile.php?z={zwift_id}", timeout=60)
        return resp.text if resp.status_code == 200 else ""


# ---------------------------------------------------------------------------
# FIT parsing
#
# The Zwift API exposes no laps, streams, or heart-rate/cadence averages —
# only the summary and a pointer at the original FIT file. Everything below
# the summary level therefore comes from parsing that file.
# ---------------------------------------------------------------------------

def parse_fit(payload: bytes) -> Dict[str, Any]:
    """
    Parse a FIT file into records, laps and a session summary.

    Returns whatever it managed to read, plus an `error` describing why it
    stopped early if it did. Zwift has written malformed files — some 2020
    activities are 500-byte stubs that reference an undefined local message
    and blow up mid-stream — and a decode error partway through a five-year
    archive must not cost the caller the frames it already had, let alone
    abort a batch of hundreds.

    fitdecode is imported here rather than at module scope so that a
    summary-only sync, and the MCP server, work without it installed.
    """
    try:
        import fitdecode
    except ImportError as e:
        raise RuntimeError(
            "Parsing FIT files needs fitdecode: pip install fitdecode"
        ) from e

    import io

    records: List[Dict[str, Any]] = []
    laps: List[Dict[str, Any]] = []
    session: Dict[str, Any] = {}

    def value(frame, name):
        try:
            return frame.get_value(name)
        except KeyError:
            return None

    error: Optional[str] = None

    # A CRC mismatch alone should not cost a whole ride: warn and read on.
    # Structural errors still raise, and are caught below.
    try:
        with fitdecode.FitReader(io.BytesIO(payload),
                                 check_crc=fitdecode.CrcCheck.WARN) as fit:
            for frame in fit:
                if frame.frame_type != fitdecode.FIT_FRAME_DATA:
                    continue

                if frame.name == "record":
                    ts = value(frame, "timestamp")
                    records.append({
                        "timestamp": int(ts.timestamp()) if ts else None,
                        "distance": _num(value(frame, "distance")),
                        "altitude": _num(value(frame, "enhanced_altitude")
                                         if value(frame, "enhanced_altitude") is not None
                                         else value(frame, "altitude")),
                        "speed": _num(value(frame, "enhanced_speed")
                                      if value(frame, "enhanced_speed") is not None
                                      else value(frame, "speed")),
                        "power": _num(value(frame, "power")),
                        "heart_rate": _int(value(frame, "heart_rate")),
                        "cadence": _int(value(frame, "cadence")),
                        "grade": _num(value(frame, "grade")),
                        "latitude": _semicircles(value(frame, "position_lat")),
                        "longitude": _semicircles(value(frame, "position_long")),
                    })

                elif frame.name == "lap":
                    ts = value(frame, "start_time")
                    laps.append({
                        "start_time": int(ts.timestamp()) if ts else None,
                        "elapsed_time_s": _num(value(frame, "total_elapsed_time")),
                        "timer_time_s": _num(value(frame, "total_timer_time")),
                        "distance": _num(value(frame, "total_distance")),
                        "avg_power": _num(value(frame, "avg_power")),
                        "max_power": _num(value(frame, "max_power")),
                        "np": _num(value(frame, "normalized_power")),
                        "avg_hr": _int(value(frame, "avg_heart_rate")),
                        "max_hr": _int(value(frame, "max_heart_rate")),
                        "avg_cadence": _int(value(frame, "avg_cadence")),
                        "max_cadence": _int(value(frame, "max_cadence")),
                        "avg_speed": _num(value(frame, "enhanced_avg_speed")
                                          if value(frame, "enhanced_avg_speed") is not None
                                          else value(frame, "avg_speed")),
                        "max_speed": _num(value(frame, "enhanced_max_speed")
                                          if value(frame, "enhanced_max_speed") is not None
                                          else value(frame, "max_speed")),
                        "ascent": _num(value(frame, "total_ascent")),
                        "descent": _num(value(frame, "total_descent")),
                        "calories": _num(value(frame, "total_calories")),
                    })

                elif frame.name == "session" and not session:
                    session = {
                        "distance": _num(value(frame, "total_distance")),
                        "elapsed_time_s": _num(value(frame, "total_elapsed_time")),
                        "timer_time_s": _num(value(frame, "total_timer_time")),
                        "avg_power": _num(value(frame, "avg_power")),
                        "max_power": _num(value(frame, "max_power")),
                        "np": _num(value(frame, "normalized_power")),
                        "avg_hr": _int(value(frame, "avg_heart_rate")),
                        "max_hr": _int(value(frame, "max_heart_rate")),
                        "avg_cadence": _int(value(frame, "avg_cadence")),
                        "max_cadence": _int(value(frame, "max_cadence")),
                        "avg_speed": _num(value(frame, "enhanced_avg_speed")
                                          if value(frame, "enhanced_avg_speed") is not None
                                          else value(frame, "avg_speed")),
                        "max_speed": _num(value(frame, "enhanced_max_speed")
                                          if value(frame, "enhanced_max_speed") is not None
                                          else value(frame, "max_speed")),
                        "ascent": _num(value(frame, "total_ascent")),
                        "descent": _num(value(frame, "total_descent")),
                        "calories": _num(value(frame, "total_calories")),
                        "work_kj": (_num(value(frame, "total_work")) or 0) / 1000.0 or None,
                    }
    except fitdecode.FitError as e:
        # Keep the frames already read: a file that dies at record 400
        # of 4000 still describes most of the ride.
        error = str(e)

    return {"records": records, "laps": laps, "session": session, "error": error}


def power_series(records: List[Dict[str, Any]]) -> List[float]:
    """
    A one-sample-per-second power series.

    Zwift records at 1 Hz, but pauses and dropped samples leave gaps. Gaps
    are filled with zeros so that a 20-minute window is genuinely 1200
    seconds of elapsed time rather than 1200 recorded samples — otherwise a
    ride with gaps would report inflated best efforts.
    """
    stamped = [(r["timestamp"], r.get("power") or 0.0)
               for r in records if r.get("timestamp")]
    if not stamped:
        return [r.get("power") or 0.0 for r in records]

    stamped.sort()
    t0 = stamped[0][0]
    span = stamped[-1][0] - t0 + 1
    # Guard against a corrupt timestamp turning into a multi-year array.
    if span <= 0 or span > 86400 * 2:
        return [p for _, p in stamped]

    series = [0.0] * span
    for ts, power in stamped:
        series[ts - t0] = power
    return series


def best_mean_power(series: List[float], duration: int) -> Optional[float]:
    """Highest average power over any `duration`-second window."""
    if len(series) < duration or duration <= 0:
        return None
    window = sum(series[:duration])
    best = window
    for i in range(duration, len(series)):
        window += series[i] - series[i - duration]
        if window > best:
            best = window
    return best / duration


def normalised_power(series: List[float]) -> Optional[float]:
    """Coggan NP: 30-second rolling average, fourth power, mean, fourth root."""
    if len(series) < 30:
        return None
    rolling = []
    window = sum(series[:30])
    rolling.append(window / 30)
    for i in range(30, len(series)):
        window += series[i] - series[i - 30]
        rolling.append(window / 30)
    fourth = sum(v ** 4 for v in rolling) / len(rolling)
    return fourth ** 0.25


def zone_distribution(values: Iterable[Optional[float]], threshold: float,
                      zones: List[Tuple[str, float, Optional[float]]],
                      metric: str) -> List[Dict[str, Any]]:
    """Seconds spent in each zone, given one sample per second."""
    buckets = [0.0] * len(zones)
    total = 0
    for v in values:
        if v is None:
            continue
        total += 1
        ratio = v / threshold if threshold else 0
        for i, (_, low, high) in enumerate(zones):
            if ratio >= low and (high is None or ratio < high):
                buckets[i] += 1
                break
    if not total:
        return []
    return [
        {
            "metric_type": metric,
            "zone_index": i + 1,
            "zone_name": name,
            "lower_bound": round(low * threshold, 1),
            "upper_bound": round(high * threshold, 1) if high is not None else None,
            "seconds": buckets[i],
            "percent": round(100.0 * buckets[i] / total, 2),
        }
        for i, (name, low, high) in enumerate(zones)
    ]


# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------

class ZwiftDownloader:
    """Fetches Zwift and ZwiftPower data into the SQLite database."""

    def __init__(self, db_path: str = DEFAULT_DB,
                 client: Optional[ZwiftClient] = None,
                 zp_client: Optional[ZwiftPowerClient] = None):
        self.db_path = db_path
        self.client = client or ZwiftClient()
        self._zp = zp_client
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.init_database()

    # -- infrastructure -------------------------------------------------

    def init_database(self) -> None:
        """Apply the schema file. Every statement is idempotent."""
        if not SCHEMA_PATH.exists():
            raise RuntimeError(f"Schema file not found: {SCHEMA_PATH}")
        # The MCP server reads this file while the nightly sync writes it.
        # Under the default rollback journal a write blocks every reader, so
        # a long recompute would hand live queries "database is locked". WAL
        # lets them run concurrently; it is a property of the file, so it
        # only has to be set once, and it survives in the copy.
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA busy_timeout = 10000")
        self.conn.executescript(SCHEMA_PATH.read_text())
        self.conn.commit()

    @property
    def zp(self) -> ZwiftPowerClient:
        """ZwiftPower client, created on first use so --skip-zp needs no login."""
        if self._zp is None:
            self._zp = ZwiftPowerClient()
        return self._zp

    def _record_sync(self, dataset: str, cursor: Optional[str] = None,
                     records: int = 0, status: str = "ok",
                     message: Optional[str] = None) -> None:
        _upsert(self.conn, "sync_state", {
            "dataset": dataset,
            "last_cursor": cursor,
            "last_synced_at": _now(),
            "records": records,
            "status": status,
            "message": message,
        })
        self.conn.commit()

    def _last_cursor(self, dataset: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT last_cursor FROM sync_state WHERE dataset = ? AND status = 'ok'",
            (dataset,),
        ).fetchone()
        return row["last_cursor"] if row else None

    def _athlete(self) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM athletes WHERE is_self = 1 "
            "ORDER BY synced_at DESC LIMIT 1"
        ).fetchone()

    def ftp(self, sport: Optional[str] = None) -> Optional[float]:
        """
        The threshold power a ride's intensity is scaled against.

        ZWIFT_FTP_OVERRIDE wins, then the in-game FTP, then ZwiftPower's zFTP.
        Zwift's FTP is whatever was last set in the game, so it can be stale
        in a way that quietly distorts every TSS in the database.

        Running is scored against ZWIFT_RUN_FTP when it is set. Without it,
        runs fall back to the cycling FTP: Zwift's running power is a
        different quantity, so those TSS values are indicative rather than
        comparable to the bike ones.
        """
        if sport and sport.upper() == "RUNNING":
            run_ftp = _num(os.getenv("ZWIFT_RUN_FTP"))
            if run_ftp:
                return run_ftp
        override = _num(os.getenv("ZWIFT_FTP_OVERRIDE"))
        if override:
            return override
        row = self._athlete()
        if row and row["ftp"]:
            return float(row["ftp"])
        zp = self.conn.execute(
            "SELECT zftp FROM zp_profile ORDER BY synced_at DESC LIMIT 1"
        ).fetchone()
        return float(zp["zftp"]) if zp and zp["zftp"] else None

    def weight(self) -> Optional[float]:
        row = self._athlete()
        return float(row["weight"]) if row and row["weight"] else None

    def _resolve_range(self, days_back: Optional[int], since: Optional[str],
                       full: bool) -> Optional[int]:
        """Earliest activity start time to sync, as epoch seconds."""
        if full:
            return None
        if days_back:
            return int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp())
        if since:
            return _epoch(since + "T00:00:00+00:00")
        cursor = self._last_cursor("activities")
        if cursor:
            # Overlap by a day: a ride saved late lands behind the watermark.
            return int(cursor) - 86400
        start = os.getenv("ZWIFT_START_DATE")
        if start:
            return _epoch(start + "T00:00:00+00:00")
        return int((datetime.now(timezone.utc) - timedelta(days=730)).timestamp())

    # -- profile --------------------------------------------------------

    def download_profile(self) -> Optional[str]:
        print("\n👤  Syncing Zwift profile…")
        data = self.client.get_profile("me")
        zwift_id = str(data.get("id") or "")
        if not zwift_id:
            self._record_sync("profile", status="error", message="no id in profile")
            print("  ⚠️  Profile response had no id")
            return None

        row = {
            "zwift_id": zwift_id,
            "first_name": data.get("firstName"),
            "last_name": data.get("lastName"),
            "display_name": (f"{data.get('firstName') or ''} "
                             f"{data.get('lastName') or ''}").strip() or None,
            "male": 1 if data.get("male") else 0,
            "age": _int(data.get("age")),
            "country_code": data.get("countryAlpha3") or data.get("countryCode"),
            "ftp": _num(data.get("ftp")),
            # The API reports grams and millimetres.
            "weight": (_num(data.get("weight")) or 0) / 1000.0 or None,
            "height": (_num(data.get("height")) or 0) / 10.0 or None,
            "max_hr": _int(data.get("maxHeartRate")),
            "rest_hr": _int(data.get("restingHeartRate")),
            "level": _int((_num(data.get("achievementLevel")) or 0) / 100) or None,
            "run_level": _int((_num(data.get("runAchievementLevel")) or 0) / 100) or None,
            "total_distance": _num(data.get("totalDistance")),
            "total_climbed": _num(data.get("totalDistanceClimbed")),
            "total_time_s": (_num(data.get("totalTimeInMinutes")) or 0) * 60 or None,
            "total_xp": _int(data.get("totalExperiencePoints")),
            "is_self": 1,
            "raw_json": json.dumps(data),
            "synced_at": _now(),
        }
        _upsert(self.conn, "athletes", row)
        self.conn.commit()
        self._record_sync("profile", cursor=zwift_id, records=1)

        print(f"  ✅ {row['display_name']} — id {zwift_id}, "
              f"FTP {row['ftp'] or '?'}W, {row['weight'] or '?'}kg")
        return zwift_id

    # -- activities -----------------------------------------------------

    def _activity_row(self, a: Dict[str, Any]) -> Dict[str, Any]:
        start = _epoch(a.get("startDate"))
        end = _epoch(a.get("endDate"))
        # Elapsed time is the awkward one: `duration` is whole minutes, and
        # the millisecond fields come and go, so prefer the timestamps.
        duration = (_ms_to_s(a.get("elapsedTimeInMs"))
                    or _ms_to_s(a.get("durationInMilliseconds")))
        if duration is None and start and end:
            duration = float(end - start)
        if duration is None and _num(a.get("duration")):
            duration = _num(a["duration"]) * 60.0
        avg_power = _num(a.get("avgWatts"))
        weight = self.weight()
        # Zwift dates are UTC, but a ride belongs to the calendar day the
        # rider was living in — a 00:30 CEST ride is "yesterday" in UTC.
        offset = _int(a.get("utcOffsetMinutes"))
        local_start = start + offset * 60 if (start and offset is not None) else start

        return {
            "activity_id": str(a.get("id") or a.get("id_str") or a.get("activityId")),
            "profile_id": str(a.get("profileId") or ""),
            "name": a.get("name") or a.get("activityName"),
            "description": a.get("description"),
            "sport": a.get("sport"),
            "world_id": _int(a.get("worldId")),
            "start_time": start,
            "end_time": end,
            "date": _yyyymmdd(local_start),
            "utc_offset_minutes": offset,
            "duration_s": duration,
            "moving_time_s": _ms_to_s(a.get("movingTimeInMs")),
            "distance": _num(a.get("distanceInMeters")),
            "elevation_gain": _num(a.get("totalElevation")),
            "calories": _num(a.get("calories")),
            "avg_power": avg_power,
            "avg_wkg": round(avg_power / weight, 2) if avg_power and weight else None,
            "privacy": a.get("privacy"),
            "ride_on_count": _int(a.get("activityRideOnCount")),
            "comment_count": _int(a.get("activityCommentCount")),
            "fit_bucket": a.get("fitFileBucket"),
            "fit_key": a.get("fitFileKey"),
            "raw_json": json.dumps(a),
            "synced_at": _now(),
        }

    def _merge_activity(self, row: Dict[str, Any]) -> None:
        """
        Write an activity, preserving everything the list endpoint cannot see.

        INSERT OR REPLACE rewrites the whole row, so the FIT-derived columns
        and the local annotations have to be carried forward explicitly or a
        routine re-sync would silently wipe them.
        """
        carry = (
            "max_power", "np", "intensity_factor", "tss", "work_kj",
            "avg_hr", "max_hr", "avg_cadence", "max_cadence",
            "avg_speed", "max_speed", "profile_ftp", "tss_source",
            # The FIT pointer is carried too: a payload that omits it must
            # not cost an activity the only route it has to detail.
            "fit_bucket", "fit_key", "fit_path", "fit_synced_at",
            "api_detail_synced_at", "detail_synced_at", "sample_count",
            "zp_event_id", "local_tags", "local_notes",
        )
        existing = self.conn.execute(
            "SELECT * FROM activities WHERE activity_id = ?",
            (row["activity_id"],),
        ).fetchone()
        if existing:
            for column in carry:
                if row.get(column) is None:
                    row[column] = existing[column]
        _upsert(self.conn, "activities", row)

    def download_activities(self, days_back: Optional[int] = None,
                            since: Optional[str] = None,
                            full: bool = False) -> int:
        cutoff = self._resolve_range(days_back, since, full)
        label = _iso_day(cutoff) if cutoff else "the beginning"
        print(f"\n🚴  Syncing activities since {label}…")

        pid = self.client.resolve_player_id()
        start, total, newest = 0, 0, None

        while True:
            page = self.client.get_activities(pid, start=start, limit=PAGE_SIZE)
            if not page:
                break

            stop = False
            for a in page:
                row = self._activity_row(a)
                if not row["activity_id"] or row["activity_id"] == "None":
                    continue
                if cutoff and row["start_time"] and row["start_time"] < cutoff:
                    stop = True
                    break
                self._merge_activity(row)
                total += 1
                if row["start_time"] and (newest is None or row["start_time"] > newest):
                    newest = row["start_time"]

            self.conn.commit()
            print(f"  … {total} activities")

            if stop or len(page) < PAGE_SIZE:
                break
            start += PAGE_SIZE
            time.sleep(0.5)

        self._record_sync("activities", cursor=str(newest) if newest else None,
                          records=total)
        print(f"  ✅ {total} activities stored")
        return total

    # -- detail via the API ----------------------------------------------

    def enrich_activity_details(self, limit: Optional[int] = None,
                                refresh: bool = False) -> int:
        """
        Fill in what only `/api/activities/{id}` knows.

        The list endpoint carries no heart rate, cadence, speed or max power
        at all — those live on the detail endpoint, along with `profileFtp`,
        the FTP Zwift had for you when the ride happened. That last one is
        why this runs before any training-load maths: scaling a 2021 ride
        against today's FTP silently rewrites history.

        One cheap request per activity, so it runs for everything missing it
        rather than being gated behind a flag like the FIT pass.
        """
        sql = ("SELECT activity_id FROM activities "
               + ("" if refresh else "WHERE api_detail_synced_at IS NULL ")
               + "ORDER BY start_time DESC")
        if limit:
            sql += f" LIMIT {int(limit)}"
        pending = [r["activity_id"] for r in self.conn.execute(sql).fetchall()]
        if not pending:
            return 0

        print(f"\n📋  Fetching detail for {len(pending)} activities…")
        done = 0
        profile_max_hr = None
        for i, activity_id in enumerate(pending, 1):
            try:
                detail = self.client.get_activity(activity_id)
            except ZwiftError as e:
                print(f"    ⚠️  {activity_id}: {e}")
                continue

            update = {
                "avg_hr": _int(detail.get("avgHeartRate")),
                "max_hr": _int(detail.get("maxHeartRate")),
                "avg_cadence": _int(detail.get("avgCadenceInRotationsPerMinute")),
                "max_cadence": _int(detail.get("maxCadenceInRotationsPerMinute")),
                "avg_speed": _num(detail.get("avgSpeedInMetersPerSecond")),
                "max_speed": _num(detail.get("maxSpeedInMetersPerSecond")),
                "max_power": _num(detail.get("maxWatts")),
                "profile_ftp": _num(detail.get("profileFtp")),
                "description": detail.get("description"),
                "api_detail_synced_at": _now(),
            }
            profile_max_hr = profile_max_hr or _int(detail.get("profileMaxHeartRate"))
            # Zero here means "not recorded" (a run with no power meter),
            # not a real zero — storing it would drag averages down.
            for key in ("avg_hr", "max_hr", "avg_cadence", "max_cadence",
                        "max_power", "avg_speed", "max_speed"):
                if not update[key]:
                    update[key] = None

            self.conn.execute(
                "UPDATE activities SET "
                + ", ".join(f"{k} = COALESCE(?, {k})" for k in update)
                + " WHERE activity_id = ?",
                list(update.values()) + [activity_id],
            )
            done += 1
            if i % 25 == 0:
                self.conn.commit()
                print(f"  … {i}/{len(pending)}")
            time.sleep(0.2)

        self.conn.commit()
        self._record_sync("activity_api_detail", records=done)
        print(f"  ✅ {done} activities enriched")

        # The profile endpoint has no max heart rate, but every activity
        # detail carries the value the game holds — so backfill it here.
        self._backfill_max_hr(profile_max_hr)
        return done

    def _backfill_max_hr(self, profile_max_hr: Optional[int] = None) -> None:
        """
        Give the athlete a max HR, which /api/profiles/me does not return.

        `profileMaxHeartRate` from any activity detail is the value the game
        holds. Failing that, the highest heart rate ever recorded is a poor
        but honest substitute — without one there are no HR zones at all.
        """
        athlete = self._athlete()
        if not athlete or athlete["max_hr"]:
            return
        source = "the game profile"
        value = profile_max_hr
        if not value:
            value = self.conn.execute(
                "SELECT MAX(max_hr) FROM activities WHERE max_hr > 0"
            ).fetchone()[0]
            source = "the highest reading on record"
        if value:
            self.conn.execute(
                "UPDATE athletes SET max_hr = ? WHERE zwift_id = ?",
                (int(value), athlete["zwift_id"]),
            )
            self.conn.commit()
            print(f"  ℹ️  Max HR taken from {source}: {int(value)} bpm")

    # -- detail via FIT --------------------------------------------------

    def activities_missing_detail(self, limit: Optional[int] = None,
                                  redo: bool = False) -> List[sqlite3.Row]:
        """
        Activities whose FIT still needs parsing.

        `redo` re-selects ones already parsed, which is what you want after
        changing how the file is interpreted — the parse is what produced the
        stored values, so fixing the parser does not fix old rows by itself.
        """
        sql = ("SELECT activity_id, fit_bucket, fit_key, name FROM activities "
               "WHERE fit_key IS NOT NULL "
               + ("" if redo else "AND detail_synced_at IS NULL ")
               + "ORDER BY start_time DESC")
        if limit:
            sql += f" LIMIT {int(limit)}"
        return self.conn.execute(sql).fetchall()

    def _fit_bytes(self, activity_id: str, bucket: str, key: str,
                   refresh: bool = False) -> bytes:
        """Fetch the FIT, caching it locally so a re-parse never re-downloads."""
        FIT_CACHE.mkdir(parents=True, exist_ok=True)
        path = FIT_CACHE / f"{activity_id}.fit"
        if path.exists() and not refresh:
            return path.read_bytes()
        payload = self.client.download_fit(bucket, key)
        path.write_bytes(payload)
        self.conn.execute(
            "UPDATE activities SET fit_path = ?, fit_synced_at = ? WHERE activity_id = ?",
            (str(path), _now(), activity_id),
        )
        return payload

    def download_activity_detail(self, activity_id: str, bucket: str, key: str,
                                 with_samples: bool = False,
                                 refresh: bool = False) -> bool:
        """Download and parse one activity's FIT into laps, zones and the curve."""
        try:
            payload = self._fit_bytes(activity_id, bucket, key, refresh)
            parsed = parse_fit(payload)
        except (ZwiftError, RuntimeError, OSError) as e:
            print(f"    ⚠️  {activity_id}: {e}")
            return False
        except Exception as e:                           # noqa: BLE001
            # These are third-party binary files spanning years of format
            # changes. Whatever one of them does, the batch goes on.
            print(f"    ⚠️  {activity_id}: unexpected error reading the FIT: "
                  f"{type(e).__name__}: {e}")
            return False

        records, laps, session = parsed["records"], parsed["laps"], parsed["session"]
        if not records:
            reason = parsed.get("error") or "it contained no records"
            print(f"    ⚠️  {activity_id}: no usable data in the FIT — {reason}")
            return False
        if parsed.get("error"):
            print(f"    ↪︎ partial: kept {len(records)} records, then "
                  f"{parsed['error']}")

        athlete = self._athlete()
        weight = self.weight()
        series = power_series(records)

        existing = self.conn.execute(
            "SELECT * FROM activities WHERE activity_id = ?", (activity_id,)
        ).fetchone()
        # Scale against the FTP Zwift held at the time, not today's — an old
        # ride judged by a current FTP reports the wrong intensity forever.
        sport = existing["sport"] if existing else None
        ftp = (existing["profile_ftp"] if existing and existing["profile_ftp"]
               else self.ftp(sport))
        if sport and sport.upper() == "RUNNING" and _num(os.getenv("ZWIFT_RUN_FTP")):
            ftp = _num(os.getenv("ZWIFT_RUN_FTP"))

        # -- summary fields the API does not provide
        hrs = [r["heart_rate"] for r in records if r.get("heart_rate")]
        cads = [r["cadence"] for r in records if r.get("cadence")]
        speeds = [r["speed"] for r in records if r.get("speed")]

        # A run recorded without a power meter still has a power channel in
        # its FIT — full of zeros. Treating that as data yields NP 0, work 0
        # and "100% of the ride in Z1", which is worse than saying nothing.
        # (Zwift's own estimated running power lives on the API summary, not
        # in the file, so it is unaffected.)
        has_power = any(v > 0 for v in series)

        np = (session.get("np") or normalised_power(series)) if has_power else None
        work_kj = session.get("work_kj")
        if work_kj is None and has_power:
            work_kj = sum(series) / 1000.0

        update = {
            "max_power": (session.get("max_power")
                          or (max(series) if has_power else None)),
            "np": np,
            "work_kj": work_kj,
            "avg_hr": session.get("avg_hr") or (round(sum(hrs) / len(hrs)) if hrs else None),
            "max_hr": session.get("max_hr") or (max(hrs) if hrs else None),
            "avg_cadence": session.get("avg_cadence") or (round(sum(cads) / len(cads)) if cads else None),
            "max_cadence": session.get("max_cadence") or (max(cads) if cads else None),
            "avg_speed": session.get("avg_speed") or (sum(speeds) / len(speeds) if speeds else None),
            "max_speed": session.get("max_speed") or (max(speeds) if speeds else None),
            "sample_count": len(records),
            "detail_synced_at": _now(),
        }
        # Where the API already gave a figure, keep it: it is what Zwift
        # itself displays, and a FIT recomputation would only introduce a
        # second, slightly different answer to the same question.
        if existing:
            for column in ("max_power", "avg_hr", "max_hr", "avg_cadence",
                           "max_cadence", "avg_speed", "max_speed"):
                # Truthy, not "is not None": a stored 0 here means the
                # channel was absent, and keeping it would pin the column at
                # zero forever once one sync had written it.
                if existing[column]:
                    update[column] = existing[column]
        if ftp and np:
            update["intensity_factor"] = np / ftp
            duration = len(series)
            update["tss"] = (duration * np * (np / ftp)) / (ftp * 3600) * 100
            update["tss_source"] = "np"

        self.conn.execute(
            "UPDATE activities SET "
            + ", ".join(f"{k} = ?" for k in update)
            + " WHERE activity_id = ?",
            list(update.values()) + [activity_id],
        )

        # -- laps
        self.conn.execute("DELETE FROM activity_laps WHERE activity_id = ?", (activity_id,))
        for i, lap in enumerate(laps):
            _upsert(self.conn, "activity_laps",
                    {"activity_id": activity_id, "lap_index": i, **lap})

        # -- power curve
        self.conn.execute("DELETE FROM power_curve WHERE activity_id = ?", (activity_id,))
        for duration in (CURVE_DURATIONS if has_power else ()):
            watts = best_mean_power(series, duration)
            if watts is None:
                continue
            _upsert(self.conn, "power_curve", {
                "activity_id": activity_id,
                "duration_s": duration,
                "watts": round(watts, 1),
                "wkg": round(watts / weight, 2) if weight else None,
            })

        # -- time in zone
        self.conn.execute(
            "DELETE FROM activity_zone_distribution WHERE activity_id = ?",
            (activity_id,),
        )
        zones: List[Dict[str, Any]] = []
        if ftp and has_power:
            zones += zone_distribution(series, ftp, POWER_ZONES, "power")
        max_hr = athlete["max_hr"] if athlete else None
        if not max_hr and hrs:
            max_hr = max(hrs)
        if max_hr:
            zones += zone_distribution(
                [r.get("heart_rate") for r in records], float(max_hr), HR_ZONES, "hr"
            )
        for zone in zones:
            _upsert(self.conn, "activity_zone_distribution",
                    {"activity_id": activity_id, **zone})

        # -- samples, only on request
        if with_samples:
            self.conn.execute("DELETE FROM activity_samples WHERE activity_id = ?",
                              (activity_id,))
            t0 = next((r["timestamp"] for r in records if r["timestamp"]), None)
            for i, r in enumerate(records):
                _upsert(self.conn, "activity_samples", {
                    "activity_id": activity_id,
                    "sample_index": (r["timestamp"] - t0) if (r["timestamp"] and t0) else i,
                    **{k: r[k] for k in (
                        "timestamp", "distance", "altitude", "speed", "power",
                        "heart_rate", "cadence", "grade", "latitude", "longitude")},
                })

        self.conn.commit()
        return True

    def backfill_detail(self, limit: Optional[int] = None,
                        with_samples: bool = False,
                        refresh: bool = False, redo: bool = False) -> int:
        pending = self.activities_missing_detail(limit, redo)
        if not pending:
            print("\n📄  No activities need detail")
            return 0

        print(f"\n📄  Downloading FIT detail for {len(pending)} activities…")
        done = 0
        failed: List[str] = []
        for i, row in enumerate(pending, 1):
            print(f"  [{i}/{len(pending)}] {row['name'] or row['activity_id']}")
            try:
                ok = self.download_activity_detail(
                    row["activity_id"], row["fit_bucket"], row["fit_key"],
                    with_samples, refresh)
            except Exception as e:                       # noqa: BLE001
                print(f"    ⚠️  {row['activity_id']}: {type(e).__name__}: {e}")
                ok = False
            if ok:
                done += 1
            else:
                failed.append(row["activity_id"])
            time.sleep(0.3)

        # Leaving detail_synced_at NULL means a failure is retried on the next
        # run. That is cheap — the FIT is already cached — and right, because
        # the fix for most of these is a parser change, not a re-download.
        message = (f"{len(failed)} unreadable: {', '.join(failed[:10])}"
                   f"{' …' if len(failed) > 10 else ''}") if failed else None
        self._record_sync("activity_detail", records=done,
                          status="ok" if not failed else "partial",
                          message=message)

        print(f"  ✅ {done} activities detailed"
              + (f", {len(failed)} skipped as unreadable" if failed else ""))
        if failed:
            print(f"     ids: {', '.join(failed[:10])}"
                  f"{' …' if len(failed) > 10 else ''}")
        return done

    # -- ZwiftPower ------------------------------------------------------

    def download_zwiftpower(self, zwift_id: Optional[str] = None,
                            with_fields: bool = False,
                            max_events: Optional[int] = None) -> int:
        """Sync race results, the critical-power curve and the ZP profile."""
        zwift_id = zwift_id or self.client.resolve_player_id()
        print(f"\n🏁  Syncing ZwiftPower for athlete {zwift_id}…")

        try:
            results = self.zp.profile_results(zwift_id)
        except ZwiftPowerError as e:
            # ZwiftPower is a separate site with its own outages and its own
            # long-running uncertainty about its future. A failure here must
            # not fail the whole sync — the game data is the primary source.
            self._record_sync("zwiftpower", status="error", message=str(e))
            print(f"  ⚠️  ZwiftPower unavailable: {e}")
            return 0

        stored = 0
        for r in results:
            event_id = _zp_str(r, "zid", "event_id")
            if not event_id:
                continue
            row = {
                "event_id": event_id,
                "zwift_id": str(zwift_id),
                "event_title": _zp_str(r, "event_title", "title", "name"),
                "event_date": _epoch(_zp(r.get("event_date") or r.get("date"))),
                "category": _zp_str(r, "category", "cat"),
                "position": _int(_zp(r.get("pos"))),
                "position_in_cat": _int(_zp(r.get("position_in_cat"))),
                "time_s": _zp_num(r, "time", "time_gun"),
                "time_gap_s": _zp_num(r, "gap", "tdiff"),
                "avg_power": _zp_num(r, "avg_power", "ap"),
                "np": _zp_num(r, "np"),
                "avg_wkg": _zp_num(r, "avg_wkg", "wkg"),
                "zp_ftp": _zp_num(r, "wftp", "ftp"),
                "avg_hr": _int(_zp_num(r, "avg_hr", "hr")),
                "max_hr": _int(_zp_num(r, "max_hr", "hrmax")),
                "weight": _zp_num(r, "weight"),
                "height": _zp_num(r, "height"),
                # ZwiftPower reports an age band ("Vet", "Snr"), not a number.
                "age": _zp_str(r, "age"),
                "wkg_5s": _zp_num(r, "wkg5"),
                "wkg_15s": _zp_num(r, "wkg15"),
                "wkg_30s": _zp_num(r, "wkg30"),
                "wkg_60s": _zp_num(r, "wkg60"),
                "wkg_5m": _zp_num(r, "wkg300"),
                "wkg_20m": _zp_num(r, "wkg1200"),
                "w_5s": _zp_num(r, "w5"),
                "w_15s": _zp_num(r, "w15"),
                "w_30s": _zp_num(r, "w30"),
                "w_60s": _zp_num(r, "w60"),
                "w_5m": _zp_num(r, "w300"),
                "w_20m": _zp_num(r, "w1200"),
                "flagged": 1 if _zp(r.get("zada")) else 0,
                "raw_json": json.dumps(r),
                "synced_at": _now(),
            }
            # Preserve a link resolved on an earlier run.
            existing = self.conn.execute(
                "SELECT activity_id FROM zp_results WHERE event_id = ? AND zwift_id = ?",
                (event_id, str(zwift_id)),
            ).fetchone()
            if existing and existing["activity_id"]:
                row["activity_id"] = existing["activity_id"]

            _upsert(self.conn, "zp_results", row)
            stored += 1

        self.conn.commit()
        print(f"  ✅ {stored} race results")

        self._download_zp_profile(zwift_id)
        self._download_zp_critical_power(zwift_id)

        if with_fields:
            self._download_zp_fields(zwift_id, max_events)

        self._record_sync("zwiftpower", cursor=str(zwift_id), records=stored)
        return stored

    def _download_zp_profile(self, zwift_id: str) -> None:
        """Category, zFTP and racing score — header values with no JSON endpoint."""
        try:
            page = self.zp.profile_page(zwift_id)
        except ZwiftPowerError as e:
            print(f"  ⚠️  ZwiftPower profile unavailable: {e}")
            return
        if not page:
            return

        latest = self.conn.execute(
            "SELECT * FROM zp_results WHERE zwift_id = ? "
            "ORDER BY event_date DESC LIMIT 1", (str(zwift_id),)
        ).fetchone()

        # The header is a plain two-column table, so each value is read from
        # the cell beside its own label. Do not match on the CSS class alone:
        # `label-cat-E` also styles the in-game level badge, which sits above
        # the real category and will happily answer in its place.
        def cell(label: str) -> Optional[str]:
            m = re.search(
                rf">\s*{label}\s*(?:</label>)?\s*</th>\s*<td[^>]*>(.*?)</td>",
                page, re.I | re.S,
            )
            if not m:
                return None
            # &nbsp; survives unescaping as \xa0, which then defeats every
            # later split and strip — normalise it away with the whitespace.
            text = html.unescape(re.sub(r"<[^>]+>", " ", m.group(1)))
            text = " ".join(text.replace("\xa0", " ").split())
            # ZwiftPower prints an em-dash run where it has no value.
            return None if text.strip("- ") == "" else text

        def leading_number(text: Optional[str]) -> Optional[float]:
            m = re.match(r"\s*([\d.]+)", text or "")
            return _num(m.group(1)) if m else None

        category = re.search(
            r"Category \(Pace Group\).*?<span[^>]*>\s*([A-E])\s*</span>",
            page, re.I | re.S,
        )
        title = re.search(r"<title>\s*(.*?)\s*</title>", page, re.I | re.S)

        row = {
            "zwift_id": str(zwift_id),
            # The page title is "ZwiftPower - <rider>", joined by a
            # non-breaking space rather than a plain one.
            "name": (re.sub(r"^ZwiftPower\s*-\s*", "",
                            html.unescape(title.group(1)).replace("\xa0", " ")).strip()
                     if title else None),
            "category": category.group(1) if category else None,
            "racing_score": leading_number(cell("Zwift Racing Score")),
            "zftp": leading_number(cell("zFTP")),
            "zmap": leading_number(cell("zMAP")),
            "weight": leading_number(cell("Weight")) or (latest["weight"] if latest else None),
            "height": latest["height"] if latest else None,
            "age": cell("Age") or (latest["age"] if latest else None),
            "country": cell("Country"),
            # From the header row, not the first team.php link on the page —
            # that one belongs to the navigation.
            "team": cell("Team"),
            "races_count": self.conn.execute(
                "SELECT COUNT(*) FROM zp_results WHERE zwift_id = ?", (str(zwift_id),)
            ).fetchone()[0],
            "synced_at": _now(),
        }
        _upsert(self.conn, "zp_profile", row)
        self.conn.commit()
        print(f"  ✅ ZwiftPower profile — category {row['category'] or '?'}, "
              f"zFTP {row['zftp'] or '?'}W")

    def _download_zp_critical_power(self, zwift_id: str) -> None:
        try:
            data = self.zp.critical_power(zwift_id)
        except ZwiftPowerError as e:
            print(f"  ⚠️  ZwiftPower critical power unavailable: {e}")
            return

        efforts = data.get("efforts") if isinstance(data, dict) else None

        # ZwiftPower answers `{"info": [], "efforts": []}` — an empty list,
        # not an empty object — when it has nothing to show, which is the
        # normal state for a rider with no recent races. Only a dict of
        # duration → points carries a curve.
        if isinstance(efforts, list) and not efforts:
            print("  ℹ️  ZwiftPower has no critical-power curve for this rider "
                  "(it is built from races, and only recent ones)")
            return
        if not isinstance(efforts, dict):
            print(f"  ⚠️  Unrecognised critical-power shape "
                  f"({type(efforts).__name__}); skipping")
            return

        weight = self.weight()
        stored = 0
        for duration, points in efforts.items():
            secs = _int(duration)
            if not secs or not points:
                continue
            # Each duration is a list of {x, y, date, zid} style points; the
            # best effort is the highest y.
            best = None
            for p in points:
                if isinstance(p, dict):
                    watts = _num(p.get("y") or p.get("watts"))
                    meta = p
                elif isinstance(p, (list, tuple)) and len(p) >= 2:
                    watts, meta = _num(p[1]), {}
                else:
                    continue
                if watts is not None and (best is None or watts > best[0]):
                    best = (watts, meta)
            if not best:
                continue
            watts, meta = best
            _upsert(self.conn, "zp_critical_power", {
                "zwift_id": str(zwift_id),
                "duration_s": secs,
                "watts": round(watts, 1),
                "wkg": round(watts / weight, 2) if weight else None,
                "effort_date": _epoch(meta.get("date") or meta.get("x")),
                "event_id": str(meta.get("zid")) if meta.get("zid") else None,
                "synced_at": _now(),
            })
            stored += 1

        self.conn.commit()
        if stored:
            print(f"  ✅ {stored} critical-power points")

    def _download_zp_fields(self, zwift_id: str, max_events: Optional[int]) -> None:
        """Full finishing fields for the races the athlete rode."""
        rows = self.conn.execute(
            "SELECT DISTINCT event_id FROM zp_results WHERE zwift_id = ? "
            "ORDER BY event_date DESC" + (f" LIMIT {int(max_events)}" if max_events else ""),
            (str(zwift_id),),
        ).fetchall()

        print(f"  🧑‍🤝‍🧑 Fetching finishing fields for {len(rows)} races…")
        stored = 0
        for row in rows:
            event_id = row["event_id"]
            try:
                field = self.zp.event_results(event_id)
            except ZwiftPowerError as e:
                print(f"    ⚠️  {event_id}: {e}")
                continue
            for r in field or []:
                rider = _zp_str(r, "zwid", "zwift_id")
                if not rider:
                    continue
                _upsert(self.conn, "zp_event_results", {
                    "event_id": event_id,
                    "zwift_id": rider,
                    "name": _zp_str(r, "name"),
                    "team": _zp_str(r, "tname", "team"),
                    "category": _zp_str(r, "category", "cat"),
                    "position": _int(_zp(r.get("pos"))),
                    "position_in_cat": _int(_zp(r.get("position_in_cat"))),
                    "time_s": _zp_num(r, "time", "time_gun"),
                    "avg_power": _zp_num(r, "avg_power"),
                    "avg_wkg": _zp_num(r, "avg_wkg"),
                    "weight": _zp_num(r, "weight"),
                    "raw_json": json.dumps(r),
                    "synced_at": _now(),
                })
                stored += 1
            self.conn.commit()
            time.sleep(0.5)
        print(f"  ✅ {stored} field entries")

    def link_zp_results(self, tolerance_s: int = 3600) -> int:
        """
        Match ZwiftPower races to local activities.

        The two systems share no identifier: ZwiftPower keys on its own event
        id, Zwift on the activity. The only thing they agree on is when the
        ride happened, so a result is linked to the activity whose start time
        is nearest the race start within `tolerance_s`. Unmatched results are
        normal — a race ridden on another account's activity, or a ride never
        saved, simply has no counterpart.
        """
        rows = self.conn.execute(
            "SELECT event_id, zwift_id, event_date FROM zp_results "
            "WHERE activity_id IS NULL AND event_date IS NOT NULL"
        ).fetchall()

        linked = 0
        for r in rows:
            match = self.conn.execute(
                "SELECT activity_id, ABS(start_time - ?) AS delta FROM activities "
                "WHERE start_time IS NOT NULL AND ABS(start_time - ?) <= ? "
                "ORDER BY delta LIMIT 1",
                (r["event_date"], r["event_date"], tolerance_s),
            ).fetchone()
            if not match:
                continue
            self.conn.execute(
                "UPDATE zp_results SET activity_id = ? WHERE event_id = ? AND zwift_id = ?",
                (match["activity_id"], r["event_id"], r["zwift_id"]),
            )
            self.conn.execute(
                "UPDATE activities SET zp_event_id = ? WHERE activity_id = ?",
                (r["event_id"], match["activity_id"]),
            )
            linked += 1

        self.conn.commit()
        if linked:
            print(f"  🔗 Linked {linked} race results to activities")
        return linked

    # -- derived metrics -------------------------------------------------

    def recompute_training_load(self) -> int:
        """
        Rebuild daily_metrics from the activities table.

        CTL and ATL are exponentially weighted averages of daily TSS with 42
        and 7 day time constants, so they depend on every preceding day —
        which is why the whole table is recomputed rather than appended to.
        Days without a ride still count, as zero.
        """
        ftp = self.ftp()
        print(f"\n📊  Recomputing training load (FTP {ftp or 'unknown'}W)…")

        # Rides with no parsed FIT have no NP, so estimate their TSS from
        # average power and mark it as an estimate. Writing it onto the
        # activity rather than only into the daily total matters: otherwise
        # `activities.tss` and `daily_metrics.tss` answer the same question
        # with different numbers depending on which one you happen to read.
        # Rows already scored from a FIT ('np') are never overwritten.
        run_ftp = _num(os.getenv("ZWIFT_RUN_FTP"))
        self.conn.execute("""
            WITH threshold AS (
                SELECT activity_id,
                       CASE WHEN UPPER(sport) = 'RUNNING' AND :run_ftp > 0
                            THEN :run_ftp
                            ELSE COALESCE(NULLIF(profile_ftp, 0), :ftp)
                       END AS ftp
                FROM activities
            )
            UPDATE activities
               SET tss = (duration_s * avg_power
                          * (avg_power / (SELECT ftp FROM threshold t
                                           WHERE t.activity_id = activities.activity_id)))
                         / ((SELECT ftp FROM threshold t
                              WHERE t.activity_id = activities.activity_id) * 3600.0) * 100,
                   intensity_factor = avg_power
                          / (SELECT ftp FROM threshold t
                              WHERE t.activity_id = activities.activity_id),
                   tss_source = 'avg_power'
             WHERE (tss IS NULL OR tss_source = 'avg_power')
               AND avg_power > 0 AND duration_s > 0
               AND (SELECT ftp FROM threshold t
                     WHERE t.activity_id = activities.activity_id) > 0
        """, {"ftp": ftp or 0, "run_ftp": run_ftp or 0})

        # Group by the rider's local day, matching activities.date — a ride
        # that starts at 00:30 local belongs to the day it felt like.
        rows = self.conn.execute("""
            SELECT date(start_time + COALESCE(utc_offset_minutes, 0) * 60,
                        'unixepoch') AS day,
                   COUNT(*) AS n,
                   SUM(duration_s) AS duration,
                   SUM(distance) AS distance,
                   SUM(elevation_gain) AS elevation,
                   SUM(tss) AS tss
            FROM activities
            WHERE start_time IS NOT NULL
            GROUP BY day ORDER BY day
        """).fetchall()
        # sqlite3 reports -1 for rowcount on a CTE-prefixed UPDATE, so count
        # the result rather than trusting it.
        estimated = self.conn.execute(
            "SELECT COUNT(*) FROM activities WHERE tss_source = 'avg_power'"
        ).fetchone()[0]
        if estimated:
            print(f"  … {estimated} activities scored from average power "
                  f"(no FIT parsed; tss_source='avg_power')")

        if not rows:
            print("  … no activities yet")
            return 0

        by_day = {r["day"]: r for r in rows}
        first = date.fromisoformat(rows[0]["day"])
        last = date.fromisoformat(rows[-1]["day"])

        ctl_k = 1 - math.exp(-1 / CTL_TIME_CONSTANT)
        atl_k = 1 - math.exp(-1 / ATL_TIME_CONSTANT)
        ctl = atl = 0.0
        written = 0

        self.conn.execute("DELETE FROM daily_metrics")
        day = first
        while day <= last:
            key = day.isoformat()
            r = by_day.get(key)
            tss = float(r["tss"] or 0) if r else 0.0
            ctl += (tss - ctl) * ctl_k
            atl += (tss - atl) * atl_k
            _upsert(self.conn, "daily_metrics", {
                "calendar_date": key,
                "activity_count": r["n"] if r else 0,
                "duration_s": r["duration"] if r else 0,
                "distance": r["distance"] if r else 0,
                "elevation_gain": r["elevation"] if r else 0,
                "tss": round(tss, 1),
                "ctl": round(ctl, 1),
                "atl": round(atl, 1),
                "tsb": round(ctl - atl, 1),
                "ftp_used": ftp,
                "updated_at": _now(),
            })
            written += 1
            day += timedelta(days=1)

        self.conn.commit()
        self._record_sync("daily_metrics", records=written)
        print(f"  ✅ {written} days — CTL {ctl:.0f}, ATL {atl:.0f}, TSB {ctl - atl:+.0f}")
        return written

    # -- reporting -------------------------------------------------------

    def print_summary(self) -> None:
        c = self.conn
        print("\n" + "=" * 60)
        print("DATABASE SUMMARY")
        print("=" * 60)

        total = c.execute("SELECT COUNT(*) FROM activities").fetchone()[0]
        detailed = c.execute(
            "SELECT COUNT(*) FROM activities WHERE detail_synced_at IS NOT NULL"
        ).fetchone()[0]
        samples = c.execute("SELECT COUNT(*) FROM activity_samples").fetchone()[0]
        races = c.execute("SELECT COUNT(*) FROM zp_results").fetchone()[0]

        print(f"Activities         : {total} ({detailed} with FIT detail)")
        print(f"Samples            : {samples:,}")
        print(f"ZwiftPower results : {races}")

        span = c.execute(
            "SELECT MIN(date(start_time,'unixepoch')) AS a, "
            "MAX(date(start_time,'unixepoch')) AS b FROM activities"
        ).fetchone()
        if span and span["a"]:
            print(f"Date range         : {span['a']} → {span['b']}")

        totals = c.execute(
            "SELECT ROUND(SUM(distance)/1000.0,1) km, ROUND(SUM(duration_s)/3600.0,1) h, "
            "ROUND(SUM(elevation_gain),0) m, ROUND(SUM(tss),0) tss FROM activities"
        ).fetchone()
        if totals and totals["km"]:
            print(f"Totals             : {totals['km']} km, {totals['h']} h, "
                  f"{totals['m']} m climbed, {totals['tss'] or 0} TSS")

        today = c.execute(
            "SELECT * FROM daily_metrics ORDER BY calendar_date DESC LIMIT 1"
        ).fetchone()
        if today:
            print(f"Form ({today['calendar_date']}) : CTL {today['ctl']}, "
                  f"ATL {today['atl']}, TSB {today['tsb']:+}")

        curve = c.execute(
            "SELECT duration_s, best_watts FROM power_curve_best "
            "WHERE duration_s IN (5, 60, 300, 1200) ORDER BY duration_s"
        ).fetchall()
        if curve:
            print("Best power         : " + ", ".join(
                f"{r['duration_s']}s {r['best_watts']:.0f}W" for r in curve))

        print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Zwift data downloader")
    parser.add_argument("--db", default=os.getenv("ZWIFT_DB_PATH", DEFAULT_DB),
                        help="SQLite database path")
    parser.add_argument("--days", type=int, help="Sync the last N days")
    parser.add_argument("--since", help="Sync from this date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true",
                        help="Re-fetch every activity, ignoring the watermark")
    parser.add_argument("--with-samples", action="store_true",
                        help="Store the per-second stream from each FIT (large)")
    parser.add_argument("--backfill-detail", action="store_true",
                        help="Only download FIT detail for activities missing it")
    parser.add_argument("--detail-limit", type=int,
                        help="Cap how many FITs to process in this run")
    parser.add_argument("--refresh-fits", action="store_true",
                        help="Re-download cached FIT files")
    parser.add_argument("--redo-detail", action="store_true",
                        help="Re-parse FITs already parsed (after a parser change)")
    parser.add_argument("--skip-fit", action="store_true",
                        help="Skip the FIT pass; no laps, streams or power curve")
    parser.add_argument("--skip-zp", action="store_true",
                        help="Skip ZwiftPower entirely")
    parser.add_argument("--zp-only", action="store_true",
                        help="Only sync ZwiftPower")
    parser.add_argument("--with-zp-fields", action="store_true",
                        help="Also store the full finishing field of each race")
    parser.add_argument("--zp-field-limit", type=int, default=25,
                        help="How many recent races to fetch fields for (default 25)")
    parser.add_argument("--summary", action="store_true",
                        help="Print the database summary and exit")
    args = parser.parse_args()

    try:
        downloader = ZwiftDownloader(args.db)
    except (ZwiftError, RuntimeError) as e:
        print(f"❌  {e}")
        sys.exit(1)

    if args.summary:
        downloader.print_summary()
        return

    try:
        if not args.zp_only:
            downloader.download_profile()

            if args.backfill_detail:
                downloader.enrich_activity_details(args.detail_limit)
                downloader.backfill_detail(args.detail_limit, args.with_samples,
                                           args.refresh_fits, args.redo_detail)
            else:
                downloader.download_activities(args.days, args.since, args.full)
                # Always run the cheap detail pass: without it there is no
                # heart rate, and no per-ride FTP to scale training load by.
                downloader.enrich_activity_details(args.detail_limit,
                                                   refresh=args.full)
                if not args.skip_fit:
                    downloader.backfill_detail(args.detail_limit, args.with_samples,
                                               args.refresh_fits, args.redo_detail)

        if not args.skip_zp:
            downloader.download_zwiftpower(
                with_fields=args.with_zp_fields,
                max_events=args.zp_field_limit,
            )
            downloader.link_zp_results()

        downloader.recompute_training_load()
        downloader.print_summary()

    except (ZwiftError, ZwiftPowerError) as e:
        print(f"\n❌  {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⏹  Interrupted — partial data is committed.")
        sys.exit(130)


if __name__ == "__main__":
    main()
