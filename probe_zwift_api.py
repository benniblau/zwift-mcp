#!/usr/bin/env python3
"""
Probe the Zwift and ZwiftPower APIs and record what they actually return.

Neither API is documented, and both change without notice. This script is the
ground truth the schema and the downloader's field mapping are written
against: it authenticates, calls each endpoint the downloader depends on, and
writes the raw payloads to probe_zwift.json.

Run it before trusting a field name, and again whenever a sync starts
returning NULLs where it used to return numbers.

    python probe_zwift_api.py                 # everything
    python probe_zwift_api.py --skip-zp       # Zwift only
    python probe_zwift_api.py --activity ID   # also dump one specific activity

Nothing is written to either account: every call here is a read.
"""

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict

from zwift_downloader import (
    PAGE_SIZE,
    ZwiftClient,
    ZwiftError,
    ZwiftPowerClient,
    ZwiftPowerError,
)

OUT_PATH = Path(__file__).with_name("probe_zwift.json")

# Fields that would put a live credential in a file on disk.
REDACT = {"accessToken", "access_token", "refresh_token", "password",
          "privateAttributes", "emailAddress", "email"}


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: ("<redacted>" if k in REDACT else redact(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    return value


def keys_of(payload: Any) -> Any:
    """A compact view of a payload's shape, for eyeballing field names."""
    if isinstance(payload, dict):
        return sorted(payload.keys())
    if isinstance(payload, list) and payload:
        return {"list_of": len(payload), "first_keys": keys_of(payload[0])}
    return type(payload).__name__


def probe_zwift(out: Dict[str, Any], activity_id: str = None) -> None:
    print("\n=== Zwift game API ===")
    client = ZwiftClient()
    client.authenticate()
    print(f"  token expires in {int(client.expires_at - time.time())}s")

    profile = client.get_profile("me")
    out["profile"] = {"keys": keys_of(profile), "payload": redact(profile)}
    print(f"  profile: id={profile.get('id')} ftp={profile.get('ftp')} "
          f"weight={profile.get('weight')} fields={len(profile)}")

    activities = client.get_activities(limit=PAGE_SIZE)
    out["activity_list"] = {
        "count": len(activities),
        "keys": keys_of(activities),
        "first": redact(activities[0]) if activities else None,
    }
    print(f"  activity list: {len(activities)} returned for limit={PAGE_SIZE}")

    # Paging behaviour: does `start` actually offset, or is it ignored?
    if len(activities) >= 2:
        page2 = client.get_activities(start=1, limit=2)
        first_ids = [str(a.get("id")) for a in activities[:3]]
        out["paging"] = {
            "page1_ids": first_ids,
            "start1_ids": [str(a.get("id")) for a in page2],
            "offsets_correctly": bool(page2) and str(page2[0].get("id")) == first_ids[1],
        }
        print(f"  paging with start= works: {out['paging']['offsets_correctly']}")

    target = activity_id or (str(activities[0].get("id")) if activities else None)
    if target:
        detail = client.get_activity(target)
        out["activity_detail"] = {
            "activity_id": target,
            "keys": keys_of(detail),
            "payload": redact(detail),
        }
        print(f"  activity {target}: {len(detail)} fields, "
              f"fit={bool(detail.get('fitFileKey'))}")

        # The FIT is the only source of laps and streams, so confirm it is
        # actually reachable rather than merely referenced.
        if detail.get("fitFileBucket") and detail.get("fitFileKey"):
            try:
                payload = client.download_fit(detail["fitFileBucket"],
                                              detail["fitFileKey"])
                out["fit"] = {"bytes": len(payload),
                              "header_ok": payload[8:12] == b".FIT"}
                print(f"  FIT download: {len(payload):,} bytes, "
                      f"valid header: {out['fit']['header_ok']}")
            except ZwiftError as e:
                out["fit"] = {"error": str(e)}
                print(f"  ⚠️  FIT download failed: {e}")


def probe_zwiftpower(out: Dict[str, Any], zwift_id: str) -> None:
    print("\n=== ZwiftPower ===")
    zp = ZwiftPowerClient()
    zp.authenticate()

    results = zp.profile_results(zwift_id)
    out["zp_results"] = {
        "count": len(results),
        "keys": keys_of(results),
        "first": results[0] if results else None,
    }
    print(f"  profile results: {len(results)} races")
    if results:
        print(f"  result keys: {', '.join(sorted(results[0])[:20])}…")

    curve = zp.critical_power(zwift_id)
    efforts = curve.get("efforts") if isinstance(curve, dict) else None
    out["zp_critical_power"] = {
        "top_level_keys": keys_of(curve),
        "effort_durations": sorted(efforts.keys()) if isinstance(efforts, dict) else None,
        "sample": (next(iter(efforts.values())) if isinstance(efforts, dict) and efforts
                   else None),
    }
    print(f"  critical power: {out['zp_critical_power']['top_level_keys']}")

    if results:
        event_id = str(results[0].get("zid") or "")
        if event_id:
            field = zp.event_results(event_id)
            out["zp_event_results"] = {
                "event_id": event_id,
                "count": len(field),
                "keys": keys_of(field),
                "first": field[0] if field else None,
            }
            print(f"  event {event_id}: {len(field)} finishers")

    page = zp.profile_page(zwift_id)
    out["zp_profile_page"] = {"bytes": len(page),
                              "has_zftp": "zFTP" in page,
                              "has_category": "label-cat" in page}
    print(f"  profile page: {len(page):,} bytes, "
          f"zFTP present: {out['zp_profile_page']['has_zftp']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe the Zwift APIs")
    parser.add_argument("--activity", help="Also dump this specific activity id")
    parser.add_argument("--skip-zp", action="store_true", help="Skip ZwiftPower")
    parser.add_argument("--out", default=str(OUT_PATH), help="Where to write the dump")
    args = parser.parse_args()

    out: Dict[str, Any] = {}
    try:
        probe_zwift(out, args.activity)
    except ZwiftError as e:
        print(f"❌  Zwift: {e}")
        out["zwift_error"] = str(e)

    if not args.skip_zp:
        zwift_id = (out.get("profile", {}).get("payload") or {}).get("id")
        if zwift_id:
            try:
                probe_zwiftpower(out, str(zwift_id))
            except ZwiftPowerError as e:
                print(f"❌  ZwiftPower: {e}")
                out["zwiftpower_error"] = str(e)
        else:
            print("\n⚠️  Skipping ZwiftPower: no athlete id from the Zwift profile")

    Path(args.out).write_text(json.dumps(out, indent=2, default=str))
    print(f"\n📝  Wrote {args.out}")
    print("    This file contains personal training data — it is gitignored.")

    if out.get("zwift_error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
