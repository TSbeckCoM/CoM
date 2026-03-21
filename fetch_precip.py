#!/usr/bin/env python3
"""
Synoptic precip fetcher (pmode=last), token sourced from environment (GitHub Actions secret).

- Stations: TT917, TT918, AR247, 023HI
- Intervals: 1, 2, 3, 6, 24, 48 (hours)
- Output: data/latestPrecip.json (override via OUTPUT_JSON)
- Token: expected in env var SYNOPTIC_TOKEN

Dependencies:
  pip install requests python-dateutil
"""

import os
import sys
import json
import requests
from datetime import datetime, timezone
from dateutil import tz

MM_TO_INCH = 1 / 25.4
DEFAULT_STATIONS = ["TT917", "TT918", "AR247", "023HI"]
DEFAULT_INTERVALS = (1, 2, 3, 6, 24, 48)
DEFAULT_OUTPUT = "data/latestPrecip.json"
HAWAII_TZ = tz.gettz("Pacific/Honolulu")


def iso_to_dt(iso_str):
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_precip_last(stations, token, intervals, end=None, timeout=45):
    base = "https://api.synopticdata.com/v2/stations/precip"
    params = {
        "stid": ",".join(stations),
        "token": token,
        "pmode": "last",
        "accum_hours": ",".join(map(str, intervals)),
        "units": "metric",
        "timeformat": "iso",
    }
    if end:
        params["end"] = end

    try:
        r = requests.get(base, params=params, headers={"Accept": "application/json"}, timeout=timeout)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error contacting Synoptic precip: {e}") from e

    try:
        data = r.json()
    except ValueError:
        preview = (r.text or "")[:1200]
        raise RuntimeError(
            f"Non-JSON response (status={r.status_code}, content-type='{r.headers.get('Content-Type','')}').\n"
            f"Preview:\n{preview}"
        )

    if not data.get("STATION"):
        preview = json.dumps(data, indent=2)[:1200]
        raise RuntimeError(f"Synoptic precip returned no STATION payload.\nPreview:\n{preview}")

    return data


def parse_intervals_in(station_entry):
    obs = station_entry.get("OBSERVATIONS", {}) or {}
    precip_list = obs.get("precipitation") or []
    out = {}
    for item in precip_list:
        hours = item.get("accum_hours")
        total_mm = item.get("total")
        if hours is None or total_mm is None:
            continue
        try:
            hours_i = int(hours)
            total_mm_f = float(total_mm)
        except Exception:
            continue
        out[hours_i] = round(total_mm_f * MM_TO_INCH, 3)
    return out


def main():
    token = os.getenv("SYNOPTIC_TOKEN")
    if not token:
        print("ERROR: SYNOPTIC_TOKEN env var not set. Pass via GitHub Actions secrets.", file=sys.stderr)
        sys.exit(2)

    stations_env = os.getenv("STATIONS")
    stations = stations_env.split(",") if stations_env else DEFAULT_STATIONS

    intervals_env = os.getenv("INTERVALS")
    intervals = [int(x) for x in intervals_env.split(",")] if intervals_env else list(DEFAULT_INTERVALS)

    raw_output = os.getenv("OUTPUT_JSON")
    output_path = (raw_output if raw_output is not None else DEFAULT_OUTPUT).strip()
    if not output_path:
        output_path = DEFAULT_OUTPUT

    data = fetch_precip_last(stations, token, intervals, end=None, timeout=45)

    summary = data.get("summary") or {}
    api_end_dt_utc = iso_to_dt(summary.get("end"))
    api_end_local = api_end_dt_utc.astimezone(HAWAII_TZ).isoformat() if api_end_dt_utc else None

    rows = []
    for st in data.get("STATION", []):
        intervals_in = parse_intervals_in(st)

        obs = st.get("OBSERVATIONS", {}) or {}
        times = obs.get("date_time", [])
        last_obs_dt_utc = iso_to_dt(times[-1]) if isinstance(times, list) and times else None
        last_obs_local = last_obs_dt_utc.astimezone(HAWAII_TZ).isoformat() if last_obs_dt_utc else None

        rows.append({
            "stid": st.get("STID"),
            "name": st.get("NAME"),
            "lat": st.get("LATITUDE"),
            "lon": st.get("LONGITUDE"),
            "elevation_m": st.get("ELEVATION"),
            "precip_in": intervals_in,
            "pmode": "last",
            "api_end_utc": api_end_dt_utc.isoformat() if api_end_dt_utc else None,
            "api_end_local": api_end_local if api_end_local else None,
            "last_obs_utc": last_obs_dt_utc.isoformat() if last_obs_dt_utc else None,
            "last_obs_local": last_obs_local,
        })

    rows = sorted(rows, key=lambda r: (r["stid"] or ""))

    dirpath = os.path.dirname(output_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "generated_local": datetime.now(timezone.utc).astimezone(HAWAII_TZ).isoformat(),
        "intervals_hours": list(intervals),
        "stations": rows,
    }

    tmp = output_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, output_path)

    print(f"Wrote {output_path} with {len(rows)} stations (pmode=last, intervals={intervals}).")


if __name__ == "__main__":
    main()
