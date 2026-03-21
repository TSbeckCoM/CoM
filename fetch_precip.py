
#!/usr/bin/env python3
"""
Synoptic precip fetcher (pmode=last), token sourced from environment (GitHub Actions secret).

- Stations: TT917, TT918, AR247, 023HI
- Intervals: 1, 2, 3, 6, 24, 48 (hours)
- Output: data/latestPrecip.json
- Token: expected in env var SYNOPTIC_TOKEN (provided by GitHub Actions secret)

Environment variables:
  SYNOPTIC_TOKEN  - required (provided via GitHub Actions secret)
Optional overrides:
  STATIONS        - comma-separated STIDs (default: TT917,TT918,AR247,023HI)
  OUTPUT_JSON     - output path (default: data/latestPrecip.json)

Dependencies:
  pip install requests python-dateutil
"""

import os
import json
import requests
from datetime import datetime, timezone
from dateutil import tz

MM_TO_INCH = 1 / 25.4
DEFAULT_STATIONS = ["TT917", "TT918", "AR247", "023HI"]
DEFAULT_INTERVALS = (1, 2, 3, 6, 24, 48)
HAWAII_TZ = tz.gettz("Pacific/Honolulu")


def iso_to_dt(iso_str):
    """Safe ISO8601 → datetime with UTC tzinfo."""
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def fetch_precip_last(stations, token, intervals, end=None, timeout=45):
    """
    Call Synoptic /stations/precip with pmode=last and accum_hours list.
    Treat presence of STATION payload as success (some responses lack summary.responseCode).
    """
    base = "https://api.synopticdata.com/v2/stations/precip"
    params = {
        "stid": ",".join(stations),
        "token": token,  # <-- token comes from env (GitHub secret)
        "pmode": "last",
        "accum_hours": ",".join(map(str, intervals)),
        "units": "metric",     # totals in mm
        "timeformat": "iso",   # standardize any date strings returned
    }
    if end:
        params["end"] = end  # defaults to "now" when omitted

    try:
        r = requests.get(base, params=params, headers={"Accept": "application/json"}, timeout=timeout)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error contacting Synoptic precip: {e}") from e

    # Try parse JSON; otherwise provide a diagnostic preview
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
    """
    Parse OBSERVATIONS.precipitation array and convert mm totals to inches.
    Returns dict {hours:int -> inches:float (3dp)}.
    """
    obs = station_entry.get("OBSERVATIONS", {}) or {}
    precip_list = obs.get("precipitation") or []
    out = {}
    for item in precip_list:
        hours = item.get("accum_hours")
        total_mm = item.get("total")
        if hours is None or total_mm is None:
            continue
        try:
            hours = int(hours)
            total_mm = float(total_mm)
        except Exception:
            continue
        out[hours] = round(total_mm * MM_TO_INCH, 3)
    return out


def main():
    # Token comes from environment (via GitHub Actions secret)
    token = os.getenv("SYNOPTIC_TOKEN")
    if not token:
        # Do NOT print token contents; just fail clearly.
        raise SystemExit("SYNOPTIC_TOKEN env var not set. Pass via GitHub Actions secrets.")

    # Allow station & output overrides via env
    stations_env = os.getenv("STATIONS")
    stations = stations_env.split(",") if stations_env else DEFAULT_STATIONS
    output_path = os.getenv("OUTPUT_JSON", "data/latest.json")
    intervals = DEFAULT_INTERVALS

    # Fetch data from Synoptic
    data = fetch_precip_last(stations, token, intervals, end=None, timeout=45)

    # Optional API timing metadata (may be absent on this endpoint)
    summary = data.get("summary") or {}
    api_end_dt_utc = iso_to_dt(summary.get("end"))
    api_end_local = api_end_dt_utc.astimezone(HAWAII_TZ).isoformat() if api_end_dt_utc else None

    # Build rows
    rows = []
    for st in data.get("STATION", []):
        intervals_in = parse_intervals_in(st)

        # Best-effort last obs time (may not be present)
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
            "precip_in": intervals_in,  # {1: x, 2: y, 3: z, 6: a, 24: b, 48: c}
            "pmode": "last",
            "api_end_utc": api_end_dt_utc.isoformat() if api_end_dt_utc else None,
            "api_end_local": api_end_local if api_end_local else None,
            "last_obs_utc": last_obs_dt_utc.isoformat() if last_obs_dt_utc else None,
            "last_obs_local": last_obs_local,
        })

    rows = sorted(rows, key=lambda r: (r["stid"] or ""))

    # Write JSON
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "generated_local": datetime.now(timezone.utc).astimezone(HAWAII_TZ).isoformat(),
        "intervals_hours": list(intervals),
        "stations": rows,
    }
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {output_path} with {len(rows)} stations (pmode=last, intervals={intervals}).")


if __name__ == "__main__":
    main()
