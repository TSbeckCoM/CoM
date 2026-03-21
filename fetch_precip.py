#!/usr/bin/env python3
"""
Synoptic precip fetcher (raw timeseries → custom bins), token sourced from environment.

Matches prior JSON schema but adds:
- "3_hr_trend": numeric inches or "no data" (last 3h minus previous 3h)

Differences vs prior pmode=last version:
- Endpoint: /stations/timeseries (raw)
- Time window: end=now (UTC), start=now-48h
- Bins: 1, 2, 3, 6, 24, 48 hours

Dependencies:
  pip install requests python-dateutil
"""

import os
import sys
import json
import requests
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone, timedelta
from dateutil import tz

# --- Configuration -------------------------------------------------------------

MM_TO_INCH = 1 / 25.4
DEFAULT_STATIONS: List[str] = [
    "TT917","PHMK","TT919","031HI","G4646","TT912","TT933","AR427",
    "023HI","PHOG","TT910","042HI","TT925","015HE","017HI","F4397"
]

# Match sample intervals (includes 6h)
DEFAULT_INTERVALS = (1, 2, 3, 6, 24, 48)
DEFAULT_OUTPUT = "data/latestPrecip.json"
HAWAII_TZ = tz.gettz("Pacific/Honolulu")

# Hard-coded station metadata (unchanged)
STATION_META: Dict[str, Dict[str, Optional[str]]] = {
    "TT917": {"display_name": "West Molokai", "island": "Molokai"},
    "PHMK": {"display_name": "Molokai Airport", "island": "Molokai"},
    "TT919": {"display_name": "Kaunakakai", "island": "Molokai"},
    "031HI": {"display_name": "East Molokai", "island": "Molokai"},
    "G4646": {"display_name": "Kihei", "island": "Maui"},
    "TT912": {"display_name": "Lahainaluna", "island": "Maui"},
    "TT933": {"display_name": "Kahana", "island": "Maui"},
    "AR427": {"display_name": "Wailuku Heights", "island": "Maui"},
    "023HI": {"display_name": "East Maui", "island": "Maui"},
    "PHOG": {"display_name": "Kahului Airport", "island": "Maui"},
    "TT910": {"display_name": "Kahakuloa", "island": "Maui"},
    "042HI": {"display_name": "Kula", "island": "Maui"},
    "TT925": {"display_name": "Olinda", "island": "Maui"},
    "015HE": {"display_name": "Pukalani", "island": "Maui"},
    "017HI": {"display_name": "Piiholo", "island": "Maui"},
    "F4397": {"display_name": "Haiku", "island": "Maui"}
}

# --- Helpers ------------------------------------------------------------------

def iso_to_dt(iso_str: Optional[str]) -> Optional[datetime]:
    """Safe ISO8601 → timezone-aware UTC datetime."""
    if not iso_str:
        return None
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def fetch_precip_timeseries(
    stations: List[str],
    token: str,
    start_iso: str,
    end_iso: str,
    timeout: int = 45
) -> Dict[str, Any]:
    """
    Call Synoptic /stations/timeseries for raw precip variables over [start, end].
    Request both `precip` (incremental) and `precip_accum` (cumulative).
    """
    base = "https://api.synopticdata.com/v2/stations/timeseries"
    params = {
        "stid": ",".join(stations),
        "token": token,
        "start": start_iso,
        "end": end_iso,
        "vars": "precip,precip_accum",
        "units": "metric",   # mm
        "timeformat": "iso",
    }

    try:
        r = requests.get(base, params=params, headers={"Accept": "application/json"}, timeout=timeout)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error contacting Synoptic timeseries: {e}") from e

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
        raise RuntimeError(f"Synoptic timeseries returned no STATION payload.\nPreview:\n{preview}")

    return data


def _coerce_float(x: Any) -> Optional[float]:
    try:
        if x is None or (isinstance(x, str) and x.strip() == ""):
            return None
        return float(x)
    except Exception:
        return None


def parse_precip_increments(station_entry: Dict[str, Any]) -> List[Tuple[datetime, float]]:
    """
    From a station's OBSERVATIONS, produce [(UTC timestamp, precip increment inches)].
    Prefer 'precip' (incremental). If missing, difference 'precip_accum' (ignore resets).
    """
    obs = station_entry.get("OBSERVATIONS") or {}
    dt_strings = obs.get("date_time") or []
    times_utc: List[datetime] = []
    for s in dt_strings:
        d = iso_to_dt(s)
        if d is not None:
            times_utc.append(d)

    precip_arr = obs.get("precip") or []
    accum_arr = obs.get("precip_accum") or []
    increments_in_inches: List[Tuple[datetime, float]] = []

    if precip_arr and len(precip_arr) == len(times_utc):
        for t, v in zip(times_utc, precip_arr):
            mm = _coerce_float(v)
            if mm is None or mm < 0:
                continue
            increments_in_inches.append((t, round(mm * MM_TO_INCH, 5)))
        return increments_in_inches

    if accum_arr and len(accum_arr) == len(times_utc):
        prev_mm: Optional[float] = None
        for t, v in zip(times_utc, accum_arr):
            mm = _coerce_float(v)
            if mm is None:
                prev_mm = None
                continue
            if prev_mm is None:
                prev_mm = mm
                continue
            delta = mm - prev_mm
            prev_mm = mm
            if delta <= 0:
                continue
            increments_in_inches.append((t, round(delta * MM_TO_INCH, 5)))
        return increments_in_inches

    return []


def bin_precip(
    increments: List[Tuple[datetime, float]],
    end_utc: datetime,
    intervals_hours: List[int]
) -> Dict[int, Any]:
    """
    Sum increments strictly within (end - H, end] for each H in intervals_hours.
    If no observations exist in the window, value is "no data"; otherwise sum (which may be 0.0).
    """
    out: Dict[int, Any] = {}
    for H in intervals_hours:
        window_start = end_utc - timedelta(hours=H)
        vals = [inc for (t, inc) in increments if (t > window_start and t <= end_utc)]
        if len(vals) == 0:
            out[H] = "no data"
        else:
            out[H] = round(sum(vals), 3)
    return out


def compute_3hr_trend(
    increments: List[Tuple[datetime, float]],
    end_utc: datetime
) -> Any:
    """
    Trend (inches) = sum(last 3h) - sum(previous 3h).
    Returns numeric inches or "no data" if either window has no observations.
    """
    last_start = end_utc - timedelta(hours=3)
    prev_start = end_utc - timedelta(hours=6)
    prev_end = end_utc - timedelta(hours=3)

    last_vals = [inc for (t, inc) in increments if (t > last_start and t <= end_utc)]
    prev_vals = [inc for (t, inc) in increments if (t > prev_start and t <= prev_end)]

    if not last_vals or not prev_vals:
        return "no data"

    last_total = round(sum(last_vals), 3)
    prev_total = round(sum(prev_vals), 3)
    return round(last_total - prev_total, 3)


# --- Main transform ------------------------------------------------------------

def build_payload_timeseries_match_schema(
    data: Dict[str, Any],
    intervals: List[int],
    api_end_utc: datetime
) -> Dict[str, Any]:
    api_end_local = api_end_utc.astimezone(HAWAII_TZ).isoformat()

    rows = []
    for st in data.get("STATION", []):
        stid = st.get("STID")
        api_name = st.get("NAME")
        lat = st.get("LATITUDE")
        lon = st.get("LONGITUDE")
        elev = st.get("ELEVATION")
        meta = STATION_META.get(stid or "", {})
        display_name = meta.get("display_name") or api_name or stid
        island = meta.get("island")

        increments = parse_precip_increments(st)
        obs = st.get("OBSERVATIONS") or {}
        times = obs.get("date_time") or []
        last_obs_dt_utc = iso_to_dt(times[-1]) if isinstance(times, list) and times else None
        last_obs_local = last_obs_dt_utc.astimezone(HAWAII_TZ).isoformat() if last_obs_dt_utc else None

        precip_bins = bin_precip(increments, api_end_utc, intervals)
        trend_3h = compute_3hr_trend(increments, api_end_utc)

        # Match your sample types: lat/lon/elevation as strings
        row = {
            "stid": stid,
            "name": api_name,
            "display_name": display_name,
            "island": island,
            "lat": None if lat is None else str(lat),
            "lon": None if lon is None else str(lon),
            "elevation_m": None if elev is None else str(elev),
            "precip_in": precip_bins,            # {1: x, 2: y, 3: z, 6: a, 24: b, 48: c or "no data"}
            "pmode": "raw",                      # metadata indicating raw/timeseries
            "api_end_utc": api_end_utc.isoformat(),
            "api_end_local": api_end_local,
            "last_obs_utc": last_obs_dt_utc.isoformat() if last_obs_dt_utc else None,
            "last_obs_local": last_obs_local,
            "3_hr_trend": trend_3h               # NEW FIELD (inches or "no data")
        }

        rows.append(row)

    rows = sorted(rows, key=lambda r: (r.get("stid") or ""))

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "generated_local": datetime.now(timezone.utc).astimezone(HAWAII_TZ).isoformat(),
        "intervals_hours": list(intervals),
        "stations": rows,
    }
    return payload


def write_json_atomic(payload: Dict[str, Any], output_path: str) -> None:
    """Write JSON atomically: temp file then replace target. Create dir only if it exists."""
    dirpath = os.path.dirname(output_path)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    tmp = output_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, output_path)


# --- Entrypoint ----------------------------------------------------------------

def main() -> None:
    token = os.getenv("SYNOPTIC_TOKEN")
    if not token:
        print("ERROR: SYNOPTIC_TOKEN env var not set. Pass via GitHub Actions secrets.", file=sys.stderr)
        sys.exit(2)

    raw_output = os.getenv("OUTPUT_JSON")
    output_path = (raw_output if raw_output is not None else DEFAULT_OUTPUT).strip()
    if not output_path:
        output_path = DEFAULT_OUTPUT

