#!/usr/bin/env python3
"""
Synoptic precip fetcher (pmode=last), token sourced from environment (GitHub Actions secret).

- Stations: permanent list in DEFAULT_STATIONS
- Metadata: hard-coded display_name and island per STID in STATION_META
- Intervals: 1, 2, 3, 6, 24, 48 (hours)
- Output: data/latestPrecip.json (override via env OUTPUT_JSON)
- Token: expected in env var SYNOPTIC_TOKEN

Dependencies:
  pip install requests python-dateutil
"""

import os
import sys
import json
import requests
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from dateutil import tz

# --- Configuration -------------------------------------------------------------

MM_TO_INCH = 1 / 25.4
DEFAULT_STATIONS: List[str] = [
    "TT917","PHMK","TT919","031HI","G4646","TT912","TT933","AR427",
    "023HI","PHOG","TT910","042HI","TT925","015HE","017HI","F4397"
]
DEFAULT_INTERVALS = (1, 2, 3, 6, 24, 48)
DEFAULT_OUTPUT = "data/latestPrecip.json"
HAWAII_TZ = tz.gettz("Pacific/Honolulu")

STATION_META: Dict[str, Dict[str, Optional[str]]] = {
    "TT917": {"display_name": "West Molokai",    "island": "Molokai"},
    "PHMK":  {"display_name": "Molokai Airport", "island": "Molokai"},
    "TT919": {"display_name": "Kaunakakai",      "island": "Molokai"},
    "031HI": {"display_name": "East Molokai",    "island": "Molokai"},
    "G4646": {"display_name": "Kihei",           "island": "Maui"},
    "TT912": {"display_name": "Lahainaluna",     "island": "Maui"},
    "TT933": {"display_name": "Kahana",          "island": "Maui"},
    "AR427": {"display_name": "Wailuku Heights", "island": "Maui"},
    "023HI": {"display_name": "East Maui",       "island": "Maui"},
    "PHOG":  {"display_name": "Kahului Airport", "island": "Maui"},
    "TT910": {"display_name": "Kahakuloa",       "island": "Maui"},
    "042HI": {"display_name": "Kula",            "island": "Maui"},
    "TT925": {"display_name": "Olinda",          "island": "Maui"},
    "015HE": {"display_name": "Pukalani",        "island": "Maui"},
    "017HI": {"display_name": "Piiholo",         "island": "Maui"},
    "F4397": {"display_name": "Haiku",           "island": "Maui"},
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


def fetch_precip_last(
    stations: List[str],
    token: str,
    intervals: List[int],
    end: Optional[str] = None,
    timeout: int = 45
) -> Dict[str, Any]:
    """Call Synoptic /stations/precip with pmode=last and accum_hours list."""
    base = "https://api.synopticdata.com/v2/stations/precip"
    params = {
        "stid": ",".join(stations),
        "token": token,
        "pmode": "last",
        "accum_hours": ",".join(map(str, intervals)),
        "units": "metric",   # totals in mm; we convert to inches
        "timeformat": "iso",
    }
    if end:
        params["end"] = end  # defaults to "now" when omitted

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


def parse_intervals_in(station_entry: Dict[str, Any]) -> Dict[int, float]:
    """
    Parse OBSERVATIONS.precipitation array and convert mm totals to inches.
    Returns dict {hours:int -> inches:float (3dp)} for the hours present.
    """
    obs = station_entry.get("OBSERVATIONS") or {}
    precip_list = obs.get("precipitation") or []
    out: Dict[int, float] = {}
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


def normalize_intervals(
    intervals_in: Dict[int, float],
    intervals: Tuple[int, ...] = DEFAULT_INTERVALS
) -> Tuple[Dict[str, float], List[float]]:
    """
    Ensure all intervals are present. Return both:
    - normalized_map: dict with string keys in JSON ("1", "2", ...)
    - normalized_arr: list aligned to DEFAULT_INTERVALS
    Missing intervals are filled with 0.0.
    """
    normalized_arr: List[float] = [float(intervals_in.get(h, 0.0)) for h in intervals]
    normalized_map: Dict[str, float] = {str(h): normalized_arr[i] for i, h in enumerate(intervals)}
    return normalized_map, normalized_arr


# --- Main transform ------------------------------------------------------------

def build_payload(data: Dict[str, Any], intervals: Tuple[int, ...]) -> Dict[str, Any]:
    summary = data.get("summary") or {}
    api_end_dt_utc = iso_to_dt(summary.get("end"))
    api_end_local = api_end_dt_utc.astimezone(HAWAII_TZ).isoformat() if api_end_dt_utc else None

    rows = []
    for st in data.get("STATION", []):
        intervals_in = parse_intervals_in(st)
        normalized_map, normalized_arr = normalize_intervals(intervals_in, intervals)

        obs = st.get("OBSERVATIONS") or {}
        times = obs.get("date_time", [])
        last_obs_dt_utc = iso_to_dt(times[-1]) if isinstance(times, list) and times else None
        last_obs_local = last_obs_dt_utc.astimezone(HAWAII_TZ).isoformat() if last_obs_dt_utc else None

        stid = st.get("STID")
        api_name = st.get("NAME")
        lat = st.get("LATITUDE")
        lon = st.get("LONGITUDE")
        elev = st.get("ELEVATION")

        meta = STATION_META.get(stid or "", {})
        display_name = meta.get("display_name") or api_name or stid
        island = meta.get("island")

        rows.append({
            "stid": stid,
            "name": api_name,
            "display_name": display_name,
            "island": island,
            "lat": lat,
            "lon": lon,
            "elevation_m": elev,
            "precip_in": normalized_map,       # mapping with string hour keys
            "precip_in_arr": normalized_arr,   # array aligned to DEFAULT_INTERVALS
            "pmode": "last",
            "api_end_utc": api_end_dt_utc.isoformat() if api_end_dt_utc else None,
            "api_end_local": api_end_local if api_end_local else None,
            "last_obs_utc": last_obs_dt_utc.isoformat() if last_obs_dt_utc else None,
            "last_obs_local": last_obs_local,
        })

    rows = sorted(rows, key=lambda r: (r.get("stid") or ""))

    payload = {
        "schema": {"version": "precip_v2", "has_precip_in_arr": True},
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "generated_local": datetime.now(timezone.utc).astimezone(HAWAII_TZ).isoformat(),
        "intervals_hours": list(intervals),
        "stations": rows,
    }
    return payload


def write_json_atomic(payload: Dict[str, Any], output_path: str) -> None:
    """Write JSON atomically: temp file then replace target. Create dir if needed."""
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

    missing_meta = [s for s in DEFAULT_STATIONS if s not in STATION_META]
    if missing_meta:
        print(f"WARNING: Missing STATION_META entries for: {', '.join(missing_meta)}", file=sys.stderr)

    print(f"[info] Requesting {len(DEFAULT_STATIONS)} STIDs: {', '.join(DEFAULT_STATIONS)}")

    data = fetch_precip_last(DEFAULT_STATIONS, token, list(DEFAULT_INTERVALS), end=None, timeout=45)

    returned_stids = [st.get("STID") for st in (data.get("STATION") or [])]
    missing = [s for s in DEFAULT_STATIONS if s not in returned_stids]
    print(f"[info] API returned {len(returned_stids)} stations: {', '.join(returned_stids)}")
    if missing:
        print(f"[warn] Missing in API response: {', '.join(missing)}")
        print("[hint] Check STID spelling/casing, token access, and whether the station has recent precip for pmode=last.")

    payload = build_payload(data, DEFAULT_INTERVALS)
    write_json_atomic(payload, output_path)

    print(f"Wrote {output_path} with {len(payload['stations'])} stations "
          f"(pmode=last, intervals={list(DEFAULT_INTERVALS)}).")


if __name__ == "__main__":
    main()
