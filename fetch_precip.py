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

# --- Configuration (edit these two blocks) ------------------------------------

MM_TO_INCH = 1 / 25.4
DEFAULT_STATIONS: List[str] = [
    # Permanent list of STIDs you want in the API request
    "TT917",
    "PHMK",
    "TT919",
    "031HI",
    "G4646",
    "TT912",
    "TT933",
    "AR427",
    "023HI",
    "PHOG",
    "TT910",
    "042HI",
    "TT925",
    "015HE",
    "017HI",
    "F4397"
]
DEFAULT_INTERVALS = (1, 2, 3, 6, 24, 48)
DEFAULT_OUTPUT = "data/latestPrecip.json"
HAWAII_TZ = tz.gettz("Pacific/Honolulu")

# Hard-coded station metadata (provide one entry per STID above)
# Islands: "Hawaiʻi", "Maui", "Oʻahu", "Kauaʻi", "Molokaʻi", "Lānaʻi" (or your naming)
STATION_META: Dict[str, Dict[str, Optional[str]]] = {
    "TT917": {"display_name": "West Molokai",       "island": "Molokai"},
    "PHMK": {"display_name": "Molokai Airport",     "island": "Molokai"},
    "TT919": {"display_name": "Kaunakakai",         "island": "Molokai"},
    "031HI": {"display_name": "East Molokai",       "island": "Molokai"},
    "G4646": {"display_name": "Kihei",              "island": "Maui"},
    "TT912": {"display_name": "Lahainaluna",        "island": "Maui"},
    "TT933": {"display_name": "Kahana",             "island": "Maui"},
    "AR427": {"display_name": "Wailuku Heights",    "island": "Maui"},
    "023HI": {"display_name": "East Maui",          "island": "Maui"},
    "PHOG": {"display_name": "Kahului Airport",     "island": "Maui"},
    "TT910": {"display_name": "Kahakuloa",          "island": "Maui"},
    "042HI": {"display_name": "Kula",               "island": "Maui"},
    "TT925": {"display_name": "Olinda",             "island": "Maui"},
    "015HE": {"display_name": "Pukalani",           "island": "Maui"},
    "017HI": {"display_name": "Piiholo",            "island": "Maui"},
    "F4397": {"display_name": "Haiku",              "island": "Maui"}
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
