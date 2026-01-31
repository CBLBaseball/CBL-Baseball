#!/usr/bin/env python3
"""Update CBL Free Agent Pool leaderboards using FanGraphs' leaders JSON endpoint.

Key fix:
- Use the SAME parameter set the FanGraphs major-league leaders page uses,
  especially `type=8` for the standard leaderboard view.

Endpoint:
  https://www.fangraphs.com/api/leaders/major-league/data
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

OUT_DIR = Path("data/fa")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SEASON = 2025
API = "https://www.fangraphs.com/api/leaders/major-league/data"

SEGMENTS = {
  "hit_am": [
    26546,
    27915,
    19455,
    23802,
    16925,
    15654,
    30028,
    21496,
    16398,
    33280,
    29646,
    35108,
    30038,
    21587,
    31912,
    21547,
    25629,
    21535,
    19458,
    25183,
    25705,
    23395,
    23695,
    31363,
    24878,
    23372,
    25477,
    29830,
    31661,
    26202,
    31370,
    28253,
    27690,
    23968,
    27501,
    26374,
    26244,
    29844,
    27963,
    24605
  ],
  "hit_nz": [
    10655,
    18054,
    27789,
    22766,
    19960,
    29571,
    19877,
    30063,
    26148,
    26143,
    10200,
    29949,
    31396,
    25999,
    31583,
    19562
  ],
  "sp": [
    31764,
    13050,
    26056,
    20370,
    31815,
    19736,
    30113,
    31312,
    26171,
    27932,
    31623,
    23301,
    19666,
    26482,
    19222,
    14120,
    31475,
    23735,
    17732,
    15094,
    26440,
    13580,
    16358,
    20629
  ],
  "rp_am": [
    31764,
    25327,
    29633,
    26203,
    13050,
    26136,
    21345,
    33568,
    27695,
    19804,
    31815,
    19736,
    13607,
    30161,
    9174,
    30113,
    30206,
    27662,
    15256,
    27481,
    22113,
    31312,
    25873,
    33248,
    27974,
    26260,
    22176,
    27626,
    19586,
    23324,
    27583,
    21863,
    19835,
    20546,
    26259,
    27932,
    21212,
    18674,
    23301,
    19666,
    26482,
    16631,
    19205,
    30016,
    24591,
    16128,
    24590,
    13190,
    27984,
    25957,
    27271,
    21924,
    15514,
    29615,
    20515,
    19222,
    14120,
    31884,
    23811,
    25839,
    24710,
    26353,
    22288,
    29770,
    20827
  ],
  "rp_nz": [
    17732,
    19281,
    29564,
    33821,
    15094,
    24094,
    26440,
    13580,
    26344,
    26285,
    20629,
    20379
  ]
}

def call_api(params: Dict[str, Any], tries: int = 6) -> Dict[str, Any]:
    delay = 2.0
    last_err: Optional[Exception] = None
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(API, params=params, timeout=60, headers={
                "User-Agent": "Mozilla/5.0 (CBL dashboard bot)",
                "Accept": "application/json,text/plain,*/*",
                "Referer": "https://www.fangraphs.com/leaders/major-league",
            })
            if r.status_code in (429, 403, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt == tries:
                raise
            time.sleep(delay)
            delay = min(delay * 1.8, 20.0)
    raise last_err or RuntimeError("Unknown error")

def leaders_params(players: List[int], stats: str, month: int) -> Dict[str, Any]:
    return {
        "ind": "0",
        "lg": "all",
        "pos": "all",
        "qual": "0",
        "season": str(SEASON),
        "season1": str(SEASON),
        "stats": stats,        # bat | pit
        "month": str(month),   # 0 all; 13 vs L; 14 vs R (leaders UI)
        "players": ",".join(map(str, players)),
        "team": "0,ts",
        "rost": "0",
        "type": "8",
        "sortcol": "17",
        "sortdir": "default",
        "pageitems": "2000",
        "pagenum": "1",
        "filter": "",
    }

def normalize(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = payload.get("data")
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    for key in ("rows", "result", "results"):
        v = payload.get(key)
        if isinstance(v, list):
            return [r for r in v if isinstance(r, dict)]
    return []

def save_json(name: str, rows: List[Dict[str, Any]]):
    (OUT_DIR / f"{name}.json").write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")

def fetch_one(out_name: str, seg_key: str, stats: str, month: int):
    players = SEGMENTS[seg_key]
    params = leaders_params(players, stats, month)
    payload = call_api(params)
    rows = normalize(payload)
    save_json(out_name, rows)
    print(f"Saved {out_name}: {len(rows)} rows")

def main():
    tasks = [
        ("hit_am_bat_all", "hit_am", "bat", 0),
        ("hit_am_bat_lhp", "hit_am", "bat", 13),
        ("hit_am_bat_rhp", "hit_am", "bat", 14),
        ("hit_nz_bat_all", "hit_nz", "bat", 0),
        ("hit_nz_bat_lhp", "hit_nz", "bat", 13),
        ("hit_nz_bat_rhp", "hit_nz", "bat", 14),
        ("sp_pit_all", "sp", "pit", 0),
        ("sp_pit_lhb", "sp", "pit", 13),
        ("sp_pit_rhb", "sp", "pit", 14),
        ("rp_am_pit_all", "rp_am", "pit", 0),
        ("rp_am_pit_lhb", "rp_am", "pit", 13),
        ("rp_am_pit_rhb", "rp_am", "pit", 14),
        ("rp_nz_pit_all", "rp_nz", "pit", 0),
        ("rp_nz_pit_lhb", "rp_nz", "pit", 13),
        ("rp_nz_pit_rhb", "rp_nz", "pit", 14),
    ]
    for out_name, seg_key, stats, month in tasks:
        fetch_one(out_name, seg_key, stats, month)
        time.sleep(1.2)

if __name__ == "__main__":
    main()
