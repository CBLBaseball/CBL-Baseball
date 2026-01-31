#!/usr/bin/env python3
"""Fetch FanGraphs leaderboard HTML and convert to JSON for the CBL Free Agent Pool page.

Why this exists:
- FanGraphs CSV export is members-only (and embeds can be blocked).
- HTML leaderboards are publicly viewable; we scrape the table and store it locally.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import requests

OUT_DIR = Path("data/fa")
OUT_DIR.mkdir(parents=True, exist_ok=True)

SEASON = 2025  # default season requested

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

def leaders_url(players: List[int], stats: str, month: int) -> str:
    # FanGraphs major-league leaders endpoint used in the browser.
    # We also request "Infinity" page size to get all rows at once.
    return (
        "https://www.fangraphs.com/leaders/major-league?"
        f"ind=0&lg=all&pos=all&qual=0&season={SEASON}&season1={SEASON}"
        f"&type=1&stats={stats}&month={month}&players=" + ",".join(map(str, players)) +
        "&pageitems=2000000000"  # effectively infinity
    )

def scrape_table(url: str) -> List[Dict[str, Any]]:
    r = requests.get(url, timeout=60, headers={"User-Agent":"Mozilla/5.0 (CBL dashboard bot)"})
    r.raise_for_status()

    # FanGraphs renders a data table in the HTML that pandas can usually read.
    # If they change markup, we fail loudly so we can adjust.
    tables = pd.read_html(r.text)
    if not tables:
        return []
    df = tables[0]

    # Clean: drop completely empty columns, normalize column names.
    df = df.dropna(axis=1, how="all")
    df.columns = [str(c).strip() for c in df.columns]

    # Convert to records; keep raw columns (standard preset) as-is.
    records = df.to_dict(orient="records")
    return records

def save_json(name: str, records: List[Dict[str, Any]]):
    (OUT_DIR / f"{name}.json").write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8")

def main():
    tasks = [
        # Hitters A–M
        ("hit_am_bat_all", "hit_am", "bat", 0),
        ("hit_am_bat_lhp", "hit_am", "bat", 13),
        ("hit_am_bat_rhp", "hit_am", "bat", 14),
        # Hitters N–Z
        ("hit_nz_bat_all", "hit_nz", "bat", 0),
        ("hit_nz_bat_lhp", "hit_nz", "bat", 13),
        ("hit_nz_bat_rhp", "hit_nz", "bat", 14),
        # SP
        ("sp_pit_all", "sp", "pit", 0),
        ("sp_pit_lhb", "sp", "pit", 13),
        ("sp_pit_rhb", "sp", "pit", 14),
        # RP A–M
        ("rp_am_pit_all", "rp_am", "pit", 0),
        ("rp_am_pit_lhb", "rp_am", "pit", 13),
        ("rp_am_pit_rhb", "rp_am", "pit", 14),
        # RP N–Z
        ("rp_nz_pit_all", "rp_nz", "pit", 0),
        ("rp_nz_pit_lhb", "rp_nz", "pit", 13),
        ("rp_nz_pit_rhb", "rp_nz", "pit", 14),
    ]

    for out_name, seg_key, stats, month in tasks:
        players = SEGMENTS[seg_key]
        url = leaders_url(players, stats, month)
        print(f"Fetching {out_name}…")
        recs = scrape_table(url)
        save_json(out_name, recs)
        time.sleep(1.0)  # be polite

if __name__ == "__main__":
    main()
