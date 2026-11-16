#!/usr/bin/env python3
# Copyright (C) 2026 Alex Gracia
# SPDX-License-Identifier: GPL-3.0-or-later
"""Free intervals (gaps) of every link, from a deployed schedule.csv.

For every directed link of the escenario topology, the gaps are the parts of
the hyperperiod not covered by any gate window of the schedule (gate windows
already include the clock synchronization error). Windows that wrap past
the end of the hyperperiod are split. The gaps are where the incremental
model can place the frames of a new stream.

USAGE
-----
    python generate_gaps.py escenario.py schedule.csv [-o gaps.csv]
        [--hyperperiod H]

OUTPUT (one row per gap): node,port,gapopen,gapclose
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from functools import reduce
from math import lcm
from typing import Dict, List, Tuple

from network import load_escenario

Link = Tuple[str, str]
Interval = Tuple[float, float]


def schedule_windows(rows: List[dict]) -> Dict[Link, List[Interval]]:
    """Gate windows (open, close) per link of schedule rows."""
    windows: Dict[Link, List[Interval]] = defaultdict(list)
    for r in rows:
        windows[(str(r["node"]), str(r["port"]))].append(
            (float(r["open"]), float(r["close"])))
    return windows


def cyclic_segments(start: float, end: float,
                    hyperperiod: float) -> List[Interval]:
    """Split [start, end) into segments inside [0, hyperperiod)."""
    if end - start >= hyperperiod:
        return [(0.0, hyperperiod)]
    s = start % hyperperiod
    e = s + (end - start)
    if e <= hyperperiod:
        return [(s, e)]
    return [(s, hyperperiod), (0.0, e - hyperperiod)]


def merge_intervals(intervals: List[Interval]) -> List[Interval]:
    merged: List[Interval] = []
    for s, e in sorted(intervals):
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    return merged


def free_intervals(busy: List[Interval], hyperperiod: float
                   ) -> List[Interval]:
    gaps: List[Interval] = []
    cursor = 0.0
    for s, e in merge_intervals(busy):
        if s > cursor:
            gaps.append((cursor, s))
        cursor = max(cursor, e)
    if cursor < hyperperiod:
        gaps.append((cursor, hyperperiod))
    return gaps


def gaps_of_schedule(rows: List[dict], links: List[Link],
                     hyperperiod: float) -> Dict[Link, List[Interval]]:
    windows = schedule_windows(rows)
    gaps = {}
    for link in links:
        busy: List[Interval] = []
        for start, end in windows.get(link, []):
            if end < start:  # window written as wrapped (close < open)
                busy.extend([(start, float(hyperperiod)), (0.0, end)])
            else:
                busy.extend(cyclic_segments(start, end, hyperperiod))
        gaps[link] = free_intervals(busy, hyperperiod)
    return gaps


def fmt(x: float) -> str:
    return str(int(x)) if float(x).is_integer() else f"{x:.10g}"


def write_gaps_csv(gaps: Dict[Link, List[Interval]], path: str) -> int:
    lines = ["node,port,gapopen,gapclose"]
    for (node, port), intervals in gaps.items():
        for s, e in intervals:
            lines.append(f"{node},{port},{fmt(s)},{fmt(e)}")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return len(lines) - 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("escenario", help="escenario .py (topology)")
    ap.add_argument("schedule", help="deployed schedule.csv")
    ap.add_argument("-o", "--output", default="gaps.csv")
    ap.add_argument("--hyperperiod", type=float, default=None,
                    help="default: lcm of the periods of the escenario "
                         "streams")
    args = ap.parse_args()

    from incremental_gurobi import read_schedule_csv
    esc = load_escenario(args.escenario)
    h = args.hyperperiod or float(
        reduce(lcm, (int(s.period) for s in esc.streams), 1))
    links = [(str(a), str(b)) for a, b in esc.network.links]
    gaps = gaps_of_schedule(read_schedule_csv(args.schedule), links, h)
    n = write_gaps_csv(gaps, args.output)
    print(f"{args.output}: {n} gaps on {len(links)} links "
          f"(hyperperiod {fmt(h)} µs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
