#!/usr/bin/env python3
# Copyright (C) 2026 Alex Gracia
# SPDX-License-Identifier: GPL-3.0-or-later
"""Independent check of a schedule.csv, cyclic over the hyperperiod.

It does not reuse any scheduler code:

  1. ROWS: open < complete <= close (complete unrolled past the
     hyperperiod when the window crosses it).
  2. GATE WINDOWS: on each link, the gate windows [open, close) of
     different streams do not overlap.
  3. TRANSMISSIONS: on each link, the transmissions [complete - tx,
     complete) of all frames are disjoint, with tx = close - open - 2*skew.
  4. QUEUES: two frames of different streams in the same queue of a port
     whose waiting intervals [queued, complete] overlap leave in the order
     they entered (FIFO). Pairs that share a queue respecting FIFO are
     counted, not reported as errors.

USAGE
-----
    python check_schedule.py schedule.csv --hyperperiod 2000 --skew 1

Exit code: 0 = valid, 1 = violations found.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

EPS = 1e-6


def read_rows(path: str) -> List[dict]:
    with open(path) as fh:
        lines = [ln.strip() for ln in fh if ln.strip()]
    if lines and not lines[0].startswith("stream"):
        lines = lines[1:]
    header = lines[0].split(",")
    rows = []
    for ln in lines[1:]:
        row = dict(zip(header, ln.split(",")))
        for k in ("queued", "open", "complete", "close"):
            row[k] = float(row[k])
        for k in ("stream", "frame", "gate"):
            row[k] = int(row[k])
        row["node"], row["port"] = int(row["node"]), int(row["port"])
        rows.append(row)
    return rows


def unrolled(start: float, end: float, h: float) -> Tuple[float, float]:
    """Cyclic interval as absolute instants, end >= start."""
    return start, end + (h if end < start else 0.0)


def cyclic_overlap(a: Tuple[float, float], b: Tuple[float, float],
                   h: float) -> bool:
    for k in (-1, 0, 1):
        if b[0] + k * h < a[1] - EPS and a[0] < b[1] + k * h - EPS:
            return True
    return False


def fifo_conflict(a: dict, b: dict, h: float) -> Optional[bool]:
    """None if the waiting intervals do not overlap; True if the pair breaks
    FIFO (same queuing instant, or they leave in the opposite order);
    False if they only share the queue."""
    ar, ac = unrolled(a["queued"], a["complete"], h)
    br, bc = unrolled(b["queued"], b["complete"], h)
    for k in (-1, 0, 1):
        r, c = br + k * h, bc + k * h
        if r < ac - EPS and ar < c - EPS:
            if abs(ar - r) < EPS:
                return True
            return (ar < r) != (ac < c)
    return None


def check(rows: List[dict], h: float, skew: float) -> Dict[str, list]:
    res: Dict[str, list] = defaultdict(list)
    for r in rows:
        c = r["complete"] + (h if r["complete"] < r["open"] - EPS else 0.0)
        if not (r["open"] < c - EPS and c <= r["close"] + EPS):
            res["bad_rows"].append(r)
    by_link: Dict[Tuple[int, int], List[dict]] = defaultdict(list)
    for r in rows:
        by_link[(r["node"], r["port"])].append(r)
    for link, frames in by_link.items():
        tx = []
        for r in frames:
            t = r["close"] - r["open"] - 2 * skew
            tx.append(unrolled((r["complete"] - t) % h, r["complete"], h))
        for i in range(len(frames)):
            for j in range(i + 1, len(frames)):
                a, b = frames[i], frames[j]
                if (a["stream"] != b["stream"] and cyclic_overlap(
                        unrolled(a["open"], a["close"], h),
                        unrolled(b["open"], b["close"], h), h)):
                    res["window_overlaps"].append((link, a, b))
                if cyclic_overlap(tx[i], tx[j], h):
                    res["tx_overlaps"].append((link, a, b))
                if a["gate"] != b["gate"] or a["stream"] == b["stream"]:
                    continue
                conflict = fifo_conflict(a, b, h)
                if conflict is True:
                    res["fifo_conflicts"].append((link, a, b))
                elif conflict is False:
                    res["shared_pairs"].append((link, a, b))
    return res


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("schedule")
    ap.add_argument("--hyperperiod", type=float, required=True)
    ap.add_argument("--skew", type=float, default=0,
                    help="clock synchronization error used to build the "
                         "schedule, in µs (default 0)")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="list every violation")
    args = ap.parse_args()
    rows = read_rows(args.schedule)
    res = check(rows, args.hyperperiod, args.skew)
    errors = ("bad_rows", "window_overlaps", "tx_overlaps", "fifo_conflicts")
    print(f"{args.schedule}: {len(rows)} rows, "
          f"{len({r['stream'] for r in rows})} streams, queues used "
          f"{sorted({r['gate'] for r in rows})}")
    for k in errors + ("shared_pairs",):
        print(f"  {k.replace('_', ' ')}: {len(res[k])}")
        if args.verbose and k in errors:
            for item in res[k]:
                print(f"    {item}")
    bad = any(res[k] for k in errors)
    print("VALID" if not bad else "VIOLATIONS FOUND")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
