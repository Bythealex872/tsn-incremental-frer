#!/usr/bin/env python3
# Copyright (C) 2026 Alex Gracia
# SPDX-License-Identifier: GPL-3.0-or-later
"""Schedule the streams of an escenario one at a time, as they would arrive
at a running network.

For every stream, in the order of the escenario: enumerate its candidate
paths (every simple talker-to-listener path), compute the gaps of the
current schedule, solve the incremental model, and if a solution exists
merge the new frames into the schedule and assign them egress queues. The
streams already deployed are never modified; a stream without a solution is
rejected and the schedule stays as it was.

USAGE
-----
    python schedule_streams.py escenario.py [--outdir out]
        [--schedule-in schedule.csv] [--jitter-bound worst|best]
        [--queues 8] [--isolation] [--time-limit 300] [--threads 1]
        [--keep-steps]

OUTPUT (in --outdir)
--------------------
    schedule.csv  final schedule: one row per frame and link
    streams.csv   one row per stream: status, max delay, paths, solve time
    with --keep-steps, per stream i: candidates_<i>.py, gaps_<i>.csv and
    schedule_<i>.csv (the inputs and output of incremental_gurobi.py)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from functools import reduce
from math import lcm
from typing import List

import gurobipy as gp
from gurobipy import GRB

import generate_gaps as gg
from incremental_gurobi import (
    IncrementalILP, get_gaps, hyperperiods_to_analyze, merged_rows,
    read_schedule_csv, write_schedule_csv,
)
from network import candidate_flows, load_escenario, write_candidates
from queues import QueueAssignmentError, assign_queues

STATUS = {GRB.OPTIMAL: "OPTIMAL", GRB.INFEASIBLE: "INFEASIBLE",
          GRB.TIME_LIMIT: "TIME_LIMIT"}


def utilization(rows: List[dict], links, bridge_links, h: float) -> float:
    """Occupied share of the bridge-to-bridge links over the hyperperiod."""
    gaps = gg.gaps_of_schedule(rows, links, h)
    free = sum(e - s for link in bridge_links for s, e in gaps[link])
    total = len(bridge_links) * h
    return round((total - free) / total, 6) if total else 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("escenario")
    ap.add_argument("--outdir", default="out")
    ap.add_argument("--schedule-in",
                    help="schedule.csv already deployed (default: empty "
                         "network); its hyperperiod must divide the one of "
                         "the escenario")
    ap.add_argument("--jitter-bound", choices=["worst", "best"],
                    default="worst",
                    help="how max_jitter is enforced (see README)")
    ap.add_argument("--queues", type=int, default=8,
                    help="egress queues per port (default 8)")
    ap.add_argument("--isolation", action="store_true",
                    help="reject a stream if any of its frames has to "
                         "share a queue with another stream's frame")
    ap.add_argument("--time-limit", type=float, default=300,
                    help="solver time limit per stream, in s (default 300)")
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--keep-steps", action="store_true",
                    help="keep the candidate, gaps and schedule files of "
                         "every step")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    esc = load_escenario(args.escenario)
    h = reduce(lcm, (int(s.period) for s in esc.streams), 1)
    links = [(str(a), str(b)) for a, b in esc.network.links]
    bridge_links = [(str(a), str(b)) for a, b in esc.network.bridge_links()]
    # one big-M for the whole run, above any delay of any stream
    big = max(4000, 2 * (h + max(s.e2e_time for s in esc.streams)))
    rows = read_schedule_csv(args.schedule_in) if args.schedule_in else []
    # new streams are numbered after the ones already deployed
    first_id = max((r["stream"] for r in rows), default=-1) + 1
    print(f"{len(esc.streams)} streams, {len(links)} links, hyperperiod "
          f"{h} µs, clock error {esc.clock_skew} µs, processing "
          f"{esc.proc_time} µs, propagation {esc.prop_time} µs")

    log = ["stream,src,dst,period,deadline,replicas,max_jitter,candidates,"
           "status,max_delay,paths,shared_queue_frames,solve_s,"
           "utilization"]
    step_dir = args.outdir if args.keep_steps else None
    for i, stream in enumerate(esc.streams):
        candidates = candidate_flows(esc.network, stream)
        gaps_now = gg.gaps_of_schedule(rows, links, h)
        gaps_path = os.path.join(args.outdir, f"gaps_{i}.csv")
        gg.write_gaps_csv(gaps_now, gaps_path)
        if step_dir:
            write_candidates(candidates,
                             os.path.join(step_dir, f"candidates_{i}.py"))

        status, max_delay, paths, shared, runtime = "NO_PATH", "", "", "", 0.0
        if candidates:
            min_trans = min(c.trans_time for c in candidates)
            gaps, old_h = get_gaps(gaps_path,
                                   min_trans + 2 * esc.clock_skew)
            hh = lcm(int(stream.period), old_h)
            env = gp.Env(empty=True)
            env.setParam("OutputFlag", 0)
            env.start()
            ilp = IncrementalILP(candidates, gaps, hh,
                                 hyperperiods_to_analyze(hh, candidates),
                                 clock_max_error=esc.clock_skew,
                                 big_number=big, prop_time=esc.prop_time,
                                 proc_time=esc.proc_time,
                                 jitter_bound=args.jitter_bound, env=env)
            ilp.model.Params.Threads = args.threads
            ilp.model.Params.TimeLimit = args.time_limit
            ilp.build()
            t0 = time.perf_counter()
            ilp.model.optimize()
            runtime = time.perf_counter() - t0
            status = STATUS.get(ilp.model.Status, str(ilp.model.Status))
            if ilp.model.SolCount > 0:
                chosen = ilp.chosen_paths()
                new_rows, sid = merged_rows(ilp, chosen, rows, old_h,
                                            stream_id=first_id + i)
                try:
                    n_shared = assign_queues(
                        new_rows, sid, ilp.H, max_queues=args.queues,
                        enforce_isolation=args.isolation)
                    rows = new_rows
                    max_delay = f"{ilp.model.ObjVal:g}"
                    paths = " | ".join(
                        "-".join([str(candidates[c].nodes[0][0])]
                                 + [str(b) for _, b in candidates[c].nodes])
                        for c in chosen)
                    shared = str(n_shared)
                except QueueAssignmentError as exc:
                    status = "NO_QUEUE"
                    print(f"  {exc}")
            env.dispose()
        if not step_dir:
            os.remove(gaps_path)
        elif status in ("OPTIMAL", "TIME_LIMIT") and paths:
            write_schedule_csv(rows, os.path.join(step_dir,
                                                  f"schedule_{i}.csv"))

        util = utilization(rows, links, bridge_links, h)
        accepted = bool(paths)
        print(f"stream {first_id + i}: {stream.src}->{stream.dst} T={stream.period} "
              f"R={stream.replica} {status} "
              f"{'accepted' if accepted else 'REJECTED'}"
              f"{f', max delay {max_delay} µs' if accepted else ''} "
              f"({runtime:.2f} s, utilization {util:.1%})")
        log.append(
            f"{first_id + i},{stream.src},{stream.dst},{stream.period},"
            f"{stream.e2e_time},{stream.replica},"
            f"{'' if stream.max_jitter is None else stream.max_jitter},"
            f"{len(candidates)},{status},{max_delay},{paths},{shared},"
            f"{runtime:.3f},{util}")

    write_schedule_csv(rows, os.path.join(args.outdir, "schedule.csv"))
    with open(os.path.join(args.outdir, "streams.csv"), "w") as fh:
        fh.write("\n".join(log) + "\n")
    n_ok = sum(1 for ln in log[1:] if ln.split(",")[10])
    print(f"{n_ok}/{len(esc.streams)} streams accepted; schedule in "
          f"{os.path.join(args.outdir, 'schedule.csv')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
