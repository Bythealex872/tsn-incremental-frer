#!/usr/bin/env python3
# Copyright (C) 2026 Alex Gracia
# SPDX-License-Identifier: GPL-3.0-or-later
"""Incremental path selection and scheduling ILP for one new stream.

Given the candidate paths of a new stream (one flow.Flow per path) and the
free intervals (gaps) of the schedule already deployed, the model selects
`replica` node-disjoint paths (FRER member streams) and places every frame
of the hyperperiod in the gaps of each link, so that the deployed streams
are not modified. The end-to-end delay of every member stream is bounded by
the deadline, the jitter by the stream's max_jitter (if given), and the
maximum end-to-end delay is minimized. The gate windows of the new frames
widen the transmission by the clock synchronization error on both sides.

After solving, the new frames are merged into the deployed schedule and
every new transmission gets an egress queue (see queues.py).

USAGE
-----
    python incremental_gurobi.py candidates.py gaps.csv
        [--schedule-in schedule.csv --csv merged.csv]
        [--clock-skew 1] [--proc-time 4] [--prop-time 0]
        [--jitter-bound worst|best] [--queues 8]
        [--time-limit 300] [--threads 1] [--lp model.lp] [--verbose]

The candidate file can be written by network.write_candidates();
schedule_streams.py runs the whole loop (paths, gaps, model, queues) for
every stream of an escenario.
"""

from __future__ import annotations

import argparse
import csv
import sys
from math import gcd
from typing import Dict, List, Optional, Tuple

import gurobipy as gp
from gurobipy import GRB

from network import load_module
from queues import QueueAssignmentError, assign_queues

Node = Tuple[int, int]
SCHEDULE_HEADER = ("stream,replica,frame,node,port,queued,open,complete,"
                   "close,gate")


def lcm(a: int, b: int) -> int:
    return a if a == b else (a * b) // gcd(a, b)


class Gap:
    """Free interval [gopen, gclose) of the link (node, port)."""

    def __init__(self, pnode: int, pport: int, gopen: float, gclose: float):
        self.node: Node = (pnode, pport)
        self.gopen = gopen
        self.gclose = gclose


def get_gaps(path: str, required_size: float) -> Tuple[List[Gap], int]:
    """Gaps of gaps.csv that can hold a gate window of required_size, and
    the hyperperiod of the deployed schedule (largest gap end)."""
    gaps: List[Gap] = []
    hyperperiod = -1.0
    with open(path) as fh:
        for row in csv.DictReader(fh):
            gopen = float(row["gapopen"])
            gclose = float(row["gapclose"])
            if gclose - gopen >= required_size:
                gaps.append(Gap(int(row["node"]), int(row["port"]),
                                gopen, gclose))
            hyperperiod = max(hyperperiod, gclose)
    return gaps, int(hyperperiod)


def load_candidates(path: str) -> list:
    return load_module(path, "candidates").set_data_to_schedule()


def default_big_number(hyperperiod: int, flows: list) -> int:
    """Big-M constant larger than any delay the model can take."""
    return max(4000, 2 * (hyperperiod + max(f.e2e_time for f in flows)))


def hyperperiods_to_analyze(hyperperiod: int, flows: list) -> int:
    """Hyperperiods a frame may span: its own plus the deadline."""
    return 2 + (hyperperiod + max(f.e2e_time for f in flows) - 1) // hyperperiod


class IncrementalILP:
    """The ILP for one new stream. Every candidate path is an index fl of
    `flows`; path(fl) = 1 when it is selected as a member stream."""

    def __init__(self, flows: list, gaps: List[Gap], hyperperiod: int,
                 num_hyperperiods: int, clock_max_error: float = 0,
                 big_number: Optional[int] = None, prop_time: float = 0,
                 proc_time: float = 0, jitter_bound: str = "worst",
                 env=None):
        if jitter_bound not in ("worst", "best"):
            raise ValueError("jitter_bound must be 'worst' or 'best'")
        self.flows = flows
        self.gaps = gaps
        self.H = hyperperiod
        self.periods = num_hyperperiods
        self.err = clock_max_error
        self.big = (big_number if big_number is not None
                    else default_big_number(hyperperiod, flows))
        self.prop = prop_time   # PropT, link propagation delay
        self.proc = proc_time   # ProcT, processing time at the next node
        self.jitter_bound = jitter_bound
        self.max_jitter = flows[0].max_jitter
        self.model = (gp.Model("incremental", env=env) if env is not None
                      else gp.Model("incremental"))
        self.vars: Dict[str, gp.Var] = {}
        self.infeasible_paths: List[int] = []

    # ---- variables ----

    def v(self, name: str) -> gp.Var:
        """Continuous variable, lb = 0."""
        if name not in self.vars:
            self.vars[name] = self.model.addVar(lb=0, name=name)
        return self.vars[name]

    def b(self, name: str) -> gp.Var:
        """Binary variable."""
        if name not in self.vars:
            self.vars[name] = self.model.addVar(vtype=GRB.BINARY, name=name)
        return self.vars[name]

    def free(self, name: str) -> gp.Var:
        """Unbounded continuous variable."""
        var = self.v(name)
        var.LB = -GRB.INFINITY
        return var

    def f_ready(self, fl, fr, n):
        return self.v("st%dfr%dsw%dpt%dready" % (fl, fr, n[0], n[1]))

    def f_start(self, fl, fr, n):
        return self.v("st%dfr%dsw%dpt%dstart" % (fl, fr, n[0], n[1]))

    def f_complete(self, fl, fr, n):
        return self.v("st%dfr%dsw%dpt%dcomplete" % (fl, fr, n[0], n[1]))

    def f_offset(self, fl, fr, n):
        return self.v("st%dfr%dsw%dpt%doffset" % (fl, fr, n[0], n[1]))

    def g_open(self, fl, n, w):
        return self.v("sw%dpt%dgt%dw%dopenOffset" % (n[0], n[1], fl, w))

    def g_close(self, fl, n, w):
        return self.v("sw%dpt%dgt%dw%dcloseOffset" % (n[0], n[1], fl, w))

    def bv_gap_open(self, fl, fr, n, g):
        return self.b("sw%dpt%dgap%dst%dfr%dopen" % (n[0], n[1], g, fl, fr))

    def bv_gap_close(self, fl, fr, n, g):
        return self.b("sw%dpt%dgap%dst%dfr%dclose" % (n[0], n[1], g, fl, fr))

    def bv_window(self, fl, fr, n, p):
        return self.b("st%dfr%dsw%dpt%dwindow%dhp%d"
                      % (fl, fr, n[0], n[1], fr, p))

    def flow_offset(self, fl):
        return self.v("st%doffset" % fl)

    def fr_delay(self, fl, fr):
        return self.v("st%dfr%ddelay" % (fl, fr))

    def fr_jitter(self, fl, fr):
        return self.v("st%dfr%djitter" % (fl, fr))

    def path(self, fl):
        return self.b("path%d" % fl)

    def n_frames(self, fl) -> int:
        return int(self.H / self.flows[fl].period)

    # ---- constraints ----

    def offset_constraints(self):
        """One release offset for the stream, shared by all its paths."""
        for fl in range(len(self.flows)):
            if fl == 0:
                self.model.addConstr(
                    self.flow_offset(fl) <= self.flows[fl].period)
            else:
                self.model.addConstr(
                    self.flow_offset(fl) == self.flow_offset(0))

    def first_ready_constraints(self):
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                self.model.addConstr(
                    self.f_ready(fl, fr, f.start_node)
                    == fr * f.period + self.flow_offset(fl))

    def frame_start_constraints(self):
        for fl, f in enumerate(self.flows):
            for node in f.nodes:
                for fr in range(self.n_frames(fl)):
                    self.model.addConstr(
                        self.f_ready(fl, fr, node)
                        <= self.f_start(fl, fr, node))

    def frame_completion_constraints(self):
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for node in f.nodes:
                    self.model.addConstr(
                        self.f_complete(fl, fr, node)
                        == self.f_start(fl, fr, node) + f.trans_time)

    def path_constraints(self):
        """Store and forward: a frame is queued on a link once it has been
        received through the previous link and processed."""
        for fl, f in enumerate(self.flows):
            for src in f.nodes:
                for dst in f.nodes:
                    if src[1] == dst[0]:
                        for fr in range(self.n_frames(fl)):
                            self.model.addConstr(
                                self.f_complete(fl, fr, src)
                                + self.prop + self.proc
                                == self.f_ready(fl, fr, dst))

    def frame_order_constraints(self):
        for fl, f in enumerate(self.flows):
            for node in f.nodes:
                for fr in range(1, self.n_frames(fl)):
                    self.model.addConstr(
                        self.f_complete(fl, fr - 1, node)
                        <= self.f_start(fl, fr, node))

    def cyclic_frame_order_constraints(self):
        """The last frame of a hyperperiod leaves a link before the first
        frame of the next one starts."""
        for fl, f in enumerate(self.flows):
            n = self.n_frames(fl)
            if n < 2:
                continue
            for node in f.nodes:
                self.model.addConstr(
                    self.f_complete(fl, n - 1, node)
                    <= self.f_start(fl, 0, node) + self.H)

    def gate_constraints_single(self):
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for node in f.nodes:
                    self.model.addConstr(self.g_open(fl, node, fr) >= 0)
                    self.model.addConstr(
                        self.g_open(fl, node, fr)
                        <= self.g_close(fl, node, fr))
                    self.model.addConstr(
                        self.g_close(fl, node, fr) <= self.H)

    def window_to_gap_constraints(self):
        """Every gate window lies inside one gap of its link."""
        H = self.H
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for node in f.nodes:
                    for g, gap in enumerate(self.gaps):
                        if node != gap.node:
                            continue
                        vo = self.bv_gap_open(fl, fr, node, g)
                        vc = self.bv_gap_close(fl, fr, node, g)
                        op = self.g_open(fl, node, fr)
                        cl = self.g_close(fl, node, fr)
                        self.model.addConstr(op - gap.gopen <= H * vo)
                        self.model.addConstr(gap.gopen - op <= H - H * vo)
                        self.model.addConstr(cl - gap.gclose <= H - H * vc)
                        self.model.addConstr(gap.gclose - cl <= H * vc)
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for node in f.nodes:
                    terms = []
                    for g, gap in enumerate(self.gaps):
                        if node == gap.node:
                            terms.append(self.bv_gap_open(fl, fr, node, g))
                            terms.append(self.bv_gap_close(fl, fr, node, g))
                    if terms:
                        self.model.addConstr(
                            gp.quicksum(terms) == len(terms) // 2 + 1)
                    elif fl not in self.infeasible_paths:
                        # the link has no usable gap: the path cannot be used
                        self.model.addConstr(self.path(fl) == 0)
                        self.infeasible_paths.append(fl)

    def frame_start_offset_constraints(self):
        """start = offset within the hyperperiod + whole hyperperiods."""
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for node in f.nodes:
                    self.model.addConstr(
                        self.f_start(fl, fr, node)
                        == self.f_offset(fl, fr, node)
                        + gp.quicksum(p * self.H * self.bv_window(fl, fr, node, p)
                                      for p in range(self.periods)))
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for node in f.nodes:
                    self.model.addConstr(
                        gp.quicksum(self.bv_window(fl, fr, node, p)
                                    for p in range(self.periods)) == 1)

    def gate_transmission_start_constraints(self):
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for node in f.nodes:
                    self.model.addConstr(
                        self.g_open(fl, node, fr)
                        == self.f_offset(fl, fr, node) - self.err)

    def gate_transmission_complete_constraints(self):
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for node in f.nodes:
                    self.model.addConstr(
                        self.f_offset(fl, fr, node) + f.trans_time + self.err
                        == self.g_close(fl, node, fr))

    def frame_delay_constraints(self):
        for fl, f in enumerate(self.flows):
            for fr in range(self.n_frames(fl)):
                for end_node in f.end_nodes:
                    self.model.addConstr(
                        self.f_complete(fl, fr, end_node)
                        + self.prop + self.proc
                        - self.f_ready(fl, fr, f.start_node)
                        == self.fr_delay(fl, fr))
        for fl in range(len(self.flows)):
            for fr in range(self.n_frames(fl)):
                self.model.addConstr(
                    self.fr_delay(fl, fr) <= self.v("st%dmaxDelay" % fl))

    def jitter_constraints(self):
        """Per-path jitter: largest deviation from the path's mean delay."""
        for fl in range(len(self.flows)):
            n = self.n_frames(fl)
            self.model.addConstr(
                self.v("st%davgDelay" % fl)
                == gp.quicksum((1 / n) * self.fr_delay(fl, fr)
                               for fr in range(n)))
        for fl in range(len(self.flows)):
            for fr in range(self.n_frames(fl)):
                avg = self.v("st%davgDelay" % fl)
                self.model.addConstr(
                    avg - self.fr_delay(fl, fr) <= self.fr_jitter(fl, fr))
                self.model.addConstr(
                    self.fr_delay(fl, fr) - avg <= self.fr_jitter(fl, fr))
        for fl in range(len(self.flows)):
            for fr in range(self.n_frames(fl)):
                self.model.addConstr(
                    self.fr_jitter(fl, fr) <= self.v("st%djitter" % fl))

    def path_delay_constraints(self):
        for fl in range(len(self.flows)):
            self.model.addConstr(
                self.free("st%ddelay_p" % fl)
                == self.v("st%davgDelay" % fl)
                - self.big + self.big * self.path(fl))
        for fl in range(len(self.flows)):
            self.model.addConstr(
                self.v("st%ddelay_p" % fl) <= self.v("delay"))

    def path_maxdelay_constraints(self):
        """MaxDelay = largest delay over the selected paths <= deadline."""
        for fl in range(len(self.flows)):
            self.model.addConstr(
                self.free("st%dmaxdelay_p" % fl)
                == self.v("st%dmaxDelay" % fl)
                - self.big + self.big * self.path(fl))
            self.model.addConstr(self.v("st%dmaxDelay" % fl) <= self.big)
        for fl in range(len(self.flows)):
            self.model.addConstr(
                self.v("st%dmaxdelay_p" % fl) <= self.v("MaxDelay"))
        self.model.addConstr(self.v("MaxDelay") <= self.flows[0].e2e_time)

    def path_jitter_constraints(self):
        """jitter = largest per-path jitter over the selected paths (best
        case: permanent failures); Diff = spread between the largest and
        the smallest delay over the selected paths (worst case:
        intermittent failures, jitter = Diff / 2)."""
        for fl in range(len(self.flows)):
            self.model.addConstr(
                self.free("st%djitter_p" % fl)
                == self.v("st%djitter" % fl)
                - self.big + self.big * self.path(fl))
        for fl in range(len(self.flows)):
            self.model.addConstr(
                self.v("st%djitter_p" % fl) <= self.v("jitter"))
        for fl in range(len(self.flows)):
            for fr in range(self.n_frames(fl)):
                self.model.addConstr(
                    self.v("st%dminDelay" % fl) <= self.fr_delay(fl, fr))
        for fl in range(len(self.flows)):
            self.model.addConstr(
                self.free("st%dminDelay_p" % fl)
                == self.v("st%dminDelay" % fl)
                + self.big - self.big * self.path(fl))
        for fl in range(len(self.flows)):
            self.model.addConstr(
                self.v("MinDelay") <= self.v("st%dminDelay_p" % fl))
        self.model.addConstr(
            self.v("Diff") == self.v("MaxDelay") - self.v("MinDelay"))
        self.model.addConstr(0.5 * self.v("Diff") <= self.v("maxjitter"))
        self.model.addConstr(self.v("jitter") <= self.v("maxjitter"))

    def jitter_bound_constraints(self):
        """Jitter of the stream <= max_jitter, under the chosen failure
        assumption."""
        if self.max_jitter is None:
            return
        if self.jitter_bound == "best":
            self.model.addConstr(self.v("jitter") <= self.max_jitter,
                                 name="jitter_bound_best")
        else:
            self.model.addConstr(0.5 * self.v("Diff") <= self.max_jitter,
                                 name="jitter_bound_worst")

    def bridge_constraints(self):
        """Selected paths are node-disjoint: a bridge is used by at most one
        of them."""
        element_flows: Dict[int, List[int]] = {}
        for route_idx, f in enumerate(self.flows, 1):
            nodes = set()
            for (a, b) in f.nodes:
                nodes.add(a)
                nodes.add(b)
            for node in nodes:
                if node > 1000:
                    element_flows.setdefault(node, []).append(route_idx)
        for node in sorted(element_flows.keys()):
            br = self.v("br%d" % node)
            self.model.addConstr(
                br == gp.quicksum(self.path(idx - 1)
                                  for idx in element_flows[node]))
            self.model.addConstr(br <= 1)

    def path_binary_variables(self):
        """Exactly `replica` paths are selected."""
        self.model.addConstr(
            gp.quicksum(self.path(fl) for fl in range(len(self.flows)))
            == self.flows[0].replica)

    def objective(self):
        self.model.setObjective(self.v("MaxDelay"), GRB.MINIMIZE)

    def build(self):
        self.objective()
        self.offset_constraints()
        self.first_ready_constraints()
        self.frame_start_constraints()
        self.frame_completion_constraints()
        self.path_constraints()
        self.frame_order_constraints()
        self.cyclic_frame_order_constraints()
        self.gate_constraints_single()
        self.window_to_gap_constraints()
        self.frame_start_offset_constraints()
        self.gate_transmission_start_constraints()
        self.gate_transmission_complete_constraints()
        self.frame_delay_constraints()
        self.jitter_constraints()
        self.path_delay_constraints()
        self.path_maxdelay_constraints()
        self.path_jitter_constraints()
        self.jitter_bound_constraints()
        self.bridge_constraints()
        self.path_binary_variables()
        self.model.update()

    def chosen_paths(self) -> List[int]:
        return [fl for fl in range(len(self.flows))
                if self.path(fl).X > 0.5]


# ---- schedule.csv ----

def read_schedule_csv(path: str) -> List[dict]:
    """Read a schedule.csv (with or without the leading file-name line)."""
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
        for k in ("stream", "replica", "frame", "gate"):
            row[k] = int(row[k])
        rows.append(row)
    return rows


def write_schedule_csv(rows: List[dict], path: str) -> None:
    lines = [SCHEDULE_HEADER]
    for r in rows:
        lines.append(
            f"{r['stream']},{r['replica']},{r['frame']},{r['node']},"
            f"{r['port']},{r['queued']:.1f},{r['open']:.1f},"
            f"{r['complete']:.1f},{r['close']:.1f},{r['gate']}")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


def merged_rows(ilp: IncrementalILP, chosen: List[int], old_rows: List[dict],
                old_h: int, stream_id: Optional[int] = None
                ) -> Tuple[List[dict], int]:
    """Deployed schedule (repeated if the hyperperiod grew) plus the frames
    of the new stream, which get gate = -1 (no queue yet).

    Per row: queued = instant the frame enters the egress queue,
    open/close = gate window (clock error included), complete = end of the
    transmission; all folded into [0, H)."""
    rows: List[dict] = []
    reps = ilp.H // old_h if old_h > 0 else 1
    frames_per_stream: Dict[int, int] = {}
    for r in old_rows:
        frames_per_stream[r["stream"]] = max(
            frames_per_stream.get(r["stream"], 0), r["frame"] + 1)
    for j in range(reps):
        for r in old_rows:
            row = dict(r)
            row["frame"] = r["frame"] + j * frames_per_stream[r["stream"]]
            for k in ("queued", "open", "complete", "close"):
                row[k] = r[k] + j * old_h
            rows.append(row)
    if stream_id is None:
        stream_id = max((r["stream"] for r in old_rows), default=-1) + 1
    big_h = float(ilp.H)
    for rep_idx, fl in enumerate(chosen):
        f = ilp.flows[fl]
        for fr in range(ilp.n_frames(fl)):
            for node in f.nodes:
                rows.append({
                    "stream": stream_id, "replica": rep_idx, "frame": fr,
                    "node": str(node[0]), "port": str(node[1]),
                    "queued": ilp.f_ready(fl, fr, node).X % big_h,
                    "open": ilp.g_open(fl, node, fr).X,
                    "complete": ilp.f_complete(fl, fr, node).X % big_h,
                    "close": ilp.g_close(fl, node, fr).X,
                    "gate": -1,
                })
    return rows, stream_id


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("candidates",
                    help="candidate file: one Flow per candidate path")
    ap.add_argument("gaps", help="gaps.csv of the deployed schedule")
    ap.add_argument("--schedule-in",
                    help="deployed schedule.csv to merge the new stream into")
    ap.add_argument("--csv", help="write the merged schedule.csv here")
    ap.add_argument("--clock-skew", type=float, default=0,
                    help="clock synchronization error in µs (default 0)")
    ap.add_argument("--proc-time", type=float, default=0,
                    help="processing time per node in µs (default 0)")
    ap.add_argument("--prop-time", type=float, default=0,
                    help="link propagation delay in µs (default 0)")
    ap.add_argument("--jitter-bound", choices=["worst", "best"],
                    default="worst",
                    help="how max_jitter is enforced: worst = intermittent "
                         "failures, (max - min delay over the member "
                         "streams) / 2; best = permanent failures, largest "
                         "per-path jitter (default worst)")
    ap.add_argument("--queues", type=int, default=8,
                    help="egress queues per port (default 8)")
    ap.add_argument("--isolation", action="store_true",
                    help="reject the stream if any of its frames has to "
                         "share a queue with another stream's frame")
    ap.add_argument("--big-number", type=int, default=None,
                    help="big-M constant (default: from the hyperperiod "
                         "and the deadline)")
    ap.add_argument("--time-limit", type=float, default=300)
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--lp", help="write the model to this .lp file")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    flows = load_candidates(args.candidates)
    min_trans = min(f.trans_time for f in flows)
    gaps, old_h = get_gaps(args.gaps, min_trans + 2 * args.clock_skew)
    h = lcm(flows[0].period, old_h)
    nper = hyperperiods_to_analyze(h, flows)
    print(f"Candidate paths: {len(flows)} (select {flows[0].replica} "
          f"node-disjoint); gaps kept: {len(gaps)}; hyperperiod {h} µs")

    ilp = IncrementalILP(flows, gaps, h, nper, clock_max_error=args.clock_skew,
                         big_number=args.big_number, prop_time=args.prop_time,
                         proc_time=args.proc_time,
                         jitter_bound=args.jitter_bound)
    ilp.model.Params.LogToConsole = 1 if args.verbose else 0
    ilp.model.Params.Threads = args.threads
    ilp.model.Params.TimeLimit = args.time_limit
    ilp.build()
    if args.lp:
        ilp.model.write(args.lp)
    ilp.model.optimize()
    status = {GRB.OPTIMAL: "OPTIMAL", GRB.INFEASIBLE: "INFEASIBLE",
              GRB.TIME_LIMIT: "TIME_LIMIT"}.get(ilp.model.Status,
                                                str(ilp.model.Status))
    print(f"Status: {status} ({ilp.model.Runtime:.2f} s)")
    if ilp.model.SolCount == 0:
        return 1

    chosen = ilp.chosen_paths()
    print(f"Max end-to-end delay: {ilp.model.ObjVal:g} µs")
    for fl in chosen:
        links = ", ".join(f"{a}->{b}" for a, b in flows[fl].nodes)
        print(f"  path {fl}: {links}")

    if args.csv:
        old_rows = read_schedule_csv(args.schedule_in) \
            if args.schedule_in else []
        rows, sid = merged_rows(ilp, chosen, old_rows, old_h)
        try:
            shared = assign_queues(rows, sid, ilp.H, max_queues=args.queues,
                                   enforce_isolation=args.isolation)
        except QueueAssignmentError as exc:
            print(f"Stream rejected by the queue assignment: {exc}")
            return 1
        write_schedule_csv(rows, args.csv)
        print(f"Merged schedule written to {args.csv} "
              f"({shared} frame(s) share a queue)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
