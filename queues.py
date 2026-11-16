# Copyright (C) 2026 Alex Gracia
# SPDX-License-Identifier: GPL-3.0-or-later
"""Egress queue assignment for the frames of a newly scheduled stream.

The frames already deployed keep their queues; only the frames of the new
stream (gate = -1) get one. On each egress port, a new frame avoids the
queues that hold another frame while it is waiting (isolation). If every
queue is in that situation, it takes a queue where sharing keeps the FIFO
order, i.e. none of the frames in that queue enters after the new frame and
leaves before it (or the other way round). If no such queue exists the
stream cannot be deployed.

Each frame waits in its queue from `queued` to `complete`; both instants
are folded into the hyperperiod, so the waiting interval may wrap past it.
"""

from __future__ import annotations

from typing import Dict, List, Tuple


class QueueAssignmentError(RuntimeError):
    """The frames of the new stream cannot be given a queue."""


def in_queue_at_the_same_time(a: dict, b: dict, hyperperiod: float) -> bool:
    """True if the waiting intervals [queued, complete] of frames a and b
    overlap, cyclically over the hyperperiod. Times are measured from the
    instant a is queued: a waits during [0, d(a.complete)], and b overlaps
    it if b is queued meanwhile or b is already waiting at that instant."""
    h = hyperperiod
    da = (a["complete"] - a["queued"]) % h
    dr = (b["queued"] - a["queued"]) % h
    dc = (b["complete"] - a["queued"]) % h
    return dr <= da or dc < dr


def breaks_fifo(a: dict, b: dict, hyperperiod: float) -> bool:
    """True if a and b cannot share a FIFO queue: b is queued at the same
    instant as a, or b is queued after a and leaves before it, or b is
    queued before a and leaves after it."""
    h = hyperperiod
    da = (a["complete"] - a["queued"]) % h
    dr = (b["queued"] - a["queued"]) % h
    dc = (b["complete"] - a["queued"]) % h
    return dr == 0 or dr <= dc <= da or (dc < dr and da <= dc)


def assign_queues(rows: List[dict], new_stream: int, hyperperiod: float,
                  max_queues: int = 8, enforce_isolation: bool = False
                  ) -> int:
    """Give a queue (row['gate']) to every frame of new_stream, in place.

    Returns how many frames share a queue with frames of other streams.
    Raises QueueAssignmentError if a frame cannot be given a queue, or if
    enforce_isolation is set and a frame would have to share one."""
    for row in rows:
        if row["stream"] == new_stream:
            row["gate"] = -1
    by_port: Dict[Tuple[str, str], List[dict]] = {}
    for row in rows:
        by_port.setdefault((row["node"], row["port"]), []).append(row)
    shared = 0
    for port, frames in by_port.items():
        for k, cur in enumerate(frames):
            if cur["gate"] != -1:
                continue
            used, forbidden = set(), set()
            for j, other in enumerate(frames):
                if j == k or other["gate"] == -1:
                    continue
                if in_queue_at_the_same_time(cur, other, hyperperiod):
                    used.add(other["gate"])
                    if breaks_fifo(cur, other, hyperperiod):
                        forbidden.add(other["gate"])
            if len(used) < max_queues:
                gate = 0
                while gate in used:
                    gate += 1
            elif len(forbidden) < max_queues:
                if enforce_isolation:
                    raise QueueAssignmentError(
                        f"stream {cur['stream']} cannot be isolated on "
                        f"{port[0]}->{port[1]}")
                shared += 1
                gate = 0
                while gate in forbidden:
                    gate += 1
            else:
                raise QueueAssignmentError(
                    f"no queue keeps the FIFO order for stream "
                    f"{cur['stream']} on {port[0]}->{port[1]}")
            cur["gate"] = gate
    return shared
