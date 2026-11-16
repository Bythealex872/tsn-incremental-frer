# Copyright (C) 2026 Alex Gracia
# SPDX-License-Identifier: GPL-3.0-or-later
"""Stream description shared by the escenario files and the model."""

from __future__ import annotations

from typing import List, Optional, Tuple

Link = Tuple[int, int]


class Flow:
    """A time-sensitive stream, or one candidate path of a stream.

    In an escenario (input of schedule_streams.py) a Flow describes a stream
    by its talker and listener and leaves `node_list` empty. In a candidate
    file (input of incremental_gurobi.py) there is one Flow per candidate
    path, all with the same stream attributes, and `node_list` holds the
    links of that path.

    Args:
        t_time: transmission time of one frame on a link (µs).
        period: stream period T_s (µs).
        node_list: links of the path, [(node, next_node), ...]; a list of
            links forming a tree describes a multicast path.
        end2end_time: end-to-end deadline D_s (µs).
        replica: number R_s of node-disjoint member streams (FRER).
        src: talker end station.
        dst: listener end station.
        max_jitter: jitter bound J_s (µs), or None for no bound.
    """

    def __init__(self, t_time: float, period: int, node_list: List[Link],
                 end2end_time: float, replica: int = 1,
                 src: Optional[int] = None, dst: Optional[int] = None,
                 max_jitter: Optional[float] = None):
        if period < t_time:
            raise ValueError(
                f"period {period} µs is shorter than the transmission time "
                f"{t_time} µs")
        if replica < 1:
            raise ValueError("replica must be >= 1")
        self.trans_time = t_time
        self.period = period
        self.nodes = node_list
        self.start_node = node_list[0] if node_list else None
        # end nodes: links not followed by another link of the path
        self.end_nodes = set(node_list)
        for node in node_list:
            for node2 in node_list:
                if node[1] == node2[0]:
                    self.end_nodes.discard(node)
                    break
        self.e2e_time = end2end_time
        self.replica = replica
        self.src = src if src is not None else (
            node_list[0][0] if node_list else None)
        self.dst = dst
        self.max_jitter = max_jitter

    def __repr__(self) -> str:
        return (f"Flow(src={self.src}, dst={self.dst}, T={self.period}, "
                f"D={self.e2e_time}, R={self.replica}, "
                f"J={self.max_jitter}, links={len(self.nodes)})")
