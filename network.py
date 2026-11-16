# Copyright (C) 2026 Alex Gracia
# SPDX-License-Identifier: GPL-3.0-or-later
"""Escenario loading and candidate path enumeration.

An escenario is a Python file that defines:

  * a dict whose name starts with ``network_topology``:
    {bridge: [neighbour bridges]} (links are bidirectional);
  * a dict whose name starts with ``listeners_talkers``:
    {bridge: [attached end stations]}; an end station attached to several
    bridges can send node-disjoint member streams (FRER);
  * ``set_data_to_schedule()``, returning the streams as flow.Flow objects
    with src, dst, period, deadline, replicas and optional max_jitter;
  * optionally CLOCK_SKEW (µs, clock synchronization error, default 0),
    SWITCH_MSD (µs, bridge processing time, default 0) and PROP_TIME (µs,
    link propagation delay, default 0).

Bridge ids must be greater than 1000 and end-station ids at most 1000.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple

from flow import Flow

Link = Tuple[int, int]


class Network:
    """Directed graph of bridges and end stations."""

    def __init__(self) -> None:
        self.kind: Dict[int, str] = {}
        self.links: List[Link] = []
        self.outgoing: Dict[int, List[Link]] = defaultdict(list)

    def add_node(self, node: int, kind: str) -> None:
        self.kind.setdefault(node, kind)

    def add_link(self, a: int, b: int) -> None:
        if (a, b) not in self.outgoing[a]:
            self.links.append((a, b))
            self.outgoing[a].append((a, b))

    def bridge_links(self) -> List[Link]:
        return [(a, b) for a, b in self.links
                if self.kind[a] == "bridge" and self.kind[b] == "bridge"]

    def all_simple_paths(self, src: int, dst: int,
                         max_hops: Optional[int] = None) -> List[List[Link]]:
        """Every loop-free path from src to dst. End stations other than
        src and dst never forward traffic."""
        results: List[List[Link]] = []
        max_hops = max_hops or len(self.kind)

        def dfs(node: int, visited: Set[int], path: List[Link]) -> None:
            if node == dst:
                results.append(list(path))
                return
            if len(path) >= max_hops:
                return
            if node != src and self.kind[node] == "host":
                return
            for link in self.outgoing[node]:
                nxt = link[1]
                if nxt in visited:
                    continue
                visited.add(nxt)
                path.append(link)
                dfs(nxt, visited, path)
                path.pop()
                visited.remove(nxt)

        dfs(src, {src}, [])
        return results


@dataclass
class Escenario:
    topology: Dict[int, List[int]]
    attachments: Dict[int, List[int]]
    streams: List[Flow]
    network: Network
    clock_skew: float = 0
    proc_time: float = 0
    prop_time: float = 0


def load_module(path: str, name: str = "escenario"):
    """Import an escenario or candidate file given by its path."""
    abs_path = os.path.abspath(path)
    for d in (os.path.dirname(abs_path),
              os.path.dirname(os.path.abspath(__file__))):
        if d not in sys.path:
            sys.path.insert(0, d)
    spec = importlib.util.spec_from_file_location(name, abs_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def dict_by_prefix(mod, prefix: str) -> dict:
    matches = [a for a in dir(mod)
               if a.startswith(prefix) and isinstance(getattr(mod, a), dict)]
    if len(matches) != 1:
        raise ValueError(f"the escenario must define exactly one dict whose "
                         f"name starts with '{prefix}' (found: "
                         f"{matches or 'none'})")
    return getattr(mod, matches[0])


def load_escenario(path: str) -> Escenario:
    mod = load_module(path)
    topology = dict_by_prefix(mod, "network_topology")
    attachments = dict_by_prefix(mod, "listeners_talkers")
    net = Network()
    for br in topology:
        net.add_node(int(br), "bridge")
    for br, hosts in attachments.items():
        for h in hosts:
            net.add_node(int(h), "host")
    for br, neighbours in topology.items():
        for nb in neighbours:
            net.add_node(int(nb), "bridge")
            net.add_link(int(br), int(nb))
            net.add_link(int(nb), int(br))
    for br, hosts in attachments.items():
        for h in hosts:
            net.add_link(int(h), int(br))
            net.add_link(int(br), int(h))
    streams = mod.set_data_to_schedule()
    for i, s in enumerate(streams):
        if s.src is None or s.dst is None:
            raise ValueError(f"stream {i}: src and dst must be set, "
                             f"e.g. Flow(15, 1000, [], 1000, 2, src=1, dst=7)")
        for end in (s.src, s.dst):
            if net.kind.get(end) != "host":
                raise ValueError(f"stream {i}: {end} is not an end station "
                                 f"of the topology")
    return Escenario(topology, attachments, streams, net,
                     clock_skew=getattr(mod, "CLOCK_SKEW", 0),
                     proc_time=getattr(mod, "SWITCH_MSD", 0),
                     prop_time=getattr(mod, "PROP_TIME", 0))


def candidate_flows(net: Network, stream: Flow) -> List[Flow]:
    """One Flow per simple talker-to-listener path, carrying the stream's
    attributes (the model selects `replica` node-disjoint ones)."""
    return [Flow(stream.trans_time, stream.period, path, stream.e2e_time,
                 stream.replica, src=stream.src, dst=stream.dst,
                 max_jitter=stream.max_jitter)
            for path in net.all_simple_paths(stream.src, stream.dst)]


def write_candidates(candidates: List[Flow], path: str) -> None:
    """Candidate file, the input of incremental_gurobi.py."""
    with open(path, "w") as fh:
        fh.write("from flow import Flow\n\n\n")
        fh.write("def set_data_to_schedule():\n    flow = []\n")
        for c in candidates:
            fh.write(f"    flow.append(Flow({c.trans_time}, {c.period}, "
                     f"{c.nodes}, {c.e2e_time}, {c.replica}, src={c.src}, "
                     f"dst={c.dst}, max_jitter={c.max_jitter}))\n")
        fh.write("    return flow\n")
