# Copyright (C) 2026 Alex Gracia
# SPDX-License-Identifier: GPL-3.0-or-later
"""Example escenario: a ring of 12 bridges (1001-1012) with 12 end stations.

End station n is attached to bridges 1000+n and 1000+(n mod 12)+1, so every
stream can use up to two node-disjoint member streams (FRER).
Run:  python schedule_streams.py examples/escenario_ring.py --outdir out
"""

from flow import Flow

CLOCK_SKEW = 1   # µs, clock synchronization error
SWITCH_MSD = 4   # µs, processing time per bridge
PROP_TIME = 0    # µs, link propagation delay

network_topology_ring = {
    1000 + i: [1000 + (i % 12) + 1, 1000 + ((i - 2) % 12) + 1]
    for i in range(1, 13)
}

listeners_talkers_ring = {1000 + i: [] for i in range(1, 13)}
for es in range(1, 13):
    listeners_talkers_ring[1000 + es].append(es)
    listeners_talkers_ring[1000 + (es % 12) + 1].append(es)


def set_data_to_schedule():
    # Flow(transmission time, period, [], deadline, member streams,
    #      src=talker, dst=listener, max_jitter=bound or None); times in µs
    flow = []
    flow.append(Flow(15, 1000, [], 1000, 2, src=1, dst=7))
    flow.append(Flow(15, 500, [], 500, 2, src=4, dst=10, max_jitter=20))
    flow.append(Flow(15, 2000, [], 2000, 1, src=2, dst=5))
    flow.append(Flow(15, 1000, [], 800, 2, src=3, dst=9, max_jitter=50))
    flow.append(Flow(15, 500, [], 500, 1, src=6, dst=12))
    flow.append(Flow(15, 2000, [], 1000, 2, src=11, dst=8))
    return flow
