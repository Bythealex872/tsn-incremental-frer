# Incremental path selection and scheduling for fault-tolerant TSN

Integer linear programming (ILP) model that adds new streams, one at a time,
to a running Time-Sensitive Network with Time-Aware Shaper (IEEE 802.1Qbv)
gate scheduling and frame replication and elimination for reliability
(FRER, IEEE 802.1CB). The streams already deployed are never modified.

For each new stream the model:

- selects `R` node-disjoint paths, one per member stream (FRER), among the
  candidate talker-to-listener paths;
- places every frame of the hyperperiod in the free intervals (gaps) that
  the deployed schedule leaves on each link, with gate windows widened by
  the clock synchronization error;
- bounds the end-to-end delay of every member stream by the deadline and,
  if requested, the jitter of the stream;
- minimizes the maximum end-to-end delay.

The new transmissions are then assigned to egress queues, keeping the FIFO
order of every queue, and merged into the schedule.

This is the implementation of the model described in:

> A. Gracia, A. G. Torres-Macías, J. Segarra, J. L. Briz, A. Ramírez-Treviño,
> H. Blanco-Alcaine. *Incremental Path Selection and Scheduling in
> Fault-Tolerant Time-Sensitive Networks*. Real-Time Systems (Springer),
> accepted for publication, 2026.

## Requirements

- Python 3.9 or newer.
- [Gurobi](https://www.gurobi.com) and its Python interface `gurobipy`
  (`pip install -r requirements.txt`). Gurobi needs a license; free
  academic licenses are available. The license bundled with the pip package
  only solves small models, enough for the example below.

## Quick start

```bash
pip install -r requirements.txt
python schedule_streams.py examples/escenario_ring.py --outdir out
python check_schedule.py out/schedule.csv --hyperperiod 2000 --skew 1
```

`schedule_streams.py` takes the streams of the escenario in order, as if
they arrived one after another at the network, and writes:

- `out/schedule.csv`: the final schedule (format below);
- `out/streams.csv`: one row per stream with its status (`OPTIMAL`,
  `TIME_LIMIT` with a solution, `INFEASIBLE`, `NO_QUEUE` when no queue
  assignment exists), maximum end-to-end delay, selected paths, solving time
  and the resulting utilization of the bridge-to-bridge links.

A stream that cannot be scheduled is rejected and the schedule stays as it
was. To start from a network that already carries traffic, pass its
schedule with `--schedule-in schedule.csv`.

## Escenario

An escenario is a Python file (see `examples/escenario_ring.py`):

```python
from flow import Flow

CLOCK_SKEW = 1   # µs, clock synchronization error (default 0)
SWITCH_MSD = 4   # µs, processing time per bridge (default 0)
PROP_TIME = 0    # µs, link propagation delay (default 0)

network_topology_example = {      # bridge -> neighbour bridges
    1001: [1002, 1003], 1002: [1001, 1003], 1003: [1001, 1002],
}
listeners_talkers_example = {     # bridge -> attached end stations
    1001: [1, 3], 1002: [1, 2], 1003: [2, 3],
}

def set_data_to_schedule():
    flow = []
    # Flow(transmission time, period, [], deadline, member streams,
    #      src=talker, dst=listener, max_jitter=bound or None)
    flow.append(Flow(15, 1000, [], 1000, 2, src=1, dst=2, max_jitter=50))
    return flow
```

- Bridge ids are greater than 1000 and end-station ids at most 1000. Links
  are bidirectional.
- An end station attached to several bridges can send node-disjoint member
  streams. Two member streams never share a bridge.
- All times are in microseconds. The hyperperiod is the least common
  multiple of the stream periods.

## Jitter bound

With FRER the listener keeps the first copy of each frame, so the jitter it
observes depends on how the member streams fail. A stream with
`max_jitter=J` is scheduled so that its jitter is at most `J` under one of
two assumptions, selected with `--jitter-bound`:

- `worst` (default), intermittent failures: consecutive frames may arrive
  through different member streams, so the jitter is half the spread
  between the largest and the smallest delay over all member streams and
  frames.
- `best`, permanent failures: frames always arrive through the same
  surviving member stream, so the jitter is the largest deviation from the
  mean delay of a single member stream.

## Schedule format

`schedule.csv` has one row per frame and link:

| column | meaning |
|---|---|
| `stream` | stream index in the escenario (after the streams of `--schedule-in`, if given) |
| `replica` | member stream (0 .. R-1) |
| `frame` | frame index within the hyperperiod |
| `node`, `port` | link from `node` to `port` (the egress port of `node`) |
| `queued` | instant the frame enters the egress queue |
| `open`, `close` | gate window, transmission plus the clock error on both sides |
| `complete` | instant the transmission ends |
| `gate` | egress queue |

Instants are folded into the hyperperiod, so `complete` may be smaller than
`queued` when the frame crosses the end of the hyperperiod.

## Scheduling a single stream

The steps of `schedule_streams.py` can also be run by hand:

```bash
# free intervals of the deployed schedule
python generate_gaps.py escenario.py schedule.csv -o gaps.csv
# model for one stream: candidates.py holds one Flow per candidate path
python incremental_gurobi.py candidates.py gaps.csv \
    --schedule-in schedule.csv --csv new_schedule.csv \
    --clock-skew 1 --proc-time 4
```

`schedule_streams.py --keep-steps` writes the candidate, gaps and schedule
files of every step. A candidate path can also be a tree (a multicast
stream with several listeners): list its links from the talker; the frame
then reaches every listener at the same instant.

## Files

| file | content |
|---|---|
| `incremental_gurobi.py` | the ILP model and the single-stream command |
| `queues.py` | egress queue assignment |
| `schedule_streams.py` | schedules every stream of an escenario in order |
| `network.py` | escenario loading and candidate path enumeration |
| `generate_gaps.py` | free intervals of every link of a schedule |
| `check_schedule.py` | independent check of a schedule (windows, transmissions, FIFO order) |
| `flow.py` | stream description |

## License

Copyright (C) 2026 Alex Gracia.

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version. See [LICENSE](LICENSE).

Additional permission under GNU GPL version 3 section 7: if you modify
this Program, or any covered work, by linking or combining it with the
Gurobi Optimizer or its Python interface gurobipy (or a modified version
of them), containing parts covered by the terms of the Gurobi license, the
licensors of this Program grant you additional permission to convey the
resulting work.
