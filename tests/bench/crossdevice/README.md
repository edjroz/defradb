# Cross-device set-reconciliation benchmark

A standalone tool that runs the **default-sync vs range-based set-reconciliation
("ranges")** contrast across an **N-node network with a configurable topology** —
the nodes may be separate processes on one machine or separate physical devices
over a real network — and records per-node control bytes, payload, and wall-clock
to CSV.

It lives outside the `go test` lane because a benchmark cannot start and drive
nodes on *other machines*. It is a benchmark tool, not a product binary: the node
(`cmd/benchnode`) has no auth and binds wherever told — run it only on trusted
benchmark hosts.

## Why a bespoke node instead of the `defradb` CLI

The whole case for reconciliation is **fewer coordination bytes/round-trips**. A
stock `defradb` binary exposes none of that: there is no per-protocol byte counter,
no libp2p bandwidth reporter wired onto the host, and no `/metrics` endpoint (only
`/health-check` + `/openapi.json`). A script driving the shipping CLI could capture
only wall-clock + on-disk size — it would miss the metric that matters.

So `cmd/benchnode` builds a real node **exactly** the way the in-process harness
does (`tests/bench/sync/harness.go`), wrapping the libp2p host in the same
`CountingHost` decorator (`tests/bench/sync.Decorator`), and exposes a thin HTTP
control API. The per-protocol counters it reports are therefore identical in shape
to the in-process benchmark — but now sourced from separate devices.

## How it works: placement × topology × scenario

Three orthogonal inputs, so new shapes are added by config, not code:

| Input | Flag | Values |
|---|---|---|
| **Placement** — where nodes run | `--devices` | `local:N` (spawn N locally) · `ssh:hostA,hostB,...` (launch one node per host over SSH) · `node:host:port,...` (connect to bench-nodes you already started) |
| **Topology** — the graph they form | `--topology` / `--topology-file` | `pair` `line` `ring` `star` `mesh` `tree:<fanout>` · or a `{nodes,edges,seeds}` JSON |
| **Scenario** — how data diverges | `--scenario` | `cold-start` `tiny-diff` `multi-writer` |

Nodes use **control port 17700** and **p2p port 9300**. (Control is deliberately
*not* 7000/7100 — macOS AirPlay Receiver / Control Center owns those, and a node
trying to bind 7000 fails with "address already in use".)

The node count comes from the topology; placement supplies that many endpoints.
The orchestrator (`run.py`) wires the nodes into the topology, applies the
scenario's seeds, then **drives anti-entropy rounds to a fixpoint** — repeating
reconcile (ranges) or `SyncDocuments` (default) over the edges until every node's
`/blockstats` reports an identical `(blocks, blockBytes)`. After full convergence
every node holds the same block set, so that equality is an identity-free
**correctness + convergence** signal (it also bounds the round count, which is
itself reported).

### Adding a topology

Add one generator branch to `topology(name, n)` in `run.py` returning
`(node_count, undirected_edges)`, or pass a `--topology-file` graph. Nothing else
changes — the node and the drive engine are topology-agnostic.

## Running

```sh
# build the node (run.py builds it automatically if --binary is omitted)
go build -o tests/bench/crossdevice/.bin/benchnode ./tests/bench/crossdevice/cmd/benchnode

# the committed local smoke runs (both modes each):
python3 tests/bench/crossdevice/run.py --devices local:2 --topology pair  --scenario tiny-diff    --docs 50
python3 tests/bench/crossdevice/run.py --devices local:3 --topology line  --scenario multi-writer --docs 30
```

Each run writes `results/<topology>-n<N>-<scenario>-docs<D>.csv` (one row per node)
and prints a summary comparing the modes.

### Across real devices

Prerequisites on each device: passwordless SSH from the orchestrator host, the
`benchnode` binary at the same path on every device (build natively per device, or
cross-compile + `scp`), and the control (17700) + p2p (9300) ports reachable. On
macOS there is no per-port firewall — just make sure the **Application Firewall**
(System Settings → Network → Firewall) isn't blocking `benchnode`; on a same-LAN
setup with the firewall off there is nothing to open. Each node must be **awake**
(an asleep Mac answers ping/ARP but drops SSH/TCP).

```sh
# build per device (same path on each), e.g. on each host:
#   git checkout feat/... && go build -o ~/benchnode ./tests/bench/crossdevice/cmd/benchnode

# auto-launch over SSH (node count comes from the topology; supply that many hosts):
python3 tests/bench/crossdevice/run.py \
    --devices ssh:user@deviceA,user@deviceB,user@deviceC \
    --topology line --scenario tiny-diff --docs 200 --binary ~/benchnode
```

Each mode (`ranges`, `default`) needs its node set started with the matching
`--reconciliation` flag; in `ssh` mode `run.py` handles that automatically.

**`node:` mode (pre-launched).** When you'd rather manage node lifecycles yourself
(or the orchestrator can't reliably SSH-launch in your environment), start the
bench-nodes by hand and point `run.py` at their control endpoints — it then only
talks HTTP, no SSH:

```sh
# start a node on each host with the flag for the mode you're measuring:
#   ranges  -> ~/benchnode --control 0.0.0.0:17700 --p2p /ip4/0.0.0.0/tcp/9300 --store memory --reconciliation
#   default -> same, WITHOUT --reconciliation  (a reconciliation node auto-reconciles on connect)
python3 tests/bench/crossdevice/run.py \
    --devices node:hostA:17700,hostB:17700 \
    --topology pair --scenario tiny-diff --docs 50 --modes ranges    # then relaunch w/o the flag and --modes default
```

The scope here is **ARM-only** (Apple-Silicon devices); x86_64 is deferred to the
team. The orchestrator code path is identical across placements — only provisioning
differs.

## CSV columns

`topology, N, scenario, mode, nodeID, ctrlBytesSent, ctrlBytesRecv, ctrlMsgs,
blocks, blockBytes, wallMs, rounds, converged`

## Reading the numbers (important)

- **`ranges` wins on the steady-state regime** — a large shared base with a small
  divergence (`tiny-diff`). The committed `pair tiny-diff docs50` run shows ranges
  at ~2× **fewer** control bytes than default, because default's `SyncDocuments`
  must enumerate *every* docID to catch one change while reconciliation prunes the
  matching majority. This is the headline.
- **`ranges` does not win on full cold transfer** — `cold-start` and
  `multi-writer` (disjoint full shards, no shared base) have nothing to prune, so
  the per-session range-splitting overhead shows. Both modes still **converge
  correctly** (the point of those runs is topology/multi-hop convergence), but
  default moves fewer control bytes there. This matches the in-process M2 data.
- **`ranges` control bytes here include bidirectional-push tip blocks.** The manual
  `ReconcileCollection` pushes the have-set tips over the control protocol, so a
  few block bytes land in `ctrlBytes` that the in-process *receiver-pull*
  benchmarks (`tests/bench/sync`) exclude. Treat the in-process numbers as the
  clean control-byte measurement and this tool as the **cross-device convergence +
  order-of-magnitude** validation.
- **`wallMs` for `default` is settle-influenced** on loopback (`SyncDocuments` is
  async; each round waits `--settle`). On a real high-latency network it reflects
  round-trips, where reconciliation's fewer rounds matter more. `ctrlBytes` /
  `ctrlMsgs` are the device-independent metrics.

## Files

- `cmd/benchnode/main.go` — the instrumented node + HTTP control API.
- `run.py` — the topology-driven orchestrator (stdlib only, like `plot.py`).
- `results/` — committed local smoke-run CSVs.
