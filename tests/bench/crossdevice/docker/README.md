# Dockerised multi-node benchmark (single host)

Run the cross-device set-reconciliation benchmark as **N benchnode containers on one
machine**, on a single private bridge network. This gives deterministic, minimal
networking — ideal for observing each node's growth and nailing down correctness —
without the flakiness of a real LAN. It does **not** replace true cross-hardware runs
(still needed for the Phase 5 weaker-ARM gate), but it is the right tool for iterating
on correctness and watching block/state growth.

The orchestrator (`../run.py`) is unchanged: it runs on the **host** in `node:` mode and
drives the containers over their published control ports. The libp2p p2p port stays
internal to the bridge.

## Prerequisites
- Docker Desktop (Apple-Silicon builds linux/arm64 images natively).
- Python 3 on the host (for `run.py`).

## Quick start (the pair)

```sh
# from tests/bench/crossdevice/docker/
./compose-run.sh ranges  tiny-diff 50    # reconciliation ON,  one pass
./compose-run.sh default tiny-diff 50    # reconciliation OFF, baseline pass
```

Each invocation builds the image (cached after the first), recreates the stack fresh
with the matching `--reconciliation`, runs one mode, writes
`../results/docker-pair-<scenario>-docs<docs>-<mode>.csv`, and tears down. Results carry
the new **`stateMatch`** column (see below).

To build/drive manually instead of the wrapper:

```sh
# build from the REPO ROOT (the Go module needs the full tree):
docker build -f tests/bench/crossdevice/docker/Dockerfile -t benchnode:local .

# ranges pass:
RECON=--reconciliation docker compose -f docker-compose.yml up -d --wait
python3 ../run.py --devices node:127.0.0.1:17701,127.0.0.1:17702 \
    --topology pair --scenario tiny-diff --docs 50 --modes ranges
docker compose -f docker-compose.yml down -v        # clears state between passes
```

## Two things that will bite if changed
1. **p2p must bind `0.0.0.0`** (`--p2p /ip4/0.0.0.0/tcp/9300`). That is the only lever to
   make libp2p advertise the container's routable bridge IP (go-p2p has no announce knob).
   Binding loopback would advertise `127.0.0.1` only and break cross-container dialing.
2. **Re-create the stack between modes** (`down -v` + `up`). A node carries one mode for its
   lifetime, and `--store badger=/data` persists in a named volume — `down -v` clears it so
   each pass starts empty and the byte comparison is meaningful. (Switch to `--store memory`
   in the compose command if you'd rather reset with a plain restart and drop the volumes.)

## The `stateMatch` step (document-level correctness)

After convergence is detected, `run.py` now calls `assert_shared_state(nodes)`: it queries
every node's documents via the new bench-node `/query` endpoint, normalises the result
order-independently (keyed by `_docID`), and **asserts every node resolves the identical
document set**. This is stronger than the block-count equality of `/blockstats` (same blocks
present does not by itself prove the same documents resolve). On any divergence it raises with
the offending docIDs; on success each CSV row records `stateMatch=True`.

## Observing growth

In a second terminal, while a run is in flight:

```sh
./monitor.sh growth.csv 2      # samples blocks/bytes + container CPU/mem every 2s
```

## Ports & layout
- Control API: `node0 -> 127.0.0.1:17701`, `node1 -> 127.0.0.1:17702` (host-loopback bound).
- p2p `9300`: **not** published — containers reach each other over the bridge.

## Extending to N nodes
Copy a `nodeN` service block in `docker-compose.yml` (publish `1770(N+1):17700`, add a
`nodeN-data` volume), add its endpoint to the `--devices node:...` list, and pick a topology
that uses `--nodes` (e.g. `--topology line --nodes 3`).

## Simulating a weaker device
Add a resource cap to a service to exercise the Phase 5 criterion-3 concern (does the
per-session O(n) tree build dominate on weak hardware?):

```yaml
  node1:
    cpus: "0.5"
    mem_limit: 512m
```

## Files
- `Dockerfile` — multi-stage CGO build (golang:1.25 → debian-bookworm-slim). CGO is required
  because the node's lens runtime is wasmtime; a static/musl (alpine) image will not work.
- `docker-compose.yml` — the two-node pair on one bridge, badger volumes, healthchecks.
- `compose-run.sh` — build + recreate + orchestrate + teardown, one mode per call.
- `monitor.sh` — host-side growth sampler.
