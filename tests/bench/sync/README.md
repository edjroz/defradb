# P2P sync benchmarks

Benchmarks for DefraDB's **existing** full-DAG-walk sync (`SyncDocuments` →
`syncDAG` → `db.Merge`) and the **range-based set-reconciliation** (Negentropy)
contrasts, sharing one measurement harness so the two converge paths are
compared like-for-like. See `.plans/plan-negentropy-set-reconciliation-sync.md`.

These benchmarks start **real libp2p nodes** and are heavier and more
timing-sensitive than the other micro-benchmarks, so they are **skipped unless
explicitly enabled**:

```sh
DEFRA_BENCH_SYNC=1 go test ./tests/bench/sync/ \
    -run '^$' -bench 'Benchmark_(Sync|Reconcile|LocalWrite)' -benchtime=1x -timeout=30m
```

or `make test:bench-sync` (and `make test:bench-sync-save DEVICE=<label>` to save
a per-device baseline — see [Baselines](#baselines)).

`-benchtime=1x` is recommended: each op starts two nodes and syncs, so a single
iteration per benchmark is the intended unit. Without `DEFRA_BENCH_SYNC` the
package compiles and the benchmarks skip immediately, keeping the default
`go test -bench=.` lane fast and stable.

## Scenarios

Default-sync baselines (the three RFC workloads), parameterised by document count
and divergence:

| Benchmark | Shape |
|---|---|
| `Benchmark_Sync_ColdStart_*`        | empty receiver pulls an N-document history (worst case: nothing short-circuits at `IsMerged`) |
| `Benchmark_Sync_IncrementalTail_*`  | receiver shares a base, source gets a small tail (live-update common case) |
| `Benchmark_Sync_ManyHead_*`         | both diverge from a shared base by `k` commits each (post-partition) |
| `Benchmark_Sync_OneDocChanged_*`    | large shared base, exactly one doc diverged — default contrast to the reconcile win |

Set-reconciliation contrasts (each pairs with a `Sync` shape above; the receiver
runs with the flag on and converges via reconciliation instead of broadcast):

| Benchmark | Shape |
|---|---|
| `Benchmark_Reconcile_ManyHead_*` / `_SingleDoc_*` | per-document head reconciliation (M1 — per-session overhead, not the asymptotic win) |
| `Benchmark_Reconcile_Collection_ColdStart_*`      | empty receiver pulls a whole collection in one session (M2) |
| `Benchmark_Reconcile_Collection_Tail_*`           | shared base, small tail across all docs |
| `Benchmark_Reconcile_Collection_OneDocChanged_*`  | large shared base, one diverged doc — the asymptotic bandwidth win (control ∝ diff, not collection size) |

The `OneDocChanged` pair is the headline contrast: at `docs50` the default
doc-list is still small, but by `docs200` the reconcile control bytes stay flat
(log-scaling) while the default's grow with collection size — the crossover.

Local-write cost (no peer): the always-on maintenance-hook overhead.

| Benchmark | Shape |
|---|---|
| `Benchmark_LocalWrite_ReconcileOff_*` / `_ReconcileOn_*` | seed N docs with the flag off vs on; the ns/op delta is the per-commit ordered-index write |

Sweep the parameters in `baseline_dagwalk_test.go` / `reconcile_bench_test.go` /
`local_write_bench_test.go` for fuller curves.

## Metrics (per op)

| Metric | Meaning | Source |
|---|---|---|
| `ns/op` / `syncMs/op` | wall-clock from issuing the sync to the last merge | merge-complete events |
| `blocks/op`           | blocks added to the receiver's blockstore | blockstore CID diff |
| `blockBytes/op`       | payload bytes added to the receiver's blockstore | blockstore CID diff |
| `ctrlBytes/op`        | control-message bytes (doc-sync / identity) seen by the receiver | `CountingHost` |
| `ctrlMsgs/op`         | control messages sent+received (round-trip proxy) | `CountingHost` |
| `heapDeltaMiB/op`     | heap allocation delta around the sync | `runtime.MemStats` |

Payload (blocks) is measured via the receiver's blockstore growth rather than by
instrumenting the content-addressed fetch path (whose reads mix local and remote
fetches). Control/coordination traffic is measured by the `CountingHost`, which
wraps the libp2p host through the `options.NodeP2P().SetHostDecorator(...)` seam.

## Harness pieces

- `counting_host.go` — `node.Peer` wrapper counting per-protocol wire traffic.
- `dag_shapes.go` — schema and DAG-shape seed helpers (`seedDocs`, `applyTail`).
- `harness.go` — node start/connect, `withReconciliation`, the reconcile drivers,
  and `measuredSync` (event-based completion).
- `metrics.go` — blockstore diff, memory, and `b.ReportMetric` reporting.
- `baseline_dagwalk_test.go` — the default-sync baselines + `OneDocChanged` contrast.
- `reconcile_bench_test.go` — the set-reconciliation contrasts.
- `local_write_bench_test.go` — the index-maintenance write-cost benchmarks.

## Baselines

Reference numbers are committed under `baselines/<device>.txt`. Regenerate on a
given machine with:

```sh
make test:bench-sync-save DEVICE=apple-m4-pro
# → tests/bench/sync/baselines/apple-m4-pro.txt
```

The target compiles the test binary and runs it directly rather than via `go
test`: `go test` merges the test binary's stderr into its own stdout, which would
splice node logs (corelog → stderr) into the benchmark result lines. Running the
binary keeps the streams separate — node logs go to `baselines/<device>.stderr.log`
and only result lines land in the `.txt`.

`DEVICE` defaults to `uname -m`. `make deps:bench` installs `benchstat`; compare
across phases/devices (an ARM tier matrix — there is no x86 hardware in scope):

```sh
benchstat tests/bench/sync/baselines/apple-m2.txt tests/bench/sync/baselines/apple-m4-pro.txt
```

The existing `apple-m2.txt` is the stale Phase-0 anchor (default-sync only);
regenerate the full suite per device.

## Notes

- Nodes are given generated identities. DocumentACP is enabled by default, so
  block serving authenticates the requesting peer; without an identity the peer
  returns an unparseable token that is never cached, forcing a fresh identity
  round-trip per block that starves transfer under load.
- `SyncDocuments` does not return promptly on completion (it waits out its
  deadline because the internal pending-peer set is keyed by multiaddr but
  cleared by bare peer ID), so completion is detected via merge-complete events.
