# P2P sync benchmarks (Phase 0)

Baseline benchmarks for DefraDB's **existing** full-DAG-walk sync
(`SyncDocuments` → `syncDAG` → `db.Merge`), plus the reusable measurement
harness that later range-based set-reconciliation (Negentropy) phases compare
against. See `.plans/plan-negentropy-set-reconciliation-sync.md` (Phase 0).

These benchmarks start **real libp2p nodes** and are heavier and more
timing-sensitive than the other micro-benchmarks, so they are **skipped unless
explicitly enabled**:

```sh
DEFRA_BENCH_SYNC=1 go test ./tests/bench/sync/ \
    -run '^$' -bench Benchmark_Sync -benchtime=1x -timeout=15m
```

`-benchtime=1x` is recommended: each op starts two nodes and syncs, so a single
iteration per benchmark is the intended unit. Without `DEFRA_BENCH_SYNC` the
package compiles and the benchmarks skip immediately, keeping the default
`go test -bench=.` lane fast and stable.

## Scenarios

The three RFC workloads, parameterised by document count and divergence:

| Benchmark | Shape |
|---|---|
| `Benchmark_Sync_ColdStart_*`        | empty receiver pulls an N-document history (worst case: nothing short-circuits at `IsMerged`) |
| `Benchmark_Sync_IncrementalTail_*`  | receiver shares a base, source gets a small tail (live-update common case) |
| `Benchmark_Sync_ManyHead_*`         | both diverge from a shared base by `k` commits each (post-partition) |

Naming: `Benchmark_Sync_<Scenario>_<shape>`. Sweep the parameters in
`baseline_dagwalk_test.go` for fuller curves.

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
- `harness.go` — node start/connect, and `measuredSync` (event-based completion).
- `metrics.go` — blockstore diff, memory, and `b.ReportMetric` reporting.
- `baseline_dagwalk_test.go` — the benchmarks.

## Baselines

Reference numbers are committed under `baselines/`. Regenerate on a given machine
with the command above and `tee` into a tier-named file, e.g.:

```sh
DEFRA_BENCH_SYNC=1 go test ./tests/bench/sync/ -run '^$' -bench Benchmark_Sync \
    -benchtime=1x -timeout=15m | tee tests/bench/sync/baselines/apple-m2.txt
```

Compare across phases/devices with `benchstat old.txt new.txt`.

## Notes

- Nodes are given generated identities. DocumentACP is enabled by default, so
  block serving authenticates the requesting peer; without an identity the peer
  returns an unparseable token that is never cached, forcing a fresh identity
  round-trip per block that starves transfer under load.
- `SyncDocuments` does not return promptly on completion (it waits out its
  deadline because the internal pending-peer set is keyed by multiaddr but
  cleared by bare peer ID), so completion is detected via merge-complete events.
