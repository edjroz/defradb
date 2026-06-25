# Phased Implementation Plan — Range-Based Set Reconciliation Sync (Negentropy)

> Companion to `.plans/rfc-negentropy-set-reconciliation-sync.md`. Turns the RFC's M0–M3
> milestones into a benchmark-first, default-off, heads-first rollout. ACP is documented (Phase 7)
> but not built.

## Context

DefraDB peer sync walks the **entire** Merkle-DAG of a document/collection on catch-up
(`internal/db/p2p/sync_dag.go:41` `syncDAG` → `loadBlockLinks`, stopping only at `IsMerged`
blocks). Cost is proportional to total history, not to the difference between two peers — painful for
cold-start, post-partition many-head divergence, and intermittent peers (RFC §2).

The RFC proposes an **additive** anti-entropy layer using Negentropy range-based set reconciliation:
peers exchange associative range fingerprints and descend only into disagreeing ranges, making
*discovery of the divergent subset* `O(diff · log n)`. Reconciliation is **discovery only**; fetch +
causal merge reuse the existing `syncDAG`/`db.Merge` machinery unchanged.

This plan applies four constraints:
1. The feature is **configurable and disabled by default**.
2. **Phase 0 is comprehensive benchmarking** — built *before* feature code, establishing a baseline of
   the existing DAG-walk sync. Phase 0 is **local-only**.
3. **Cross-device benchmarking is a hard gate before final delivery** (Apple Silicon laptop tier vs
   the existing x86_64 CI anchor).
4. **ACP is out of scope** now but is a **documented future phase** (per RFC §10.1).

Decisions locked: **heads-first MVP** (RFC M1 — proves the protocol end-to-end; does **not** fix
cold-start, which lands in the full-set phase); **plain bool** flag (no generic feature-flag framework
exists in the repo — only build tags, a `development` bool, and the `EnablePubSub`-style P2P option
bools, which is the precedent we follow); cross-device tier = **Apple Silicon laptop**.

---

## Phase 0 — Benchmark & measurement harness (LOCAL-ONLY) — *precedes all feature code*

**Goal.** Build the byte/round-trip/compute measurement apparatus and capture a **baseline** of the
existing full-DAG-walk sync across the three RFC workloads, so every later phase has a comparison
target. No feature code.

**Why first.** There is currently **no P2P/network/sync benchmark and no byte-counting instrumentation
anywhere** in the repo. Without this, the later `O(diff)` claims are unfalsifiable.

**Deliverables** — new package `tests/bench/sync/`:
- `counting_host.go` — a `client.Host` wrapper (the interface at `client/p2p.go`) that embeds the real
  host and counts, **per protocol-ID and direction**: bytes sent/received and round-trips on `Send`
  and registered stream handlers; broadcast bytes on the pubsub publish methods; and **block-fetch
  bytes** by wrapping `IPLDStore()`'s reader (the baseline's dominant cost is block fetching via
  bitswap, not control messages). Atomic counters with `Snapshot()`/`Reset()`. Because it satisfies
  `client.Host`, it drops into `internal/db/p2p.New(...)` and the integration harness with zero
  production changes.
- `dag_shapes.go` — DAG-shape seed helpers parameterized by `N` (total commits) and `k` (divergence),
  built on `tests/bench/bench_util.go` (`BackfillBenchmarkDB`, `NewTestDB`) and `tests/bench/fixtures`:
  - **cold-start**: node B has `N`-commit history, node A empty (`seedLinearHistory`).
  - **post-partition many-head**: both nodes backfilled to `N`, then `k` independent updates per side
    without syncing (`seedDivergentHeads`).
  - **incremental tail**: shared `base`, B gets `k` more (`seedTail`) — the live-update common case.
- `baseline_dagwalk_test.go` — drives the **existing** sync path (two-node real-libp2p harness from
  `tests/integration/p2p_config.go` `RandomNetworkingConfig` + actions `AddDoc`/`ConnectPeers`/
  `SyncDocs`/`WaitForSync`) for all three scenarios, recording every metric below.
- `metrics.go` — maps captured counters to `b.ReportMetric` for `go test -bench` cases, plus a JSON
  emitter for multi-node cases that don't fit the standard bench model.
- `Makefile` target wired into top-level `make test:bench` / `test:bench-short`; `README.md`.

**Metrics captured (comprehensive):** bytes exchanged (control + blocks, per protocol), round-trips,
wall-clock (ns/op), peak memory (`runtime.ReadMemStats` delta → `peakMiB`), CPU/fingerprint-compute
time (`fpµs`, 0 in baseline), per-block aux-index write overhead (`idxBytes/blk`, `idxµs/blk`, 0 in
baseline). Naming: `Benchmark_Sync_<Scenario>_<N>_<k>` so **benchstat** groups cleanly.

**Reporting/CI.** Add the package to the bench run set in
`.github/workflows/lint-then-benchmark.yml` (benchstat HTML diff already produced for `go test -bench`
cases). Archive multi-node JSON as CI artifacts; commit a baseline under
`tests/bench/sync/baselines/`.

**Exit criteria.** Reproducible baseline numbers (variance bounded on re-run) for cold-start,
many-head, and incremental-tail; CI publishes them. **Local-only.**

---

## Phase 1 — Default-off config flag + gated protocol scaffold

**Goal.** Land the flag (disabled by default) and a gated, no-op protocol registration so all later
feature code sits behind one switch from day one. Flag off ⇒ behavior byte-for-byte identical to today.

**Config chain (plain bool, mirrors `EnablePubSub`):**
| Hop | File | Change |
|---|---|---|
| Field | `client/options/node.go` `NodeP2POptions` (~L101) | add `EnableSetReconciliation bool` |
| Builder | `client/options/node.go` (~L526) | add `SetEnableSetReconciliation(bool)` |
| CLI flag | `cli/start.go` (~L302) | `set-reconciliation` bool flag, "experimental, default off" |
| CLI→opts | `cli/start.go` (~L104) | `.SetEnableSetReconciliation(cfg.GetBool("net.setReconciliationEnabled"))` |
| Viper key | `cli/config/config.go` `ConfigFlags` (~L74) | `"set-reconciliation": "net.setreconciliationenabled"` |
| Default | `cli/config/config.go` `ConfigDefaults` (~L92) | `"net.setReconciliationEnabled": false` |

**Reach into `p2p.New`** (the registration point at `internal/db/p2p/p2p.go:195` is below go-p2p's
option layer, so mirror the **`P2PBlockSyncTimeout`** threading, not `EnablePubSub`'s): add a
`P2PSetReconciliationEnabled() bool` method to the DB interface used in `internal/db/p2p/p2p.go`
(next to `P2PBlockSyncTimeout()` ~L98), defaulting false in `internal/db/config.go`.

**Gating point.** In `internal/db/p2p/p2p.go` `New()` after L195, **only when the flag is true**:
`p.reconcileProtocol = protocol.NewCommChannel(host, "reconcile", &reconcileCommProcessor{p2p:&p})`
→ endpoints `/defradb/reconcile_req|resp/0.0.1`. Off ⇒ handlers never registered ⇒ disabled/old peers
return libp2p "protocol not supported" and fall back to pushlog/head-broadcast (RFC §6.4).

**Exit criteria.** Flag defaults false; full suite green; unit test asserts reconcile stream handlers
are **absent** when off and **present** when on; old-peer fallback test. **Local-only.**

---

## Phase 2 (RFC M0) — Reconciler core, no network

**Goal.** Pure Negentropy engine — unit/property-testable in isolation, no DefraDB net deps.

**Deliverables** — new package `internal/db/p2p/negentropy/`:
- `sortkey.go` — `sortKey = heightBE8 || cidBytes` (RFC §4.4); heads path reuses the CID order already
  produced by `internal/core/block/heads.go:List`.
- `fingerprint.go` — `fp = Hash( (Σ SHA256(cidBytes)) mod 2^256 || uvarint(count) )` truncated to 16B;
  associative + commutative so adjacent ranges combine.
- `reconciler.go` — stateless responder + initiator state machine (Skip / Fingerprint / IdList) with
  hard caps (rounds, ranges/message, id-list entries/range) baked in from the start.
- Property tests (RFC §7.1–7.3): `fp(A∪B)` combinability, order-independence, empty-range identity,
  total-order determinism; convergence-vs-symmetric-difference over an in-memory **counting** transport
  asserting bytes scale with `|A△B|` not `|A∪B|`; sweep diff ∈ {0,1,k,n}.
- **Fingerprint-compute micro-benchmark** (feeds the scaling concern): compute time vs `n`, to later
  contrast on-demand scan vs maintained tree.

**Exit criteria.** Property tests green; counting transport confirms `O(diff)` bandwidth in isolation.
**Local-only.**

---

## Phase 3 (RFC M1) — Per-document **heads** reconciliation — **the MVP**

**Goal.** End-to-end opt-in reconciliation of a single document's heads between two peers; need-set fed
into the **existing** fetch+merge. (Explicitly does **not** fix cold-start — RFC §4.2; full-set is
Phase 4.)

**Deliverables:**
- `internal/db/p2p/protocol/reconcile.go` — `ReconcileMessage{MetaData; Scope; Ranges}` / `Range` CBOR
  types (pattern: `protocol/pushlog.go:18-31`); handler registration (pattern: `protocol/identity.go`,
  `protocol/comm_channel.go:73-74`).
- `internal/db/p2p/reconcile.go` — session driver + inbound handler + "need-set → fetch → merge" glue,
  mirroring `sync_doc.go`. Reconciled-set source = `coreblock.NewHeadSet(...).List`
  (`heads.go:33,76`; call-site shape `sync_doc.go:313-315`). Discovered heads handed to
  `syncDocumentDAG → syncDAG` (`sync_doc.go:262`, `sync_dag.go:41`) then `db.Merge` (`p2p.go:600`);
  the `IsMerged` short-circuit (`sync_dag.go:68`) keeps the per-head walk diff-proportional.
- **Session deadline + per-message timeout** — fixes the `context.Background()` gap at
  `comm_channel.go:99` *as the protocol first goes on the wire* (do not defer to hardening).
- Manual API `ReconcileDocument(ctx, peerID, docID)` on `P2P`, surfaced on `client.P2P` (then
  http handler/client + a `cli/p2p_reconcile.go` command, following `cli/p2p_info.go`).
- Integration tests in `tests/integration/net/sync/reconcile/` (next to `collection_version`,
  `branchable_collection`) + a `tests/action/sync_reconcile.go` action (mirror
  `sync_branchable_collection.go`).

**Exit criteria.** Two divergent nodes (A=N, B=N−k) converge to identical heads via reconciliation;
Phase-0 counting Host shows bytes ∝ k (RFC §7.4). Oracle test: result set equals what existing
`syncDAG` converges to (RFC §7.3). Adversarial round-cap + oversize id-list rejected by 16 MiB cap
(RFC §7.5). Re-run Phase-0 incremental-tail and many-head benches **flag ON**, compare to baseline.
Pushlog untouched. **Local benches OK; cross-device deferred to its gate.**

---

## Phase 4 (RFC M2) — Per-collection **full-block-set** + maintained ordered index

**Goal.** True `O(diff)` cold-start: reconcile the full per-scope block-CID set, fetch missing blocks
directly, topo-merge. This is what actually removes the DAG walk (RFC §11).

**Deliverables:**
- New ordered-index store namespace alongside `headStoreKey`/`blockStoreKey`
  (`internal/datastore/multi.go:31-32`) with an accessor like `Headstore()` (`multi.go:71`), keyed
  per-scope by `sortKey`.
- **Maintained Fenwick/segment-tree fingerprint index — FIRM requirement, not an open question.**
  Rationale (from the scaling analysis): discovery bandwidth `O(diff·log n)` is fine, but the real
  over-time cost is **`O(n)` fingerprint *compute* per session** unless a tree of partial sums is
  maintained, giving `O(log n)` fingerprints + `O(diff·log n)` session compute. Update the index **in
  the same merge txn** that persists blocks (`db.Merge`, `p2p.go:600`) so it cannot drift on crash;
  provide a derive-on-read/rebuild fallback.
- Direct missing-block fetch via the same `linkSys.Load`/block-service session as `loadBlockLinks`,
  **preserving signature verification** (`sync_dag.go:78-86`); **topologically sort by CRDT height
  before `db.Merge`** (RFC §4.3, §6.1).
- Trigger on `peerEventHandler` connect (`p2p.go:531`).
- **Scope to public/policy-free collections and configured replicator pairs only** (RFC §10.1.4) —
  this hardcoded restriction sidesteps the per-peer fingerprint/ACP-leak problem until the ACP phase.
- Caps: max blocks fetched/session, links/block on fetch (DAG fan-out-bomb mitigation, RFC §10.2).

**Exit criteria.** Cold-start (empty vs N-history) converges; **discovery** bytes ∝ `diff·log n`;
per-block aux-index overhead measured within budget (~40–70 B/block, RFC §6.2); crash/rebuild
consistency test; topo-merge correctness. Phase-0 `fpµs` metric confirms `O(log n)` compute with the
maintained tree. **Not "delivered" until the Phase-5 cross-device gate signs off.**

---

## Phase 5 — Cross-device benchmark gate (REQUIRED before final delivery)

**Goal.** Validate the `O(n)`-compute and aux-index-write concerns on the target laptop tier — costs
that hide on a powerful CI machine but can dominate elsewhere.

**Setup.** Re-run the Phase-0 harness (counting Host + JSON emitter, unchanged) on **two tiers**:
- **Apple Silicon laptop** (user-selected target tier).
- **x86_64 CI runner** (the existing `lint-then-benchmark.yml` EC2 anchor) as the comparison baseline.

Capture fingerprint-compute time, peak memory, and aux-index write latency on the laptop tier; confirm
the maintained tree keeps per-session compute sub-linear and within agreed bounds. (SBC and
WAN-latency shaping are noted as optional future extensions, not in scope now.)

**Exit criteria.** On the laptop tier, full-set reconciliation compute + memory within bounds; no
regression vs heads-only on incremental-tail. Sign-off recorded as the gate to Phase 6. **Cross-device
required.**

---

## Phase 6 (RFC M3) — Scheduling & hardening

**Goal.** Production-readiness; still default-off (flipping the default is a separate later decision).

**Deliverables.** Configurable periodic anti-entropy scheduler over shared scopes; full adversarial
hardening (RFC §10.2: session deadline + all caps, stateless-responder invariant enforced, peer
scoring/backoff, libp2p connmgr + resource-manager config — currently unset); metrics surfaced;
optionally route `sync_doc.go`/`sync_branchable_col.go` through reconciliation.

**Exit criteria.** Adversarial suite green (round-amplification, bogus-CID flood, fan-out bomb, Sybil
within trust-scoping); scheduler interval configurable; metrics emitted; cross-device suite stable.

---

## Phase 7 (FUTURE — DOCUMENTED ONLY, NOT BUILT) — ACP-aware reconciliation

Reconciliation moves the trust decision **earlier** than today's fetch-time `hasAccess`
(`p2p.go:341`, wired as bitswap's `PeerBlockRequestFilter`): fingerprints/id-lists reveal set
membership **before** any fetch, so a naive build is a free enumeration oracle. The MVP avoids this by
the Phase-4 public/replicator-only scoping. This future phase lifts that restriction (per RFC §10.1):

1. **Identity handshake = round 0** — run `IdentityProtocol.GetIdentity` (`protocol/identity.go:65-98`,
   JWT bound to requester, verified via `VerifyAuthToken` `p2p.go:433`) **before** emitting any
   fingerprint; unauthenticated peers get only the public set.
2. **Per-identity filtered fingerprints** — filter CIDs through `CheckDocAccessWithIdentityFunc`
   (`DocumentReadPerm`, `acp/types/types.go:42`) **before** they enter the reconciler. Invariant: no
   fingerprint/id-list includes a CID the identity can't read. This makes fingerprints per-identity
   (defeats single-fingerprint-per-range caching).
3. **Scope enum** — promote the plain bool to `off / replicators / public / all`; `all` requires the
   full per-identity filtering of this phase.
4. **Integration points that change** — `reconcile.go` gains the round-0 exchange + ACP filter step;
   the Phase-4 full-set scope is redefined as "per-collection over the requester's readable subset";
   the maintained index must support per-identity views or recompute over the filtered subset.
   Encryption stays orthogonal (encrypted-but-public reconciles fine; only ACP-gated docs are filtered).
5. **Residual side channel (accepted for that version):** which ranges split + session timing leak
   coarse gated-set size info even with filtering.

---

## Sequencing risks

- **Benchmark-before-code is load-bearing** — treat the counting Host + baseline as a gate; rushing
  Phase 0 makes every later `O(diff)` claim unverifiable.
- **Maintained tree is firm** — do not ship full-set scope (Phase 4) with an `O(n)`-scan fingerprint;
  Phase 5 exists specifically to catch this on the laptop tier.
- **Aux-index transactional consistency** (RFC §6.2) — write in the same merge txn or it drifts on
  crash; verify with a crash/rebuild test + derive-on-read fallback.
- **Causal ordering** (RFC §4.3) — full-set fetch must topo-sort by height before `db.Merge` and
  preserve signature verification, or CRDT merge may misvalidate.
- **Adversarial surface lands at Phase 3**, not Phase 6 — the `context.Background()` timeout gap and
  basic caps must be addressed when the protocol first goes on the wire.
- **Default-off discipline** — every phase keeps the flag-off path byte-identical; keep the
  handler-absence regression test from Phase 1 green throughout.

---

## Verification (per phase)

- **Phase 0:** re-run variance bounded; baselines archived; CI benchstat wired.
- **Phase 1:** handlers present iff flag on; old-peer fallback; full suite green.
- **Phase 2:** property tests (fingerprint algebra, total order); convergence-vs-`|A△B|` on counting
  transport (RFC §7.1–7.3).
- **Phase 3:** two-node convergence + bytes∝k via counting Host (§7.4); oracle vs `syncDAG` (§7.3);
  round-cap + 16 MiB id-list (§7.5); Phase-0 benches flag-on vs baseline.
- **Phase 4:** cold-start convergence + aux-index overhead measured; crash/rebuild consistency;
  topo-merge correctness; `fpµs` confirms `O(log n)` with the tree.
- **Phase 5:** laptop-tier JSON metrics within bounds; sign-off gate.
- **Phase 6:** full adversarial suite; scheduler interval test; metrics emission; cross-device stable.

---

## Critical files

- `tests/bench/bench_util.go`, `tests/bench/fixtures/` — Phase-0 harness builds on these.
- `client/p2p.go` — the `client.Host` interface wrapped by the counting Host (`Send`,
  `SetStreamHandler`, `IPLDStore`).
- `internal/db/p2p/p2p.go` — `New()` ~L195 gated registration; DB interface ~L98 for the flag method.
- `client/options/node.go`, `cli/start.go`, `cli/config/config.go`, `internal/db/config.go` — the
  default-off config chain.
- `internal/db/p2p/sync_dag.go` — `syncDAG`/`loadBlockLinks`/`IsMerged`: baselined in Phase 0, reused
  as the fetch+merge sink in Phases 3–4.
- `internal/db/p2p/negentropy/` (new, Phase 2), `internal/db/p2p/protocol/reconcile.go` +
  `internal/db/p2p/reconcile.go` (new, Phase 3).
- `internal/datastore/multi.go` — Phase-4 index namespace; `internal/core/block/heads.go` —
  reconciled-set source; `internal/db/p2p/protocol/comm_channel.go` + `identity.go` — registration
  pattern.

---

## Note

Once this is executed, sync RFC §8 (Rollout/milestones) to match this phasing (Phase 0 benchmarking,
plain-bool flag, cross-device gate, ACP as Phase 7) so the two documents don't drift.
