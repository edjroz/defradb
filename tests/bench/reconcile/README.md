# Reconciliation control-traffic comparison (Phase 2 → Phase 3 gate)

A contrasting benchmark for the case we want to make before building Phase 3: how
much **control traffic** does it take to discover *which blocks two peers are
missing*, under the **default DefraDB sync** versus **range-based set
reconciliation (Negentropy)**?

Both approaches transfer the same payload afterwards — the `d` blocks of the
symmetric difference. What differs is the coordination cost of *finding* that
difference. The default has no set-reconciliation primitive, so to learn which of
`n` items a peer lacks it must communicate the identity of the whole set —
`O(n)`. Negentropy recursively compares fingerprints and narrows in on just the
difference — `O(d · log n)`.

This directory is self-contained and adds nothing to the default test/bench lane.

## What is measured vs modelled

| Side | Source | How |
|---|---|---|
| **Negentropy** | **Measured** from the real Phase-2 engine (`internal/db/p2p/negentropy`) | A full session is driven over an in-memory transport; control bytes are the summed `Message.EncodedSize()` across every message, round-trips are the initiator's round count. The engine's recovered need/have set is asserted to equal the exact symmetric difference. |
| **Default sync** | **Modelled** as a naive full-identifier exchange | The initiator sends its entire id list (`n` ids); the responder diffs locally and replies with the `d` ids naming the difference. One round-trip, `(n + d)` ids on the wire. |

The baseline is a deliberately **generous lower bound** on the real default sync:
an actual DAG walk re-fetches block headers (CID + links + height), not bare CIDs,
and gossips per-head, so it costs *at least* this much. The per-id byte unit
(`32-byte id + 1 framing`) is held identical on both sides so the comparison is
apples-to-apples. `Message.EncodedSize()` is the engine's own stable size
approximation (not the final CBOR wire codec, which lands with Phase 3), which is
why this comparison reports *protocol* bytes rather than raw socket bytes.

## Reproduce

```sh
# one command: regenerate the dataset + render the SVGs + index.html
make test:bench-reconcile-report

# then view
open tests/bench/reconcile/index.html
```

Or run the two steps by hand:

```sh
# 1. regenerate the dataset (drives the real engine; gated, ~0.3s)
DEFRA_RECONCILE_REPORT=1 go test ./tests/bench/reconcile/ -run TestGenerateComparisonData -v

# 2. render the SVG charts + index.html (pure stdlib, no matplotlib)
python3 tests/bench/reconcile/plot.py
```

The `make` target regenerates only the modelled `data/comparison.csv`; the
`measured_*.csv` inputs (including the crossdevice sweep) stay committed.

Without `DEFRA_RECONCILE_REPORT` the test skips, keeping the default lane fast.

## Files

- `comparison_test.go` — the gated data generator (engine driver + baseline model).
- `plot.py` — dependency-free SVG chart renderer (stdlib only).
- `data/comparison.csv` — generated synthetic dataset (modelled baseline vs measured engine).
- `plots/0[1-4]_*.svg` — synthetic sweep charts; `plots/07_*`–`plots/15_*` — measured charts.
- `index.html` embeds them all with a narrative.

## Results

Two sweeps, plus the real-harness anchor below.

**Sweep 1 — control bytes vs set size `n`, difference fixed at `d=2`:**

| n | negentropy | default (full-set) | reduction |
|--:|--:|--:|--:|
| 100 | 2.6 KB | 3.4 KB | 1× |
| 1,000 | 6.4 KB | 33 KB | 5× |
| 10,000 | 10.4 KB | 330 KB | 32× |
| 30,000 | 16.4 KB | 990 KB | 60× |
| 100,000 | 17.5 KB | 3.3 MB | **188×** |

Negentropy's control bytes grow ~logarithmically (2.6 KB → 17.5 KB as the set
grows 1000×) while the default grows linearly. This is the headline
(`plots/01_bytes_vs_n.svg`, `plots/02_reduction_vs_n.svg`).

**Sweep 2 — control bytes vs difference `d`, set fixed at `n=10,000`:** Negentropy
scales with `d`; full exchange is flat at ~330 KB. They **cross at `d ≈ 5–10 % of
n`** — below that, RBSR wins by orders of magnitude; above it (most of the set
differs, e.g. cold start of a huge collection) full exchange is no worse, and
Phase 4's direct block-set reconciliation is the right tool
(`plots/03_bytes_vs_d.svg`).

**The trade-off:** negentropy spends a few more (logarithmic, ≤4 here) round-trips
to buy those byte savings; the default needs one (`plots/04_rounds_vs_n.svg`).

### Measured — Phase 3 / M1, real nodes (Apple M4 Pro)

Phase 3 wires the engine to the network (`ReconcileDocument`), so the baseline is no
longer modelled — both sides are now **measured** on real libp2p nodes by the
`CountingHost`, which captures the `/defradb/reconcile_*` and pushlog traffic
automatically. Reproduce:

```sh
DEFRA_BENCH_SYNC=1 go test ./tests/bench/sync/ -run '^$' \
    -bench 'Benchmark_(Sync_ManyHead|Reconcile)' -benchtime=1x -timeout=10m
```

Same post-partition divergence (`docs10_base5_k3`), converged two ways:

| approach | ctrlBytes | ctrlMsgs | blocks | blockBytes | syncMs |
|---|--:|--:|--:|--:|--:|
| default doc-sync (1 broadcast) | 429 | 1 | 60 | 14,910 | 1,245 |
| reconcile (10 per-doc sessions) | 18,970 | 42 | 60 | 14,910 | 1,945 |
| reconcile, single doc | 2,869 | 6 | 6 | 1,491 | 200 |

**What this measures, honestly:** the **payload is identical** (`blocks`/`blockBytes`
match) — reconciliation changes only how the diff is *discovered*, not what is
transferred. At M1's per-document scale reconciliation is **more expensive on the
control plane** (~44× the bytes, 42 vs 1 round-trips), because M1 opens one session
*per document* and a document's head-set is tiny. This is the expected M1 result and
the reason the headline asymptotic win is **explicitly an M2 deliverable**:

- M2 reconciles a **whole collection's block-CID set in one session**, so the 42
  messages collapse toward a single logarithmic session over a large set — the regime
  the synthetic sweep above models (small diff, large `n`). The measured M2 collapse
  of this M1 overhead is charts 07–08; the modelled sweep shows where the curve goes
  once the set is large and batched.

The benchmark's value at M1 is therefore (1) validating the real path end-to-end
(convergence, payload ∝ diff) and (2) quantifying the per-session overhead that M2
must amortise — not claiming a bandwidth win at this scale.

### Measured — Phase 4a / M2 per-collection (real nodes, Apple M4 Pro)

Phase 4a reconciles a whole collection's composite block set in **one session**
(`ReconcileCollection`), versus M1's one-session-per-document. Reproduce:

```sh
DEFRA_BENCH_SYNC=1 go test ./tests/bench/sync/ -run '^$' \
    -bench 'Benchmark_(Sync_ColdStart|Reconcile_Collection)' -benchtime=1x -timeout=10m
```

Converging a 10-document collection (cold start), three ways:

| approach | ctrlBytes | ctrlMsgs | blocks | blockBytes |
|---|--:|--:|--:|--:|
| default doc-sync (broadcast) | 1,509 | 3 | 60 | 12,960 |
| M1 per-document (10 sessions)¹ | 18,970 | 42 | 60 | 14,910 |
| **M2 per-collection (1 session)** | **2,172** | **4** | 60 | 12,960 |

¹ M1 figure is the post-partition `docs10` run; the apples-to-apples point is the
**session count** — M1 opens one session per document (≈42 round-trips for 10 docs),
M2 opens one (4 round-trips), regardless of document count.

**The M2 win (charts 07–08):** per-collection reconciliation **collapses M1's
per-document overhead** — round-trips drop ~10× (42 → 4) and control bytes ~9×
(19 KB → 2.2 KB), landing at roughly broadcast doc-sync levels, with identical
payload. At *cold start* (diff = the whole collection) M2's control cost is
necessarily comparable to broadcast — the asymptotic *bandwidth* win is in
**partial catch-up** (large shared base, small diff), where one session fetches only
the divergent blocks. The synthetic sweep (charts 01–04) models that regime; a true
tiny-diff/large-base measurement is the next harness addition.

The deeper win is correctness, not just bytes: M2 gives true **O(diff) cold-start**
— it discovers the missing block set up front and fetches it directly, instead of
walking the entire DAG.

### Real-harness anchor (Apple M4 Pro)

Measured `ctrlBytes` from the Phase-0 full-DAG-walk harness (`tests/bench/sync`)
confirms the default's control channel really is `O(#docs)` — the `SyncDocuments`
request carries a per-document id list:

| scenario | docs | ctrlBytes | ctrlMsgs |
|---|--:|--:|--:|
| ColdStart | 10 | 1,509 | 3 |
| ColdStart | 50 | 3,190 | 3 |
| ManyHead (k=3) | 10 | 429 | 1 |

Control bytes scale with document count (10 → 50 docs roughly doubles them),
empirically matching the `O(n)` baseline the synthetic sweep models — at small `n`
the real numbers are dominated by fixed libp2p/pubsub overhead, which is exactly
why the synthetic sweep is needed to expose the asymptotic difference.

## Caveats / honesty

- The default-sync baseline is a **model**, not a live capture; it is a lower
  bound on the real cost (see above). The negentropy side is real engine output.
- `EncodedSize()` is a stable approximation, not the final CBOR wire size.
- Negentropy is **not** universally cheaper: past the crossover (`d` ≳ 10 % of
  `n`) it loses to full exchange, and it always costs more round-trips. The win is
  specifically the **small-difference / large-set** regime — the steady-state
  anti-entropy and incremental-tail cases Phase 3 targets.
- These are control-plane bytes only; block payload transfer is identical for both
  and excluded.
