# Range-based set reconciliation — the case for sign-off

This is the consolidated evidence for the experimental, **default-off** range-based
set-reconciliation (Negentropy) sync, gathered across Phases 2–5. It exists to
answer one question before further investment: **does reconciliation buy a real,
measured bandwidth win over the existing DAG-walk sync, at an acceptable cost?**

Short answer: **yes, in the regime it targets** — large shared state with a small
divergence — with a small, bounded maintenance cost when enabled and **zero**
change to the default (flag-off) path.

All charts referenced below are in [`index.html`](index.html) (regenerate with
`python3 plot.py`). Numbers are Apple M4 Pro; see the device matrix in the gate doc.

## The story in seven measurements

| # | What | Result | Source |
|---|---|---|---|
| 1 | **Asymptotic potential** (modelled sweep) | **188× fewer control bytes** to find a 2-item difference at n=100k | charts 1–4, `data/comparison.csv` (Phase-2 engine, measured) vs modelled full-set baseline |
| 2 | **Per-document overhead** (M1, real nodes) | reconciliation costs **~44×** the control bytes of broadcast doc-sync at per-document scale (one session per doc); **identical payload** | README §"Measured — Phase 3 / M1" table (429 vs 18,970 B) |
| 3 | **Per-collection collapse** (M2, real nodes) | one collection session brings that back to **~broadcast levels** (2172 vs 1509 B at 10 docs; 4 round-trips, not 42) | charts 7–8, `data/measured_m2.csv` |
| 4a | **Payoff vs collection size** (real nodes) | one changed doc, collection 50→1000: default control is linear (**2.1→42KB**, ~42 B/doc) while reconciliation is logarithmic (**2.7→6.4KB**) — a **6.5× gap at 1000 docs** | chart 9, `data/measured_m5.csv` |
| 4b | **Payoff vs difference size** (real nodes) | collection fixed at 500, diff 1→500: default is **flat at 21KB** (`O(n)`, lists every docID) while reconciliation rises **linearly** (**5.3→23.2KB**; at fixed n, `O(diff·log n)`=`O(diff)`) and **crosses above default at ~diff 395 (~79% of the collection)** — the measured trade-off *and* its crossover | chart 12, `data/measured_diffsweep.csv` |
| 5 | **Compute** (segment tree) | full-range fingerprint is **O(log n) ~280ns flat** at n=100k vs the Vector's O(n) ~5.8ms (**~21,000×**); per-session build is O(n) ~31ms at n=100k (amortised across the session) | `negentropy` micro-benchmarks |
| 6 | **The cost** (index-write, real node) | the always-on per-commit ordered-index write adds **~4.6%** local write latency (76.4ms→79.96ms for 50 docs × 5 updates) and **~2.8%** heap | chart 10, `data/measured_indexwrite.csv` |
| 7 | **Long-branch depth-invariance** (real nodes) | branches **3→50 commits deep** (16×): reconciliation control is **flat** (18.97→18.98 KB, 42 round-trips) — as is default sync (429 B) — while payload scales **60→1000 blocks**. Depth is a payload/merge cost, not a discovery cost | charts 13–14, `data/measured_depthinvariance.csv` |

## What each measurement means

- **It is not free at small scale.** At per-document granularity (M1) reconciliation
  is *more* expensive than a broadcast — the session overhead dominates when there
  is nothing to prune. This is honest and expected; the win is asymptotic.
- **The win is in the target regime.** Measurement 4 is the crux: when most of a
  collection is shared and only a little diverges (the steady-state anti-entropy
  case), the default sync still pays for the *whole* collection (it must enumerate
  every docID) while reconciliation pays for the *difference*. The crossover is
  already visible by 200 docs and widens with collection size.
- **Payload is identical** in every measured case — reconciliation changes the
  *coordination* cost, not the blocks transferred.
- **The maintenance cost is small and bounded** (≈5%), comfortably inside the gate's
  ≤15% budget, and is paid only when the flag is on.
- **Depth is not a cost axis for discovery.** Measurement 7: a divergent branch 50
  commits deep costs the same control traffic to *find* as one 3 commits deep — both
  reconciliation and default sync discover divergence head-first, so only the payload
  (blocks fetched + merged) grows with depth. The win/cost story is set by collection
  size and diff *count* (measurements 4a/4b), not branch depth. (In this M1 per-doc
  regime reconciliation's control is higher than broadcast — measurement 2 — so
  charts 13–14 are read for *depth-invariance*, not for a per-doc byte win.)

## Cross-device validation

The measurements above run both nodes in one process. `tests/bench/crossdevice/`
runs the same default-vs-ranges contrast across an **N-node network with
configurable topologies**, using an instrumented bench-node that reports the same
control-byte metric. Measured here on **N real Docker containers on one host bridge**
(`tests/bench/crossdevice/docker/`, `star`/`tiny-diff`/docs50, one diverged doc on the
hub), swept over node count:

| nodes | reconcile | default | reduction |
|---|---|---|---|
| 2 | 10,008 B | 17,984 B | **1.8×** |
| 5 | 40,032 B | 148,190 B | **3.7×** |
| 10 | 90,072 B | 1,283,120 B | **14×** |

Reconciliation scales ~linearly with the star's edges (≈10 KB/edge) while default
broadcast grows super-linearly (every node re-lists all docIDs), so the reduction
**compounds** with network size. Every run converged with an **identical document set
on all nodes** (`stateMatch`), not just an equal block count (chart 11,
`data/measured_crossdevice.csv`). Arbitrary topologies also converge correctly
(`line`/`multi-writer`, multi-hop). This is a single-host container network — a
true weaker-second-device run (the Phase 5 ARM gate) is still separately required;
see that directory's README, including the honest note that the manual API's `ranges`
control bytes include bidirectional-push tips, so the in-process benchmarks remain the
cleanest per-node control-byte measurement.

## Honest caveats

- **Charts 1–4 are modelled**, not measured end-to-end: the negentropy side is the
  real Phase-2 engine, but the default-sync baseline is a *generous* full-identifier
  exchange (a lower bound on the real DAG-walk cost). Charts 7–15 are measured on
  real nodes.
- **Single device tier (M4 Pro) here.** The gate requires a second, weaker
  Apple-Silicon device before final sign-off; x86_64 is out of scope (deferred to
  the team). See `.plans/phase-5-cross-device-gate.md`.
- **Default-off.** None of this touches the default sync path; the flag-off
  benchmarks are unchanged.

## Sign-off criteria

See `.plans/phase-5-cross-device-gate.md` for the pass/fail gate, the device matrix
to fill, and the explicit pause point.
