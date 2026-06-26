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

## The story in six measurements

| # | What | Result | Source |
|---|---|---|---|
| 1 | **Asymptotic potential** (modelled sweep) | **188× fewer control bytes** to find a 2-item difference at n=100k | charts 1–4, `data/comparison.csv` (Phase-2 engine, measured) vs modelled full-set baseline |
| 2 | **Per-document overhead** (M1, real nodes) | reconciliation costs **~44×** the control bytes of broadcast doc-sync at per-document scale (one session per doc); **identical payload** | charts 5–6, `data/measured_m1.csv` |
| 3 | **Per-collection collapse** (M2, real nodes) | one collection session brings that back to **~broadcast levels** (2172 vs 1509 B at 10 docs; 4 round-trips, not 42) | charts 7–8, `data/measured_m2.csv` |
| 4 | **The payoff** (tiny-diff, real nodes) | one changed doc in a growing collection: default control scales with collection size (**2.1KB→8.4KB**, 50→200 docs) while reconciliation stays ~flat (**2.7KB→4.5KB**) and **crosses below** it — control ∝ diff, not collection size | chart 9, `data/measured_m5.csv` |
| 5 | **Compute** (segment tree) | full-range fingerprint is **O(log n) ~280ns flat** at n=100k vs the Vector's O(n) ~5.8ms (**~21,000×**); per-session build is O(n) ~31ms at n=100k (amortised across the session) | `negentropy` micro-benchmarks |
| 6 | **The cost** (index-write, real node) | the always-on per-commit ordered-index write adds **~4.6%** local write latency (76.4ms→79.96ms for 50 docs × 5 updates) and **~2.8%** heap | chart 10, `data/measured_indexwrite.csv` |

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

## Cross-device validation

The measurements above run both nodes on one machine. `tests/bench/crossdevice/`
runs the same default-vs-ranges contrast across an **N-node network with
configurable topologies** on separate processes/devices, using an instrumented
bench-node that reports the same control-byte metric. The committed smoke runs show
the tiny-diff win reproduces across separate processes (**~2× fewer control bytes**,
`pair`/`tiny-diff`) and that arbitrary topologies converge correctly
(`line`/`multi-writer`, multi-hop). See that directory's README — including the
honest note that its `ranges` control bytes include bidirectional-push tips, so the
in-process numbers here remain the cleanest control-byte measurement.

## Honest caveats

- **Charts 1–4 are modelled**, not measured end-to-end: the negentropy side is the
  real Phase-2 engine, but the default-sync baseline is a *generous* full-identifier
  exchange (a lower bound on the real DAG-walk cost). Charts 5–10 are measured on
  real nodes.
- **Single device tier (M4 Pro) here.** The gate requires a second, weaker
  Apple-Silicon device before final sign-off; x86_64 is out of scope (deferred to
  the team). See `.plans/phase-5-cross-device-gate.md`.
- **Default-off.** None of this touches the default sync path; the flag-off
  benchmarks are unchanged.

## Sign-off criteria

See `.plans/phase-5-cross-device-gate.md` for the pass/fail gate, the device matrix
to fill, and the explicit pause point.
