# `negentropy` — range-based set reconciliation core

This package is the pure, transport-agnostic engine for **range-based set
reconciliation (RBSR)**, also known as **Negentropy**. It lets two peers
efficiently discover which items each is missing while exchanging data
proportional to the size of their **difference**, not the size of their sets.

- **Bandwidth:** `O(diff · log n)` — independent of the agreed-upon majority.
- **Rounds:** `O(log n)`.
- **Dependencies:** none beyond the standard library and `defradb/errors`. A CID
  is treated as opaque `[]byte`; there is no network, libp2p, or store coupling.

It is the algorithmic heart of the broader Negentropy P2P-sync work; the wire
codec, the head/DAG integration, and the block transfer that consumes the
discovered sets live in later phases. See
[`../../../../.plans/rfc-negentropy-set-reconciliation-sync.md`](../../../../.plans/rfc-negentropy-set-reconciliation-sync.md)
for the full design and [hoytech/negentropy](https://github.com/hoytech/negentropy)
for the reference protocol this mirrors.

---

## 1. Scope (what this package does and does not do)

**Does:** order items, fingerprint ranges, run the Skip/Fingerprint/IdList state
machine to convergence, and produce the initiator's **have-set** and **need-set**.
Correctness and the `O(diff)` bandwidth claim are proven in-package over an
in-memory counting transport.

**Does not (later phases):** CBOR wire encoding + the signed comm-channel; the
maintained fingerprint tree; head/`syncDAG` integration and a syncDAG-parity
oracle; the actual fetching/pushing of blocks.

---

## 2. Sort key

Items are ordered by a **sort key**:

```
sortKey = heightBE8 || cidBytes
```

an 8-byte big-endian block height followed by the raw CID bytes (`SortKey`).
Ordering by height first groups items into causal layers (useful for later fetch
ordering); appending the CID breaks height ties and guarantees a **strict total
order**. Heads-only callers may pass `height = 0` for plain CID-byte order.

Range bounds use two sentinels alongside real keys, always compared via
`CompareBound` (never `bytes.Compare` directly):

- `MinBound()` — the inclusive low end: an empty, **non-nil** slice that orders
  before every real key.
- `MaxBound()` — the exclusive "+infinity" high end: a **nil** slice recognized
  by `IsMaxBound`. Real keys are never nil and are at least 8 bytes, so nil is
  unambiguously the max sentinel. (This nil/empty distinction is in-memory only;
  the wire encoding of the sentinel is defined in a later phase.)

---

## 3. Fingerprint

A range is summarized by a 16-byte fingerprint:

```
fp(range) = SHA256( ( Σ_{cid ∈ range} SHA256(cid) )  mod 2^256  ||  uvarint(count) )[:16]
```

Implemented by `Accumulator`:

1. **Per-item hash** — `SHA256(cid)` (over the raw CID bytes, not the sort key).
2. **Additive sum mod 2^256** — a fixed `[32]byte` big-endian accumulator with
   byte-wise add-and-carry; carry out of the top byte is discarded (the defining
   wraparound). This is the `acc256` type — no `math/big`.
3. **Count fold-in** — the item count, appended as a `uvarint`.
4. **Outer hash + truncate** — `SHA256(...)` truncated to 16 bytes.

### Worked example (algebraic)

- **Empty range:** `EmptyFingerprint() == SHA256(0×32 || uvarint(0))[:16]`, equal
  to `(Accumulator{}).Finalize()`. *(asserted by `TestEmptyFingerprintIdentity`)*
- **Order independence:** for any permutation of IDs, the accumulator's sum is the
  same (modular addition is commutative), so `FingerprintOf(a,b,c) ==
  FingerprintOf(c,a,b)`. *(asserted by `TestFingerprintOrderIndependent`)*
- **Combinability:** for disjoint sets `A` and `B`,

  ```
  sum(A∪B) = (sum(A) + sum(B)) mod 2^256      count(A∪B) = count(A) + count(B)
  ```

  so `fp(A∪B)` is recoverable from the two partial accumulators via
  `Accumulator.Merge` without rescanning. *(asserted by `TestFingerprintCombinable`)*
  This is exactly what lets the responder split a range and what lets the Phase-4
  tree answer range fingerprints in `O(log n)`.

### Why fold in the count?

A plain additive (or XOR) sum is vulnerable to **pairwise cancellation**: two
different sets can share the same sum. Folding the count into the outer hash makes
two ranges with identical sums but different sizes produce different fingerprints.
*(asserted by `TestCountFoldDefeatsCancellation`)*

---

## 4. The protocol

Modes per range: **Skip** (agreed), **Fingerprint** (compare/split), **IdList**
(explicit IDs). A `Message` is an ordered list of ranges that **tiles**
`[MinBound, MaxBound)`: each range's lower bound is the previous range's
(exclusive) upper bound; the first starts at `MinBound`, the last ends at
`MaxBound`.

### Roles

- **Initiator** (`Initiator`, stateful) drives the session. `Initiate` sends one
  full-range Fingerprint. Each `Reconcile` maps every incoming range to exactly
  **one** outgoing range — `Skip` when its fingerprint matches, an echoed
  `Fingerprint` when it differs, `Skip` after consuming an `IdList` — so the
  initiator never changes the tiling. It accumulates have/need from IdLists.
- **Responder** (`Respond`, stateless) answers from its own data with no
  per-session memory. For a mismatching Fingerprint range it emits an `IdList`
  when the window is `≤ IDListThreshold`, otherwise it **splits** into
  `BranchingFactor` sub-ranges (each a Fingerprint). Split bounds land on real
  item sort keys so both peers derive identical windows via `Seek`.

Each round at least `BranchingFactor`-way narrows every still-disagreeing range,
so the session converges in `O(log n)` rounds and `O(diff·log n)` bytes — agreed
regions settle to `Skip` immediately and are never drilled into.

### Topology

A **single initiator-driven session** makes the *initiator* learn both its
**need-set** (`Need()` — IDs the peer holds that it lacks, to pull) and its
**have-set** (`Have()` — IDs it holds that the peer lacks, to push). The responder
stays stateless and learns nothing; pushing/pulling the actual blocks is a later
phase.

### Round-by-round walkthrough

Initiator `A = {1, 2, 3}` reconciling against responder `B = {2, 3, 4}`
(this is `TestCountingTransportConverges`):

| Round | Direction | Message | Why |
|-------|-----------|---------|-----|
| 0 | A → B | `[ (+∞, Fingerprint fp_A) ]` | `Initiate`: one full-range fingerprint. |
| 1 | B → A | `[ (+∞, IdList {2,3,4}) ]` | `fp_A ≠ fp_B` and `|B|=3 ≤ IDListThreshold`, so B lists its IDs. |
| 1 | A → B | `[ (+∞, Skip) ]` | A diffs the list: `need = {4}` (in B, not A), `have = {1}` (in A, not B); range settled → all-Skip → **done**. |

Result: `Need() = {4}`, `Have() = {1}`. For larger sets the round-1 IdList is
replaced by a recursive split that drills only into the disagreeing buckets.

---

## 5. Caps (`caps.go`)

Baked in so a single misbehaving peer cannot cause unbounded work:

| Constant | Value | Purpose |
|----------|-------|---------|
| `BranchingFactor` | 16 | sub-ranges per split |
| `MaxRounds` | 32 | round cap → a never-agreeing peer terminates with `ErrRoundCapExceeded` |
| `IDListThreshold` | 64 | list-vs-split decision (window size) |
| `MaxIDsPerRange` | 64 | hard cap on IDs in one IdList (≥ threshold, so lists never exceed it) |
| `MaxRangesPerMessage` | 16384 | beyond this the responder defers refinement to a later round |
| `MaxFrameBytes` | 16 MiB | mirrors the eventual transport frame cap |

---

## 6. Invariants

Each is asserted by the named test; later phases must not break them.

1. **Total order is strict & deterministic** — `heightBE8 || cidBytes`.
   *(`TestSortKeyHeightFirstTotalOrder`, `TestSortKeyHeightZeroIsPlainCIDOrder`)*
2. **Fingerprint is order-independent and combinable; empty is the identity.**
   *(`TestFingerprintOrderIndependent`, `TestFingerprintCombinable`, `TestEmptyFingerprintIdentity`)*
3. **Every message tiles `[MinBound, MaxBound)`** — contiguous, strictly
   ascending, last range ends at `MaxBound`. *(`assertTiles`, run on every message
   in every session test)*
4. **Convergence to the exact set difference** — `Need() == B\A`, `Have() == A\B`.
   *(`TestReconcileMatchesNaiveSetDiff` over 25 random seeds)*
5. **Bandwidth scales with `|A△B|`, not `|A∪B|`** — identical sets reconcile in
   constant bytes; small diffs cost far less than the union.
   *(`TestBytesScaleWithSymmetricDifference`, `TestConvergesIdenticalLargeSetInConstantBytes`)*
6. **Bounded under adversarial input** — a peer that never agrees terminates at
   `MaxRounds` with an error and partial results. *(`TestGarbageFingerprintTerminates`)*

---

## 7. Storage seam (how Phase 4 plugs in)

The reconciler depends only on the `Storage` interface
(`Size`/`Seek`/`ItemID`/`ItemSortKey`/`Fingerprint`/`Accumulate`). This phase ships
`Vector` — a sorted, sealed slice whose `Fingerprint(lo,hi)` is a **literal O(n)
scan** (deliberately, so it is obviously correct and so the micro-benchmark has
something to measure). A later phase adds a maintained Fenwick/segment tree
implementing the same interface to answer range fingerprints in `O(log n)`; the
reconciler is unaware of the swap. The combinability of `Accumulator` (§3) is the
property that tree relies on.

`BenchmarkVectorFingerprintScan` characterizes the current scan (≈ linear in `n`)
as the baseline for that future comparison.

---

## 8. Testing

```sh
go test ./internal/db/p2p/negentropy/...                                  # all unit/property tests
go test -run '^$' -bench BenchmarkVectorFingerprintScan -benchmem ./internal/db/p2p/negentropy/
```

Tests are pure and deterministic: item IDs are `sha256(seed)`, so no CIDs,
network, or store are involved.
