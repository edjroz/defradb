// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package negentropy

// Storage is the read-only, sorted view of a local item set that the reconciler
// queries. It is the seam that lets the fingerprint backend change without
// touching the reconciler:
//
//   - Phase 2 ships only [Vector], an O(n)-scan implementation.
//   - Phase 4 plugs in a maintained Fenwick/segment tree giving O(log n)
//     Fingerprint, implementing this same interface — the reconciler is unaware.
//
// Implementations MUST present items in strict ascending SortKey order with no
// duplicates, and MUST be immutable ("sealed") for the lifetime of a session.
type Storage interface {
	// Size returns the number of items.
	Size() int

	// Seek returns the smallest index i in [0, Size()] whose item has
	// SortKey >= bound (the lower-bound insertion point). MinBound maps to 0 and
	// MaxBound maps to Size().
	Seek(bound []byte) int

	// ItemID returns the raw CID bytes of the item at index i (0 <= i < Size()).
	ItemID(i int) []byte

	// ItemSortKey returns the SortKey of the item at index i. Split boundaries
	// land on real item sort keys so both peers derive identical windows.
	ItemSortKey(i int) []byte

	// Fingerprint summarizes the items in the half-open index range [lo, hi).
	// It returns EmptyFingerprint when lo == hi.
	Fingerprint(lo, hi int) Fingerprint

	// Accumulate returns the combinable partial state for [lo, hi), letting a
	// caller compose adjacent sub-ranges via Accumulator.Merge.
	Accumulate(lo, hi int) Accumulator
}
