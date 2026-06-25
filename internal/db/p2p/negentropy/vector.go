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

import (
	"sort"
)

// Vector is the Phase-2 [Storage]: a sorted, sealed slice of items.
//
// Its Fingerprint is a literal O(n) scan over the requested window — deliberately
// so, both to keep the implementation obviously correct and to give the
// fingerprint micro-benchmark something to measure. Phase 4 replaces the O(n)
// scan with a maintained tree behind the same [Storage] interface.
type Vector struct {
	items []Item
}

var _ Storage = (*Vector)(nil)

// VectorBuilder accumulates items and seals them into a [Vector]. Insertion order
// does not matter; [VectorBuilder.Build] sorts by SortKey and rejects duplicates.
type VectorBuilder struct {
	items []Item
}

// NewVectorBuilder returns an empty builder, pre-sizing for capHint items.
func NewVectorBuilder(capHint int) *VectorBuilder {
	return &VectorBuilder{items: make([]Item, 0, capHint)}
}

// Add appends an item built from a block height and CID bytes.
func (b *VectorBuilder) Add(height uint64, cid []byte) {
	b.items = append(b.items, NewItem(height, cid))
}

// AddItem appends a pre-built item.
func (b *VectorBuilder) AddItem(it Item) {
	b.items = append(b.items, it)
}

// Build sorts the accumulated items by SortKey, verifies there are no duplicate
// sort keys, and returns a sealed [Vector]. It returns an error from
// [NewErrDuplicateItem] on a collision.
func (b *VectorBuilder) Build() (*Vector, error) {
	items := b.items
	sort.Slice(items, func(i, j int) bool {
		return CompareItems(items[i], items[j]) < 0
	})
	for i := 1; i < len(items); i++ {
		if CompareItems(items[i-1], items[i]) == 0 {
			return nil, NewErrDuplicateItem(items[i].SortKey)
		}
	}
	return &Vector{items: items}, nil
}

// NewVector seals the given items into a [Vector] (sorting and dedup-checking
// them). It is a convenience around [VectorBuilder] for already-collected items.
func NewVector(items []Item) (*Vector, error) {
	b := &VectorBuilder{items: append([]Item(nil), items...)}
	return b.Build()
}

// Size returns the number of items.
func (v *Vector) Size() int { return len(v.items) }

// Seek returns the smallest index whose item has SortKey >= bound. MinBound maps
// to 0; MaxBound maps to Size().
func (v *Vector) Seek(bound []byte) int {
	if IsMaxBound(bound) {
		return len(v.items)
	}
	return sort.Search(len(v.items), func(i int) bool {
		return CompareBound(v.items[i].SortKey, bound) >= 0
	})
}

// ItemID returns the raw CID bytes of the item at index i.
func (v *Vector) ItemID(i int) []byte { return v.items[i].ID }

// ItemSortKey returns the SortKey of the item at index i.
func (v *Vector) ItemSortKey(i int) []byte { return v.items[i].SortKey }

// Fingerprint summarizes the items in [lo, hi) via an O(n) scan.
func (v *Vector) Fingerprint(lo, hi int) Fingerprint {
	return v.Accumulate(lo, hi).Finalize()
}

// Accumulate folds the IDs in [lo, hi) into a fresh accumulator (O(n) scan).
func (v *Vector) Accumulate(lo, hi int) Accumulator {
	var a Accumulator
	for i := lo; i < hi; i++ {
		a.Add(v.items[i].ID)
	}
	return a
}
