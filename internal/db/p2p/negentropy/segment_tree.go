// Copyright 2026 Democratized Data Foundation
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

// SegmentTree is a [Storage] whose range fingerprint is O(log n) instead of the
// Vector's O(n) scan. It holds the same sorted, sealed item slice as a [Vector]
// (so Size/Seek/ItemID/ItemSortKey are identical), plus an iterative segment tree
// of per-item [Accumulator]s: each leaf accumulates one item ID and each internal
// node is the Merge of its children. A range query over [lo, hi) combines O(log n)
// node accumulators — sound because Accumulator.Merge is associative and
// commutative (see accumulator.go).
//
// It is built once per session from the maintained ordered index; building is O(n)
// but every per-range fingerprint during the multi-round session is then O(log n).
type SegmentTree struct {
	items []Item
	// tree is a 1-indexed iterative segment tree over size leaves: leaves live at
	// [size, size+len(items)), internal node i is Merge(tree[2i], tree[2i+1]).
	tree []Accumulator
	size int
}

var _ Storage = (*SegmentTree)(nil)

// SegmentTreeBuilder accumulates items and seals them into a [SegmentTree]. It
// mirrors [VectorBuilder]: insertion order does not matter, Build sorts by SortKey
// and rejects duplicates.
type SegmentTreeBuilder struct {
	items []Item
}

// NewSegmentTreeBuilder returns an empty builder, pre-sizing for capHint items.
func NewSegmentTreeBuilder(capHint int) *SegmentTreeBuilder {
	return &SegmentTreeBuilder{items: make([]Item, 0, capHint)}
}

// Add appends an item built from a block height and CID bytes.
func (b *SegmentTreeBuilder) Add(height uint64, cid []byte) {
	b.items = append(b.items, NewItem(height, cid))
}

// AddItem appends a pre-built item.
func (b *SegmentTreeBuilder) AddItem(it Item) {
	b.items = append(b.items, it)
}

// Build sorts the accumulated items by SortKey, verifies there are no duplicate
// sort keys, and returns a sealed [SegmentTree] with its accumulator tree built.
func (b *SegmentTreeBuilder) Build() (*SegmentTree, error) {
	items := b.items
	sort.Slice(items, func(i, j int) bool {
		return CompareItems(items[i], items[j]) < 0
	})
	for i := 1; i < len(items); i++ {
		if CompareItems(items[i-1], items[i]) == 0 {
			return nil, NewErrDuplicateItem(items[i].SortKey)
		}
	}
	return newSegmentTree(items), nil
}

// NewSegmentTree seals the given items into a [SegmentTree] (sorting and
// dedup-checking them). It is a convenience around [SegmentTreeBuilder].
func NewSegmentTree(items []Item) (*SegmentTree, error) {
	b := &SegmentTreeBuilder{items: append([]Item(nil), items...)}
	return b.Build()
}

// newSegmentTree builds the accumulator tree over already sorted, de-duped items.
func newSegmentTree(items []Item) *SegmentTree {
	n := len(items)
	if n == 0 {
		return &SegmentTree{items: items}
	}
	tree := make([]Accumulator, 2*n)
	// Leaves.
	for i := 0; i < n; i++ {
		var a Accumulator
		a.Add(items[i].ID)
		tree[n+i] = a
	}
	// Internal nodes, bottom-up.
	for i := n - 1; i >= 1; i-- {
		var a Accumulator
		a.Merge(tree[2*i])
		a.Merge(tree[2*i+1])
		tree[i] = a
	}
	return &SegmentTree{items: items, tree: tree, size: n}
}

// Size returns the number of items.
func (s *SegmentTree) Size() int { return len(s.items) }

// Seek returns the smallest index whose item has SortKey >= bound. MinBound maps
// to 0; MaxBound maps to Size().
func (s *SegmentTree) Seek(bound []byte) int {
	if IsMaxBound(bound) {
		return len(s.items)
	}
	return sort.Search(len(s.items), func(i int) bool {
		return CompareBound(s.items[i].SortKey, bound) >= 0
	})
}

// ItemID returns the raw CID bytes of the item at index i.
func (s *SegmentTree) ItemID(i int) []byte { return s.items[i].ID }

// ItemSortKey returns the SortKey of the item at index i.
func (s *SegmentTree) ItemSortKey(i int) []byte { return s.items[i].SortKey }

// Fingerprint summarizes the items in [lo, hi) in O(log n).
func (s *SegmentTree) Fingerprint(lo, hi int) Fingerprint {
	if lo >= hi {
		return EmptyFingerprint()
	}
	return s.Accumulate(lo, hi).Finalize()
}

// Accumulate combines the cached node accumulators covering [lo, hi) in O(log n).
// Merge is commutative, so the left/right walk order does not matter.
func (s *SegmentTree) Accumulate(lo, hi int) Accumulator {
	var acc Accumulator
	if lo >= hi || s.size == 0 {
		return acc
	}
	// Standard iterative segment-tree range fold over the half-open [lo, hi).
	l := lo + s.size
	r := hi + s.size
	for l < r {
		if l&1 == 1 {
			acc.Merge(s.tree[l])
			l++
		}
		if r&1 == 1 {
			r--
			acc.Merge(s.tree[r])
		}
		l >>= 1
		r >>= 1
	}
	return acc
}
