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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// segTreeFrom builds a sealed height-0 SegmentTree from integer seeds.
func segTreeFrom(t *testing.T, intSeeds ...int) *SegmentTree {
	t.Helper()
	b := NewSegmentTreeBuilder(len(intSeeds))
	for _, s := range intSeeds {
		b.Add(0, tid(s))
	}
	st, err := b.Build()
	require.NoError(t, err)
	return st
}

// TestSegmentTree_EquivalentToVector asserts the segment tree is a drop-in for the
// O(n)-scan Vector: identical Size, Seek, ItemID/ItemSortKey, and — exhaustively —
// identical Fingerprint over every [lo, hi) window.
func TestSegmentTree_EquivalentToVector(t *testing.T) {
	for _, n := range []int{0, 1, 2, 5, 16, 33, 100} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			vb := NewVectorBuilder(n)
			sb := NewSegmentTreeBuilder(n)
			for i := 0; i < n; i++ {
				h := uint64(i % 4) // varied heights to exercise sort-key ordering
				id := tid(i)
				vb.Add(h, id)
				sb.Add(h, id)
			}
			vec, err := vb.Build()
			require.NoError(t, err)
			st, err := sb.Build()
			require.NoError(t, err)

			require.Equal(t, vec.Size(), st.Size())

			for lo := 0; lo <= n; lo++ {
				for hi := lo; hi <= n; hi++ {
					require.Equal(t, vec.Fingerprint(lo, hi), st.Fingerprint(lo, hi),
						"fingerprint mismatch [%d,%d)", lo, hi)
				}
			}

			for i := 0; i < n; i++ {
				require.Equal(t, vec.ItemID(i), st.ItemID(i), "ItemID(%d)", i)
				require.Equal(t, vec.ItemSortKey(i), st.ItemSortKey(i), "ItemSortKey(%d)", i)
			}

			require.Equal(t, vec.Seek(MinBound()), st.Seek(MinBound()))
			require.Equal(t, vec.Seek(MaxBound()), st.Seek(MaxBound()))
			for i := 0; i < n; i++ {
				bound := vec.ItemSortKey(i)
				require.Equal(t, vec.Seek(bound), st.Seek(bound), "Seek(item %d)", i)
			}
		})
	}
}

// TestSegmentTree_ConvergesInSession drives a full reconciliation with the segment
// tree as both peers' Storage and checks the recovered symmetric difference.
func TestSegmentTree_ConvergesInSession(t *testing.T) {
	a := segTreeFrom(t, seeds(1, 10)...)
	b := segTreeFrom(t, seeds(6, 15)...)

	it := NewInitiator(a)
	msg := it.Initiate()
	for round := 0; ; round++ {
		require.Less(t, round, MaxRounds+5, "session must converge")
		resp, err := Respond(b, msg)
		require.NoError(t, err)
		next, done := it.Reconcile(resp)
		require.NoError(t, it.Err())
		if done {
			break
		}
		msg = next
	}

	require.Equal(t, setOf(seeds(11, 15)), idSet(it.Need()), "need = B \\ A")
	require.Equal(t, setOf(seeds(1, 5)), idSet(it.Have()), "have = A \\ B")
}
