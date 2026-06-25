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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSortKeyHeightFirstTotalOrder pins the height-then-CID ordering: a lower
// height always sorts first (grouping causal layers), and the CID breaks ties.
func TestSortKeyHeightFirstTotalOrder(t *testing.T) {
	cidLow := []byte{0x01}
	cidHigh := []byte{0xFF}

	// Lower height wins even when its CID is larger.
	require.Equal(t, -1, CompareBound(SortKey(1, cidHigh), SortKey(2, cidLow)))
	// Equal height -> the CID breaks the tie.
	require.Equal(t, -1, CompareBound(SortKey(5, cidLow), SortKey(5, cidHigh)))
}

func TestSortKeyHeightZeroIsPlainCIDOrder(t *testing.T) {
	a := []byte{0x01, 0x02}
	b := []byte{0x01, 0x03}
	require.Equal(t, bytes.Compare(a, b), CompareBound(SortKey(0, a), SortKey(0, b)))
}

func TestSortKeyStructure(t *testing.T) {
	cid := []byte("cid-bytes")
	key := SortKey(0x0102030405060708, cid)

	require.Len(t, key, SortKeyLen+len(cid))
	require.Equal(t, []byte{1, 2, 3, 4, 5, 6, 7, 8}, key[:SortKeyLen])
	require.Equal(t, cid, key[SortKeyLen:])
}

func TestCompareBoundSentinels(t *testing.T) {
	real := SortKey(7, []byte{0xAB})

	require.True(t, IsMaxBound(MaxBound()))
	require.False(t, IsMaxBound(MinBound()))
	require.False(t, IsMaxBound(real))

	require.Equal(t, -1, CompareBound(MinBound(), real), "min < real")
	require.Equal(t, 1, CompareBound(real, MinBound()))
	require.Equal(t, -1, CompareBound(real, MaxBound()), "real < max")
	require.Equal(t, 1, CompareBound(MaxBound(), real))
	require.Equal(t, -1, CompareBound(MinBound(), MaxBound()))
	require.Equal(t, 0, CompareBound(MaxBound(), MaxBound()))
	require.Equal(t, 0, CompareBound(MinBound(), MinBound()))
}

func TestItemOrdering(t *testing.T) {
	a := NewItem(1, []byte{0x01})
	b := NewItem(1, []byte{0x02})

	require.Equal(t, -1, CompareItems(a, b))
	require.Equal(t, []byte{0x01}, a.ID)
}
