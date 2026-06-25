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
	"testing"

	"github.com/stretchr/testify/require"
)

func mkVector(t *testing.T, items ...Item) *Vector {
	t.Helper()
	v, err := NewVector(items)
	require.NoError(t, err)
	return v
}

func TestVectorBuildSortsAndSeals(t *testing.T) {
	v, err := NewVector([]Item{
		NewItem(2, []byte{0x01}),
		NewItem(1, []byte{0x09}),
		NewItem(1, []byte{0x01}),
	})
	require.NoError(t, err)
	require.Equal(t, 3, v.Size())

	// Sorted ascending by sort key: (h1,c01) < (h1,c09) < (h2,c01).
	require.Equal(t, []byte{0x01}, v.ItemID(0))
	require.Equal(t, []byte{0x09}, v.ItemID(1))
	require.Equal(t, []byte{0x01}, v.ItemID(2))
}

func TestVectorBuildRejectsDuplicates(t *testing.T) {
	_, err := NewVector([]Item{
		NewItem(1, []byte{0x01}),
		NewItem(1, []byte{0x01}),
	})
	require.Error(t, err)
}

func TestVectorSeekExclusiveBoundary(t *testing.T) {
	v := mkVector(t,
		NewItem(0, []byte{0x10}),
		NewItem(0, []byte{0x20}),
		NewItem(0, []byte{0x30}),
	)

	require.Equal(t, 0, v.Seek(MinBound()))
	require.Equal(t, 3, v.Seek(MaxBound()))
	require.Equal(t, 1, v.Seek(SortKey(0, []byte{0x20})), "exact key -> its own index (>=)")
	require.Equal(t, 2, v.Seek(SortKey(0, []byte{0x25})), "between items")
	require.Equal(t, 3, v.Seek(SortKey(0, []byte{0xFF})), "past the end")
}

func TestVectorFingerprintMatchesScan(t *testing.T) {
	a := NewItem(0, []byte("a"))
	b := NewItem(0, []byte("b"))
	c := NewItem(0, []byte("c"))
	v := mkVector(t, a, b, c)

	require.Equal(t, FingerprintOf(a.ID, b.ID, c.ID), v.Fingerprint(0, 3))
	require.Equal(t, FingerprintOf(b.ID), v.Fingerprint(1, 2))
	require.Equal(t, EmptyFingerprint(), v.Fingerprint(1, 1))
}

func TestVectorAccumulateCombines(t *testing.T) {
	v := mkVector(t,
		NewItem(0, []byte("a")),
		NewItem(0, []byte("b")),
		NewItem(0, []byte("c")),
		NewItem(0, []byte("d")),
	)

	full := v.Accumulate(0, 4)
	left := v.Accumulate(0, 2)
	right := v.Accumulate(2, 4)
	left.Merge(right)

	require.Equal(t, full.Finalize(), left.Finalize())
}
