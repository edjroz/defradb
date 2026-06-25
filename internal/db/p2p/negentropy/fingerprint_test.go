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

func TestFingerprintOrderIndependent(t *testing.T) {
	a, b, c := []byte("alpha"), []byte("bravo"), []byte("charlie")

	require.Equal(t, FingerprintOf(a, b, c), FingerprintOf(c, a, b))
	require.Equal(t, FingerprintOf(a, b, c), FingerprintOf(b, c, a))
}

// TestFingerprintCombinable is the algebra that lets a range fingerprint be
// derived from its sub-ranges' partial sums: fp(A∪B) == Merge(acc(A), acc(B)).
func TestFingerprintCombinable(t *testing.T) {
	idsA := [][]byte{[]byte("a1"), []byte("a2")}
	idsB := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3")}

	var accA Accumulator
	for _, x := range idsA {
		accA.Add(x)
	}
	var accB Accumulator
	for _, x := range idsB {
		accB.Add(x)
	}
	accA.Merge(accB)

	all := append(append([][]byte{}, idsA...), idsB...)
	require.Equal(t, FingerprintOf(all...), accA.Finalize())
	require.Equal(t, uint64(5), accA.Count())
}

func TestEmptyFingerprintIdentity(t *testing.T) {
	require.Equal(t, EmptyFingerprint(), Accumulator{}.Finalize())
	require.Equal(t, EmptyFingerprint(), FingerprintOf())
}

// TestCountFoldDefeatsCancellation pins that the item count is folded into the
// outer hash: two ranges with the same additive sum but different counts must
// produce different fingerprints (white-box construction of the sum).
func TestCountFoldDefeatsCancellation(t *testing.T) {
	var s acc256
	s[31] = 0x42

	one := Accumulator{sum: s, count: 1}
	two := Accumulator{sum: s, count: 2}

	require.NotEqual(t, one.Finalize(), two.Finalize())
}

func TestFingerprintLength(t *testing.T) {
	require.Equal(t, 16, FingerprintLen)
}
