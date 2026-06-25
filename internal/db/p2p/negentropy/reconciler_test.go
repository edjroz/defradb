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

func TestConvergesEmptyVsEmpty(t *testing.T) {
	need, have, _ := runSession(t, vecFrom(t), vecFrom(t))
	require.Empty(t, need)
	require.Empty(t, have)
}

func TestConvergesEmptyVsFull(t *testing.T) {
	need, have, _ := runSession(t, vecFrom(t), vecFrom(t, seeds(1, 10)...))
	require.Equal(t, setOf(seeds(1, 10)), idSet(need))
	require.Empty(t, have)
}

func TestConvergesFullVsEmpty(t *testing.T) {
	need, have, _ := runSession(t, vecFrom(t, seeds(1, 10)...), vecFrom(t))
	require.Empty(t, need)
	require.Equal(t, setOf(seeds(1, 10)), idSet(have))
}

func TestConvergesDisjoint(t *testing.T) {
	need, have, _ := runSession(t, vecFrom(t, seeds(1, 10)...), vecFrom(t, seeds(11, 20)...))
	require.Equal(t, setOf(seeds(11, 20)), idSet(need))
	require.Equal(t, setOf(seeds(1, 10)), idSet(have))
}

func TestSingleElementDiff(t *testing.T) {
	a := vecFrom(t, seeds(1, 100)...)
	b := vecFrom(t, append(seeds(1, 100), 101)...)

	need, have, _ := runSession(t, a, b)

	require.Equal(t, setOf([]int{101}), idSet(need))
	require.Empty(t, have)
}

// TestBytesScaleWithSymmetricDifference is the core O(diff) claim: with a fixed
// shared prefix, bytes exchanged grow with |A△B| and are far below a full
// union transfer when the diff is small.
func TestBytesScaleWithSymmetricDifference(t *testing.T) {
	const n = 1000
	shared := seeds(1, n)

	_, _, bytes0 := runSession(t, vecFrom(t, shared...), vecFrom(t, shared...))

	aExtra := append(append([]int{}, shared...), n+1)
	bExtra := append(append([]int{}, shared...), n+2)
	need2, have2, bytes2 := runSession(t, vecFrom(t, aExtra...), vecFrom(t, bExtra...))
	require.Equal(t, setOf([]int{n + 2}), idSet(need2))
	require.Equal(t, setOf([]int{n + 1}), idSet(have2))

	_, _, bytesN := runSession(t, vecFrom(t, shared...), vecFrom(t, seeds(n+1, 2*n)...))

	require.Less(t, bytes0, bytes2, "diff=0 is cheaper than diff=2")
	require.Less(t, bytes2, bytesN, "bytes grow with the symmetric difference")
	require.Less(t, bytes2, bytesN/2, "a small diff is far cheaper than transferring the union")
}

func TestConvergesIdenticalLargeSetInConstantBytes(t *testing.T) {
	// diff=0 should settle in a couple of messages regardless of set size.
	_, _, smallN := runSession(t, vecFrom(t, seeds(1, 50)...), vecFrom(t, seeds(1, 50)...))
	_, _, largeN := runSession(t, vecFrom(t, seeds(1, 5000)...), vecFrom(t, seeds(1, 5000)...))
	require.Equal(t, smallN, largeN, "identical sets reconcile in constant bytes")
}

// TestGarbageFingerprintTerminates pins the round-cap safety net: a peer that
// never agrees forces termination within MaxRounds, with an error, rather than
// looping forever.
func TestGarbageFingerprintTerminates(t *testing.T) {
	it := NewInitiator(vecFrom(t, seeds(1, 100)...))
	_ = it.Initiate()

	var done bool
	for r := 0; r < MaxRounds+5; r++ {
		var fp Fingerprint
		copy(fp[:], tid(r)) // garbage, varies per round, never matches
		garbage := Message{Ranges: []Range{{UpperBound: MaxBound(), Mode: ModeFingerprint, Fingerprint: fp}}}
		_, done = it.Reconcile(garbage)
		if done {
			break
		}
	}

	require.True(t, done)
	require.Error(t, it.Err())
	require.LessOrEqual(t, it.Round(), MaxRounds+1)
}
