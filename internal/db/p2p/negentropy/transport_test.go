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
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// tid returns a deterministic 32-byte item ID for an integer seed.
func tid(n int) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	h := sha256.Sum256(b[:])
	return h[:]
}

// seeds returns the inclusive integer range [lo, hi].
func seeds(lo, hi int) []int {
	s := make([]int, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		s = append(s, i)
	}
	return s
}

// vecFrom builds a sealed Vector (all at height 0) from integer seeds.
func vecFrom(t *testing.T, intSeeds ...int) *Vector {
	t.Helper()
	b := NewVectorBuilder(len(intSeeds))
	for _, s := range intSeeds {
		b.Add(0, tid(s))
	}
	v, err := b.Build()
	require.NoError(t, err)
	return v
}

// idSet collapses a list of IDs into a set keyed by their bytes.
func idSet(ids [][]byte) map[string]bool {
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[string(id)] = true
	}
	return m
}

// setOf returns the set of IDs for the given integer seeds.
func setOf(intSeeds []int) map[string]bool {
	m := make(map[string]bool, len(intSeeds))
	for _, s := range intSeeds {
		m[string(tid(s))] = true
	}
	return m
}

// assertTiles verifies a message's ranges tile [MinBound, MaxBound) in strict
// ascending order with the final range ending at MaxBound.
func assertTiles(t *testing.T, m Message) {
	t.Helper()
	if len(m.Ranges) == 0 {
		return
	}
	prev := MinBound()
	for k := range m.Ranges {
		r := m.Ranges[k]
		if k == len(m.Ranges)-1 {
			require.True(t, IsMaxBound(r.UpperBound), "final range must end at MaxBound")
		} else {
			require.False(t, IsMaxBound(r.UpperBound), "non-final range must have a real bound")
			require.Equal(t, -1, CompareBound(prev, r.UpperBound), "bounds must strictly ascend")
			prev = r.UpperBound
		}
	}
}

// runSession drives a full reconciliation between an initiator holding set a and
// a responder holding set b, returning the initiator's need/have plus the total
// bytes exchanged. Every message is checked for the tiling invariant, and the
// loop is bounded so a non-converging bug fails fast instead of hanging.
func runSession(t *testing.T, a, b *Vector) (need, have [][]byte, bytesExchanged int) {
	t.Helper()
	it := NewInitiator(a)
	msg := it.Initiate()
	assertTiles(t, msg)
	bytesExchanged += msg.EncodedSize()

	for round := 0; ; round++ {
		require.Less(t, round, MaxRounds+5, "session failed to converge")

		resp, err := Respond(b, msg)
		require.NoError(t, err)
		assertTiles(t, resp)
		bytesExchanged += resp.EncodedSize()

		next, done := it.Reconcile(resp)
		require.NoError(t, it.Err())
		if done {
			break
		}
		assertTiles(t, next)
		bytesExchanged += next.EncodedSize()
		msg = next
	}
	return it.Need(), it.Have(), bytesExchanged
}

func TestCountingTransportConverges(t *testing.T) {
	need, have, bytesExchanged := runSession(t, vecFrom(t, 1, 2, 3), vecFrom(t, 2, 3, 4))

	require.Equal(t, setOf([]int{4}), idSet(need), "initiator needs what only the responder has")
	require.Equal(t, setOf([]int{1}), idSet(have), "initiator has what only it holds")
	require.Positive(t, bytesExchanged)
}
