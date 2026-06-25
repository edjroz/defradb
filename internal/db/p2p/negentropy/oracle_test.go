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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// randomSets partitions a fixed universe into two overlapping sets with a
// controlled mix of A-only, B-only, shared, and absent elements.
func randomSets(r *rand.Rand) (a, b []int) {
	const universe = 200
	for i := 1; i <= universe; i++ {
		switch r.Intn(4) {
		case 0:
			a = append(a, i)
		case 1:
			b = append(b, i)
		case 2:
			a = append(a, i)
			b = append(b, i)
		default:
			// absent from both
		}
	}
	return a, b
}

func setDiff(x, y map[string]bool) map[string]bool {
	out := make(map[string]bool)
	for k := range x {
		if !y[k] {
			out[k] = true
		}
	}
	return out
}

// TestReconcileMatchesNaiveSetDiff is the oracle property test: for random
// overlapping sets, the reconciled need/have must equal the naive in-memory set
// difference. The oracle is a plain map diff (not syncDAG — that integration
// oracle belongs to a later phase).
func TestReconcileMatchesNaiveSetDiff(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		r := rand.New(rand.NewSource(seed))
		aSeeds, bSeeds := randomSets(r)

		need, have, _ := runSession(t, vecFrom(t, aSeeds...), vecFrom(t, bSeeds...))

		aSet, bSet := setOf(aSeeds), setOf(bSeeds)
		require.Equal(t, setDiff(bSet, aSet), idSet(need), "need == B\\A (seed %d)", seed)
		require.Equal(t, setDiff(aSet, bSet), idSet(have), "have == A\\B (seed %d)", seed)
	}
}
