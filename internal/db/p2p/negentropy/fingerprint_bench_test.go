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
	"fmt"
	"testing"
)

func benchVector(b *testing.B, n int) *Vector {
	b.Helper()
	builder := NewVectorBuilder(n)
	for i := 0; i < n; i++ {
		builder.Add(0, tid(i))
	}
	v, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	return v
}

// BenchmarkVectorFingerprintScan characterizes the cost of the literal O(n)
// full-range fingerprint scan as the set grows. It is the baseline the Phase-4
// maintained tree (O(log n) fingerprints) will be measured against.
func BenchmarkVectorFingerprintScan(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		v := benchVector(b, n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = v.Fingerprint(0, n)
			}
		})
	}
}
