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

func benchSegmentTree(b *testing.B, n int) *SegmentTree {
	b.Helper()
	builder := NewSegmentTreeBuilder(n)
	for i := 0; i < n; i++ {
		builder.Add(0, tid(i))
	}
	st, err := builder.Build()
	if err != nil {
		b.Fatal(err)
	}
	return st
}

// BenchmarkVectorFingerprintScan characterizes the cost of the literal O(n)
// full-range fingerprint scan as the set grows.
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

// BenchmarkSegmentTreeBuild measures the O(n) per-session build cost (n Add calls
// plus the bottom-up Build) that BenchmarkSegmentTreeFingerprint excludes. The
// session pays this once at start, then amortizes it across many O(log n)
// fingerprints; the gate weighs this build against the per-session query savings
// to decide whether the build-per-session design needs a maintained-across-writes
// tree (the Phase-5 pivot).
func BenchmarkSegmentTreeBuild(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				builder := NewSegmentTreeBuilder(n)
				for j := 0; j < n; j++ {
					builder.Add(0, tid(j))
				}
				if _, err := builder.Build(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSegmentTreeFingerprint measures the O(log n) full-range fingerprint of
// the segment tree against the Vector's O(n) scan above — the Phase-4b speedup.
// (Build cost is excluded; it is amortized across a session's many fingerprints.)
func BenchmarkSegmentTreeFingerprint(b *testing.B) {
	for _, n := range []int{100, 1000, 10000, 100000} {
		st := benchSegmentTree(b, n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = st.Fingerprint(0, n)
			}
		})
	}
}
