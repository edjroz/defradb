// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package reconcile contrasts the control-plane cost of discovering which blocks
// two peers are missing under (a) the default DefraDB sync — modelled as a naive
// full-identifier-set exchange — versus (b) the range-based set reconciliation
// (Negentropy) engine measured directly from internal/db/p2p/negentropy.
//
// Both approaches end up transferring the same payload: the d blocks that make up
// the symmetric difference. What differs is the *control* traffic spent finding
// that difference. The default has no set-reconciliation primitive, so to learn
// which of n items a peer lacks it must communicate the identity of the whole set
// (O(n)); Negentropy narrows in on the difference with O(d log n) control bytes.
//
// This file is the data generator: it drives the real engine over an in-memory
// transport, models the baseline at the same protocol abstraction, and writes a
// CSV consumed by plot.py. It is gated behind DEFRA_RECONCILE_REPORT so it never
// runs in the normal test lane.
package reconcile

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ng "github.com/sourcenetwork/defradb/internal/db/p2p/negentropy"
)

// idSize is the per-item identifier length, matching the 32-byte SHA-256 CIDs the
// Negentropy Vector stores. idFraming mirrors negentropy's per-id list framing so
// the baseline and the engine are measured with the same per-id accounting.
const (
	idSize    = sha256.Size // 32
	idFraming = 1
	idUnit    = idSize + idFraming // bytes to name one item in either approach
)

// disjoint seed bases keep the shared / A-only / B-only item sets from colliding.
const (
	sharedBase = 0
	aOnlyBase  = 1_000_000
	bOnlyBase  = 2_000_000
)

// tid is the deterministic 32-byte identifier for an integer seed (the same
// construction the negentropy tests use, re-stated here so this package does not
// depend on that package's test helpers).
func tid(n int) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(n))
	h := sha256.Sum256(b[:])
	return h[:]
}

// vecOf builds a sealed height-0 Vector from a list of integer seeds.
func vecOf(t *testing.T, seeds []int) *ng.Vector {
	t.Helper()
	b := ng.NewVectorBuilder(len(seeds))
	for _, s := range seeds {
		b.Add(0, tid(s))
	}
	v, err := b.Build()
	if err != nil {
		t.Fatalf("build vector: %v", err)
	}
	return v
}

// seedRange returns base, base+1, ..., base+count-1.
func seedRange(base, count int) []int {
	s := make([]int, 0, count)
	for i := 0; i < count; i++ {
		s = append(s, base+i)
	}
	return s
}

// scenario is one (shared, divergence) configuration. aOnly + bOnly == d, the
// symmetric difference; nA is the initiator's set size.
type scenario struct {
	sweep  string
	shared int
	aOnly  int
	bOnly  int
}

func (s scenario) d() int  { return s.aOnly + s.bOnly }
func (s scenario) nA() int { return s.shared + s.aOnly }
func (s scenario) nB() int { return s.shared + s.bOnly }

// result holds one approach's measured/modelled cost for a scenario.
type result struct {
	controlBytes int
	roundTrips   int
	need         int
	have         int
}

// measureNegentropy drives a full reconciliation session through the real engine
// and returns the control bytes exchanged (summed Message.EncodedSize across every
// message), the number of initiator rounds, and the resulting need/have sizes.
func measureNegentropy(t *testing.T, a, b *ng.Vector) result {
	t.Helper()
	it := ng.NewInitiator(a)
	msg := it.Initiate()
	bytes := msg.EncodedSize()

	for {
		resp, err := ng.Respond(b, msg)
		if err != nil {
			t.Fatalf("responder: %v", err)
		}
		bytes += resp.EncodedSize()

		next, done := it.Reconcile(resp)
		if err := it.Err(); err != nil {
			t.Fatalf("initiator: %v", err)
		}
		if done {
			break
		}
		bytes += next.EncodedSize()
		msg = next
	}
	return result{
		controlBytes: bytes,
		roundTrips:   it.Round(),
		need:         len(it.Need()),
		have:         len(it.Have()),
	}
}

// modelBaseline models the default sync's control cost as a naive full-identifier
// exchange: the initiator sends its entire id list (nA ids); the responder, which
// can now diff locally, replies with the d ids that name the symmetric difference
// (the need ids the initiator lacks plus the have ids it must push). That is one
// round-trip but O(nA) bytes, flat in the difference size.
//
// This is a deliberately *generous* lower bound on the real default: an actual DAG
// walk re-fetches block headers (CID + links + height), not bare CIDs, and gossips
// per-head, so it costs at least this much. Holding the per-id unit identical to
// the engine's keeps the comparison apples-to-apples.
func modelBaseline(s scenario) result {
	return result{
		controlBytes: (s.nA() + s.d()) * idUnit,
		roundTrips:   1,
		need:         s.bOnly,
		have:         s.aOnly,
	}
}

// scenarios returns the full sweep set across the three plots.
func scenarios() []scenario {
	var out []scenario

	// Sweep 1 — control cost vs set size n at a fixed, tiny difference (d=2, one
	// unique item per side). This is the headline: it isolates how each approach
	// scales with the set as the difference stays constant.
	for _, n := range []int{100, 300, 1000, 3000, 10000, 30000, 100000} {
		out = append(out, scenario{sweep: "bytes_vs_n", shared: n, aOnly: 1, bOnly: 1})
	}

	// Sweep 2 — control cost vs difference size d at a fixed set size (n=10000).
	// Shows Negentropy scaling with d and the crossover where, once the difference
	// approaches the union, full exchange stops being the more expensive option.
	for _, d := range []int{2, 10, 50, 100, 500, 1000, 5000, 20000} {
		out = append(out, scenario{sweep: "bytes_vs_d", shared: 10000, aOnly: d / 2, bOnly: d - d/2})
	}

	return out
}

// TestGenerateComparisonData drives the engine across every scenario, models the
// baseline, and writes data/comparison.csv. Enable with DEFRA_RECONCILE_REPORT=1.
func TestGenerateComparisonData(t *testing.T) {
	if os.Getenv("DEFRA_RECONCILE_REPORT") == "" {
		t.Skip("set DEFRA_RECONCILE_REPORT=1 to regenerate the comparison dataset")
	}

	var b strings.Builder
	b.WriteString("sweep,n,shared,d,approach,control_bytes,round_trips,need,have,payload_items\n")

	for _, sc := range scenarios() {
		a := vecOf(t, append(seedRange(sharedBase, sc.shared), seedRange(aOnlyBase, sc.aOnly)...))
		bb := vecOf(t, append(seedRange(sharedBase, sc.shared), seedRange(bOnlyBase, sc.bOnly)...))

		ngRes := measureNegentropy(t, a, bb)
		blRes := modelBaseline(sc)

		// Correctness guard: the engine must recover the exact symmetric difference.
		if ngRes.need != sc.bOnly || ngRes.have != sc.aOnly {
			t.Fatalf("scenario %+v: engine diff need=%d have=%d, want need=%d have=%d",
				sc, ngRes.need, ngRes.have, sc.bOnly, sc.aOnly)
		}

		writeRow(&b, sc, "negentropy", ngRes)
		writeRow(&b, sc, "baseline", blRes)

		t.Logf("%-12s n=%-7d d=%-6d  negentropy=%8dB/%dRT  baseline=%9dB/%dRT  reduction=%.0fx",
			sc.sweep, sc.nA(), sc.d(), ngRes.controlBytes, ngRes.roundTrips,
			blRes.controlBytes, blRes.roundTrips,
			float64(blRes.controlBytes)/float64(ngRes.controlBytes))
	}

	out := filepath.Join("data", "comparison.csv")
	if err := os.WriteFile(out, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write %s: %v", out, err)
	}
	t.Logf("wrote %s", out)
}

func writeRow(b *strings.Builder, sc scenario, approach string, r result) {
	fmt.Fprintf(b, "%s,%d,%d,%d,%s,%d,%d,%d,%d,%d\n",
		sc.sweep, sc.nA(), sc.shared, sc.d(), approach,
		r.controlBytes, r.roundTrips, r.need, r.have, sc.d())
}
