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
	"encoding/binary"
)

// SortKeyLen is the length of the big-endian height prefix of a sort key.
const SortKeyLen = 8

// SortKey returns the reconciliation sort key for an item: an 8-byte big-endian
// height followed by the raw CID bytes (heightBE8 || cid). The resulting keys
// order lexicographically, so ordering by height groups items into causal layers
// (helpful for later fetch ordering) while the appended CID breaks height ties
// and guarantees a strict total order.
//
// Heads-only callers that do not have (or need) a height may pass height == 0,
// which yields plain CID-byte order.
func SortKey(height uint64, cid []byte) []byte {
	key := make([]byte, SortKeyLen+len(cid))
	binary.BigEndian.PutUint64(key[:SortKeyLen], height)
	copy(key[SortKeyLen:], cid)
	return key
}

// Range bounds use two out-of-band sentinels alongside real sort keys:
//
//   - MinBound — the inclusive low end of the keyspace, an empty but non-nil
//     slice that orders before every real key.
//   - MaxBound — the exclusive "+infinity" high end, represented by a nil slice
//     (distinct from the empty MinBound) and recognized by IsMaxBound.
//
// Real sort keys are always non-nil and at least SortKeyLen bytes long, so nil is
// unambiguously the max sentinel. All bound comparisons in this package go
// through CompareBound so the sentinels are honored everywhere; never compare
// bounds with bytes.Compare directly.
//
// NOTE: this nil/empty distinction is an in-memory representation only. The wire
// (CBOR) encoding of the MaxBound sentinel is defined in a later phase, since
// nil and empty byte strings do not necessarily round-trip distinctly.

// MinBound returns the inclusive low end of the keyspace: an empty, non-nil slice
// that orders before every real sort key.
func MinBound() []byte { return []byte{} }

// MaxBound returns the exclusive "+infinity" high end of the keyspace, used as
// the upper bound of the final range tiling [MinBound, MaxBound).
func MaxBound() []byte { return nil }

// IsMaxBound reports whether b is the MaxBound ("+infinity") sentinel.
func IsMaxBound(b []byte) bool { return b == nil }

// CompareBound orders two range bounds, treating MaxBound as greater than every
// real key (and than MinBound). It returns -1, 0, or +1. Real keys (and the empty
// MinBound) are compared with bytes.Compare.
func CompareBound(a, b []byte) int {
	aMax, bMax := IsMaxBound(a), IsMaxBound(b)
	switch {
	case aMax && bMax:
		return 0
	case aMax:
		return 1
	case bMax:
		return -1
	default:
		return bytes.Compare(a, b)
	}
}
