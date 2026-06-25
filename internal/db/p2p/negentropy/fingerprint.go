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
)

// FingerprintLen is the truncated digest length of a [Fingerprint]. RFC: 16 bytes.
const FingerprintLen = 16

// Fingerprint is a 16-byte digest summarizing the set of item IDs in a range.
// Two ranges holding the same set of IDs have equal fingerprints regardless of
// order. Truncating to 16 bytes keeps the wire form compact while retaining
// negligible collision odds at the range sizes reconciliation deals with.
type Fingerprint [FingerprintLen]byte

// Accumulator incrementally summarizes a set of item IDs so the fingerprints of
// disjoint ranges can be combined without rescanning. It holds
//
//	Σ SHA256(id)  (mod 2^256)
//
// together with the item count. The zero value is the empty accumulator, whose
// [Accumulator.Finalize] equals [EmptyFingerprint].
type Accumulator struct {
	sum   acc256
	count uint64
}

// Add folds one item ID (the raw CID bytes) into the accumulator: it adds
// SHA256(id) into the running sum and increments the count. Because the sum is
// modular addition, folding the same set of IDs in any order yields the same
// accumulator.
func (a *Accumulator) Add(id []byte) {
	h := sha256.Sum256(id)
	a.sum.addInto(&h)
	a.count++
}

// Merge folds another accumulator into a: sum += b.sum (mod 2^256) and
// count += b.count. This is the O(1) combinability that lets a range fingerprint
// be derived from its sub-ranges' partial sums — i.e. fp(A∪B) is obtainable from
// Merge of acc(A) and acc(B), without rescanning either range. Phase 4's
// maintained fingerprint tree relies on this.
func (a *Accumulator) Merge(b Accumulator) {
	a.sum.addAcc(&b.sum)
	a.count += b.count
}

// Count returns the number of IDs folded into the accumulator.
func (a Accumulator) Count() uint64 { return a.count }

// Finalize produces the range fingerprint:
//
//	SHA256( sum32 || uvarint(count) )  truncated to FingerprintLen bytes
//
// The count is folded into the outer hash so that two distinct sets which happen
// to share the same additive sum (pairwise cancellation) still produce different
// fingerprints. A plain additive or XOR sum without the count fold would be
// vulnerable to such cancellation.
func (a Accumulator) Finalize() Fingerprint {
	var countBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(countBuf[:], a.count)

	h := sha256.New()
	h.Write(a.sum[:])
	h.Write(countBuf[:n])
	digest := h.Sum(nil)

	var fp Fingerprint
	copy(fp[:], digest[:FingerprintLen])
	return fp
}

// EmptyFingerprint is the fingerprint of an empty range:
// SHA256(0^32 || uvarint(0)) truncated to FingerprintLen bytes. It is equal to
// (Accumulator{}).Finalize() and serves as the identity element.
func EmptyFingerprint() Fingerprint {
	return Accumulator{}.Finalize()
}

// FingerprintOf folds the given IDs into a fresh accumulator and finalizes it.
// It is a convenience for the [Vector] scan path and for tests.
func FingerprintOf(ids ...[]byte) Fingerprint {
	var a Accumulator
	for _, id := range ids {
		a.Add(id)
	}
	return a.Finalize()
}
