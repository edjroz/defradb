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

// Mode selects how a [Range] is reconciled.
type Mode uint8

const (
	// ModeSkip marks a range both peers agree on: nothing to do.
	ModeSkip Mode = iota
	// ModeFingerprint carries a [Fingerprint] to compare; a mismatch is split or
	// listed.
	ModeFingerprint
	// ModeIDList carries the explicit item IDs the sender holds in the range. An
	// empty ID list means "I hold nothing in this range".
	ModeIDList
)

// Deterministic per-element byte overheads used by [Message.EncodedSize]. These
// are not the exact CBOR wire sizes (that lands with the wire codec in a later
// phase); they are a stable approximation good enough to assert that bytes
// exchanged scale with the symmetric difference rather than the union.
const (
	modeTagSize  = 1 // one mode tag byte per range
	boundFraming = 1 // length-prefix framing per range upper bound
	idFraming    = 1 // length-prefix framing per id in an id list
)

// Range is one contiguous slice of the keyspace. Its lower bound is the previous
// range's UpperBound (the first range's lower bound is MinBound); UpperBound is
// exclusive, and the final range's UpperBound is MaxBound. A message's ranges
// tile [MinBound, MaxBound) completely and in ascending order.
type Range struct {
	// UpperBound is the exclusive sort-key upper bound of this range; MaxBound on
	// the final range.
	UpperBound []byte
	// Mode selects how this range is handled.
	Mode Mode
	// Fingerprint is valid when Mode == ModeFingerprint.
	Fingerprint Fingerprint
	// IDs are the raw CID bytes in ascending order, valid when Mode == ModeIDList.
	IDs [][]byte
}

// Message is an ordered list of contiguous ranges tiling [MinBound, MaxBound).
type Message struct {
	Ranges []Range
}

// IsAllSkip reports whether every range is ModeSkip — the convergence sentinel.
// An empty message is vacuously all-skip.
func (m Message) IsAllSkip() bool {
	for i := range m.Ranges {
		if m.Ranges[i].Mode != ModeSkip {
			return false
		}
	}
	return true
}

// EncodedSize returns a deterministic byte size for the message used by the
// in-memory counting transport: per range it counts the upper-bound bytes plus
// framing and a mode tag, plus the mode payload — a 16-byte fingerprint for
// ModeFingerprint, or the summed ID bytes (with per-id framing) for ModeIDList.
func (m Message) EncodedSize() int {
	total := 0
	for i := range m.Ranges {
		total += m.Ranges[i].encodedSize()
	}
	return total
}

func (r Range) encodedSize() int {
	size := len(r.UpperBound) + boundFraming + modeTagSize
	switch r.Mode {
	case ModeFingerprint:
		size += FingerprintLen
	case ModeIDList:
		for _, id := range r.IDs {
			size += len(id) + idFraming
		}
	}
	return size
}
