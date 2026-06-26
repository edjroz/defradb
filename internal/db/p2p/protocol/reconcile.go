// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protocol

import (
	"github.com/sourcenetwork/defradb/internal/db/p2p/message"
	"github.com/sourcenetwork/defradb/internal/db/p2p/negentropy"
)

// Reconcile scope kinds. The kind selects which set a session reconciles.
const (
	// ScopeDocHeads reconciles a single document's composite heads, keyed by ID
	// (the docID). It is the only scope in the M1 (heads-only) slice.
	ScopeDocHeads uint8 = 1
)

// ReconcileScope identifies the set being reconciled in a session.
type ReconcileScope struct {
	Kind uint8
	ID   string
}

// ReconcileRange is the wire form of a [negentropy.Range]. It is a CBOR-friendly
// projection of the in-memory engine type: the engine never goes on the wire, so
// the codec lives here and keeps the negentropy package dependency-free.
//
// The UpperBound carries the two engine sentinels explicitly: MaxBound (+infinity)
// is signalled by IsMax (because nil and an empty byte string do not round-trip
// distinctly through CBOR — see the note in negentropy/sortkey.go), while the
// empty MinBound is just an empty UpperBound with IsMax false.
type ReconcileRange struct {
	IsMax       bool     `cbor:"1,keyasint"`
	UpperBound  []byte   `cbor:"2,keyasint,omitempty"`
	Mode        uint8    `cbor:"3,keyasint"`
	Fingerprint []byte   `cbor:"4,keyasint,omitempty"` // exactly FingerprintLen bytes when Mode==Fingerprint
	IDs         [][]byte `cbor:"5,keyasint,omitempty"` // raw cid.Cid.Bytes() when Mode==IdList
}

// ReconcileMessage is the libp2p message exchanged during range-based set
// reconciliation (Negentropy). The same shape is used for both the initiator's
// request and the responder's reply; it embeds [message.MetaData] to inherit
// signing and the 16 MiB frame cap. The session converges when every range is
// ModeSkip.
type ReconcileMessage struct {
	message.MetaData
	Scope  ReconcileScope
	Ranges []ReconcileRange
}

// ToWire projects an engine [negentropy.Message] into its wire form under the
// given scope.
func ToWire(scope ReconcileScope, m negentropy.Message) ReconcileMessage {
	ranges := make([]ReconcileRange, len(m.Ranges))
	for i := range m.Ranges {
		r := m.Ranges[i]
		w := ReconcileRange{Mode: uint8(r.Mode)}
		if negentropy.IsMaxBound(r.UpperBound) {
			w.IsMax = true
		} else {
			w.UpperBound = r.UpperBound
		}
		switch r.Mode {
		case negentropy.ModeFingerprint:
			w.Fingerprint = append([]byte(nil), r.Fingerprint[:]...)
		case negentropy.ModeIDList:
			w.IDs = r.IDs
		}
		ranges[i] = w
	}
	return ReconcileMessage{Scope: scope, Ranges: ranges}
}

// FromWire reconstructs an engine [negentropy.Message] from its wire form. It
// errors via [NewErrBadFingerprintLen] if a fingerprint range does not carry
// exactly [negentropy.FingerprintLen] bytes.
func FromWire(rm ReconcileMessage) (negentropy.Message, error) {
	ranges := make([]negentropy.Range, len(rm.Ranges))
	for i := range rm.Ranges {
		w := rm.Ranges[i]
		r := negentropy.Range{Mode: negentropy.Mode(w.Mode)}
		if w.IsMax {
			r.UpperBound = negentropy.MaxBound()
		} else if w.UpperBound == nil {
			// A non-max range always has a real (possibly empty MinBound) key;
			// normalize a decoded nil to the empty MinBound slice.
			r.UpperBound = negentropy.MinBound()
		} else {
			r.UpperBound = w.UpperBound
		}
		switch r.Mode {
		case negentropy.ModeFingerprint:
			if len(w.Fingerprint) != negentropy.FingerprintLen {
				return negentropy.Message{}, NewErrBadFingerprintLen(len(w.Fingerprint))
			}
			copy(r.Fingerprint[:], w.Fingerprint)
		case negentropy.ModeIDList:
			r.IDs = w.IDs
		}
		ranges[i] = r
	}
	return negentropy.Message{Ranges: ranges}, nil
}
