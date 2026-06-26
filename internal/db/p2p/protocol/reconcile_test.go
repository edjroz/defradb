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
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/internal/db/p2p/negentropy"
)

// fp16 builds a non-zero 16-byte fingerprint from a seed byte.
func fp16(seed byte) negentropy.Fingerprint {
	var f negentropy.Fingerprint
	for i := range f {
		f[i] = seed + byte(i)
	}
	return f
}

// roundTrip sends a negentropy.Message through ToWire -> CBOR -> FromWire and
// returns the recovered message, mirroring the real send/receive path.
func roundTrip(t *testing.T, scope ReconcileScope, m negentropy.Message) negentropy.Message {
	t.Helper()
	wire := ToWire(scope, m)

	data, err := cbor.Marshal(wire)
	require.NoError(t, err)

	var decoded ReconcileMessage
	require.NoError(t, cbor.Unmarshal(data, &decoded))
	require.Equal(t, scope, decoded.Scope, "scope must survive the wire")

	got, err := FromWire(decoded)
	require.NoError(t, err)
	return got
}

func TestReconcileWire_RoundTripAllModes(t *testing.T) {
	scope := ReconcileScope{Kind: ScopeDocHeads, ID: "bafyDoc"}

	// A message tiling [MinBound, MaxBound) with one of every mode, the final
	// range carrying the MaxBound (+inf) sentinel.
	msg := negentropy.Message{Ranges: []negentropy.Range{
		{UpperBound: negentropy.SortKey(0, []byte("aaaa")), Mode: negentropy.ModeSkip},
		{UpperBound: negentropy.SortKey(0, []byte("mmmm")), Mode: negentropy.ModeFingerprint, Fingerprint: fp16(7)},
		{
			UpperBound: negentropy.MaxBound(),
			Mode:       negentropy.ModeIDList,
			IDs:        [][]byte{[]byte("cid-one"), []byte("cid-two")},
		},
	}}

	got := roundTrip(t, scope, msg)
	require.Equal(t, msg, got)

	// The final range's MaxBound must come back as the nil sentinel, not an empty
	// slice (CBOR does not distinguish them without the IsMax flag).
	require.True(t, negentropy.IsMaxBound(got.Ranges[2].UpperBound))
}

func TestReconcileWire_MinBoundNotConfusedWithMaxBound(t *testing.T) {
	// A single non-final range whose UpperBound is the empty MinBound slice must
	// NOT be decoded as MaxBound.
	msg := negentropy.Message{Ranges: []negentropy.Range{
		{UpperBound: negentropy.MinBound(), Mode: negentropy.ModeSkip},
		{UpperBound: negentropy.MaxBound(), Mode: negentropy.ModeSkip},
	}}

	got := roundTrip(t, ReconcileScope{Kind: ScopeDocHeads, ID: "d"}, msg)
	require.Len(t, got.Ranges, 2)
	require.False(t, negentropy.IsMaxBound(got.Ranges[0].UpperBound), "MinBound must stay a real (empty) key")
	require.Equal(t, negentropy.MinBound(), got.Ranges[0].UpperBound)
	require.True(t, negentropy.IsMaxBound(got.Ranges[1].UpperBound))
}

func TestReconcileWire_EmptyMessage(t *testing.T) {
	got := roundTrip(t, ReconcileScope{Kind: ScopeDocHeads, ID: "d"}, negentropy.Message{})
	require.Empty(t, got.Ranges)
}

func TestFromWire_RejectsBadFingerprintLength(t *testing.T) {
	bad := ReconcileMessage{
		Scope: ReconcileScope{Kind: ScopeDocHeads, ID: "d"},
		Ranges: []ReconcileRange{
			{IsMax: true, Mode: uint8(negentropy.ModeFingerprint), Fingerprint: make([]byte, 15)},
		},
	}
	_, err := FromWire(bad)
	require.Error(t, err)
}
