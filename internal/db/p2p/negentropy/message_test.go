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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessageIsAllSkip(t *testing.T) {
	require.True(t, Message{}.IsAllSkip())
	require.True(t, Message{Ranges: []Range{{Mode: ModeSkip}, {Mode: ModeSkip}}}.IsAllSkip())
	require.False(t, Message{Ranges: []Range{{Mode: ModeSkip}, {Mode: ModeFingerprint}}}.IsAllSkip())
}

func TestEncodedSizeDeterministicAndScales(t *testing.T) {
	fpRange := Range{UpperBound: SortKey(1, []byte{0x01}), Mode: ModeFingerprint}
	fpMsg := Message{Ranges: []Range{fpRange}}

	require.Equal(t, fpMsg.EncodedSize(), fpMsg.EncodedSize(), "deterministic")
	require.Greater(t, fpMsg.EncodedSize(), FingerprintLen, "carries a 16-byte fingerprint plus framing")

	// IdList size grows with the number of IDs carried.
	small := Message{Ranges: []Range{{UpperBound: MaxBound(), Mode: ModeIDList, IDs: [][]byte{{0x01}}}}}
	big := Message{Ranges: []Range{{UpperBound: MaxBound(), Mode: ModeIDList,
		IDs: [][]byte{{0x01}, {0x02}, {0x03}, {0x04}}}}}
	require.Greater(t, big.EncodedSize(), small.EncodedSize())

	// A settled (Skip) range is cheaper than a Fingerprint range over the same bound.
	skip := Message{Ranges: []Range{{UpperBound: fpRange.UpperBound, Mode: ModeSkip}}}
	require.Less(t, skip.EncodedSize(), fpMsg.EncodedSize())
}
