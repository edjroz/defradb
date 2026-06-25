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

// Reconciliation caps, baked in from the start so a single misbehaving peer
// cannot cause unbounded work, allocation, or message size.
const (
	// BranchingFactor is the number of sub-ranges a responder splits a
	// mismatching Fingerprint range into. Higher fan-out means fewer rounds but
	// larger messages.
	BranchingFactor = 16

	// MaxRounds bounds the number of initiator-driven rounds per session. A peer
	// returning fingerprints that never agree therefore terminates with an error
	// (see [NewErrRoundCapExceeded]) instead of looping forever.
	MaxRounds = 32

	// IDListThreshold is the largest item count in a range for which a responder
	// emits an explicit IdList instead of splitting further. At or below it, the
	// range is listed; above it, the range is split into BranchingFactor buckets.
	IDListThreshold = 64

	// MaxIDsPerRange is the hard cap on IDs in a single IdList range. It is kept
	// equal to IDListThreshold so a listed range never exceeds the cap; the
	// constant is named separately so the list-vs-split threshold and the hard
	// frame guard can be tuned independently later.
	MaxIDsPerRange = 64

	// MaxRangesPerMessage bounds the number of ranges a single response may
	// carry. Once reached, the responder defers refinement of the remaining
	// keyspace (echoing an unsplit fingerprint) to a later round rather than
	// emitting an oversized message.
	MaxRangesPerMessage = 1 << 14 // 16384

	// MaxFrameBytes mirrors the eventual 16 MiB transport frame cap. No single
	// message produced by this package may exceed it.
	MaxFrameBytes = 16 << 20 // 16 MiB
)
