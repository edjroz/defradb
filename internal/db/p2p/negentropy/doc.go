// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package negentropy implements the transport-agnostic core of range-based set
// reconciliation (RBSR, a.k.a. Negentropy): an anti-entropy protocol that lets
// two peers discover the difference between their sets while exchanging data
// proportional to the size of that difference (|A△B|), not the size of the sets
// (|A∪B|). It converges in O(log n) rounds with O(diff·log n) bytes.
//
// This package is pure and standalone: it has no network, libp2p, or DefraDB
// dependencies and treats an item ID (a CID) as opaque bytes. A later phase wraps
// it in a signed CBOR comm-channel and feeds it from the block head/DAG store;
// the actual block transfer driven by the discovered sets is also out of scope
// here. See README.md for worked examples, a round-by-round walkthrough, and the
// full invariant list.
//
// # Sort key and fingerprint
//
// Items are ordered by a sort key, heightBE8 || cid: an 8-byte big-endian block
// height followed by the raw CID bytes (see [SortKey]). Ordering by height groups
// items into causal layers while the CID breaks ties for a strict total order.
//
// A range is summarized by a 16-byte [Fingerprint]:
//
//	fp = SHA256( (Σ SHA256(cid)) mod 2^256 || uvarint(count) )[:16]
//
// The inner sum is additive modulo 2^256, which is associative and commutative,
// so the fingerprints of adjacent ranges combine from their partial sums (see
// [Accumulator.Merge]). The item count is folded into the outer hash to defeat
// pairwise cancellation that a plain additive or XOR sum would admit.
//
// # Protocol
//
// A session is driven by a single [Initiator] against a stateless responder
// ([Respond]). The initiator opens with one full-range Fingerprint ([Initiator.Initiate])
// and then, each round ([Initiator.Reconcile]), maps every incoming range to one
// outgoing range, never changing the tiling. Only the responder splits a
// mismatching Fingerprint range into [BranchingFactor] sub-ranges, or lists a
// small mismatching range as an explicit IdList. The session converges when the
// initiator's outgoing message is all-Skip. A single initiator-driven session
// makes the initiator learn both its need-set ([Initiator.Need], to pull) and its
// have-set ([Initiator.Have], to push); the responder learns nothing.
//
// # Storage seam
//
// The reconciler queries the local set only through the [Storage] interface, so
// the fingerprint backend can change without touching the protocol. This phase
// ships [Vector], an O(n)-scan implementation; a later phase plugs in a
// maintained tree giving O(log n) fingerprints behind the same interface.
package negentropy
