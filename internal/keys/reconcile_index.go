// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import (
	"encoding/binary"
)

const (
	// ReconcileIndexShortIDLen is the fixed-width big-endian collection-short-ID
	// prefix length of a reconcile-index key.
	ReconcileIndexShortIDLen = 4
	// reconcileIndexHeightLen is the fixed-width big-endian block-height segment.
	reconcileIndexHeightLen = 8
	// reconcileIndexPrefixLen is the offset of the CID segment within a key.
	reconcileIndexPrefixLen = ReconcileIndexShortIDLen + reconcileIndexHeightLen
)

// The reconcile-index store (namespace 'r') holds one entry per composite block in a
// collection, keyed BINARY (not the slash-delimited string form used by the
// headstore) so a lexicographic prefix scan yields the reconciliation sort order:
//
//	key   = collectionShortID (4-byte BE) || height (8-byte BE) || cidBytes
//	value = (empty)
//
// Ordering by the fixed-width height then the CID groups a collection's blocks into
// causal layers and gives a strict total order, consumed directly by the Phase-4a
// Vector build and the Phase-4b maintained tree. The (height||cid) layout mirrors the
// negentropy sort key but is defined independently here — the Vector recomputes its
// own sort key from (height, cid), so the bytes need not match.

// ReconcileIndexKey builds the index key for a composite block in a collection.
func ReconcileIndexKey(collectionShortID uint32, height uint64, cidBytes []byte) []byte {
	key := make([]byte, reconcileIndexPrefixLen+len(cidBytes))
	binary.BigEndian.PutUint32(key[:ReconcileIndexShortIDLen], collectionShortID)
	binary.BigEndian.PutUint64(key[ReconcileIndexShortIDLen:reconcileIndexPrefixLen], height)
	copy(key[reconcileIndexPrefixLen:], cidBytes)
	return key
}

// ReconcileIndexScopePrefix returns the key prefix selecting every entry for the
// given collection, for a prefix scan in reconciliation sort order.
func ReconcileIndexScopePrefix(collectionShortID uint32) []byte {
	prefix := make([]byte, ReconcileIndexShortIDLen)
	binary.BigEndian.PutUint32(prefix, collectionShortID)
	return prefix
}

// ReconcileIndexEntry decodes a stored reconcile-index key into its block height and
// raw CID bytes. ok is false if the key is too short to be valid.
func ReconcileIndexEntry(key []byte) (height uint64, cidBytes []byte, ok bool) {
	if len(key) <= reconcileIndexPrefixLen {
		return 0, nil, false
	}
	height = binary.BigEndian.Uint64(key[ReconcileIndexShortIDLen:reconcileIndexPrefixLen])
	return height, key[reconcileIndexPrefixLen:], true
}
