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

// Item is one element of a reconcilable set.
//
// SortKey (heightBE8 || cid) drives ordering and range bounds; ID (the raw CID
// bytes) is the input to the fingerprint and the payload carried in an IdList.
// Items are kept in strict ascending SortKey order with no duplicates.
type Item struct {
	// SortKey orders the item within the keyspace (see [SortKey]).
	SortKey []byte
	// ID is the raw CID bytes — the identity exchanged in IdList ranges and
	// summed into fingerprints.
	ID []byte
}

// NewItem builds an Item for the given block height and CID bytes, deriving the
// SortKey via [SortKey]. The CID slice is retained as the item ID; callers that
// reuse the buffer should pass a copy.
func NewItem(height uint64, cid []byte) Item {
	return Item{
		SortKey: SortKey(height, cid),
		ID:      cid,
	}
}

// CompareItems orders two items by SortKey, returning -1, 0, or +1.
func CompareItems(a, b Item) int {
	return CompareBound(a.SortKey, b.SortKey)
}
