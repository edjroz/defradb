// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/sourcenetwork/corekv/memory"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/client"
	dbid "github.com/sourcenetwork/defradb/internal/db/id"
)

func testIndexCID(seed int) cid.Cid {
	h, err := multihash.Sum([]byte{byte(seed), byte(seed >> 8)}, multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

// TestReconcileIndex_RoundTripSortedByHeightThenCID inserts items out of order for two
// collections and asserts each scope enumerates only its own items, in (height, CID)
// order.
func TestReconcileIndex_RoundTripSortedByHeightThenCID(t *testing.T) {
	ctx := context.Background()
	store := memory.NewDatastore(ctx)

	const colA, colB = uint32(7), uint32(9)
	a1, a2, a3 := testIndexCID(1), testIndexCID(2), testIndexCID(3)

	// Insert out of order; include a different collection that must not leak in.
	require.NoError(t, insertReconcileIndex(ctx, store, colA, 5, a3))
	require.NoError(t, insertReconcileIndex(ctx, store, colA, 2, a1))
	require.NoError(t, insertReconcileIndex(ctx, store, colA, 2, a2))
	require.NoError(t, insertReconcileIndex(ctx, store, colB, 1, testIndexCID(99)))

	items, err := reconcileIndexItems(ctx, store, colA)
	require.NoError(t, err)
	require.Len(t, items, 3)

	// Ordered by height first; within equal height, by CID bytes ascending.
	require.Equal(t, uint64(2), items[0].Height)
	require.Equal(t, uint64(2), items[1].Height)
	require.Equal(t, uint64(5), items[2].Height)
	require.Equal(t, a3, items[2].Cid)
	require.Less(t, string(items[0].Cid.Bytes()), string(items[1].Cid.Bytes()))

	// The other scope is isolated.
	other, err := reconcileIndexItems(ctx, store, colB)
	require.NoError(t, err)
	require.Len(t, other, 1)
	require.Equal(t, uint64(1), other[0].Height)
}

func TestReconcileIndex_EmptyScope(t *testing.T) {
	ctx := context.Background()
	items, err := reconcileIndexItems(ctx, memory.NewDatastore(ctx), 42)
	require.NoError(t, err)
	require.Empty(t, items)
}

// TestReconcileIndex_Idempotent confirms re-inserting the same block is a no-op (the
// key is deterministic) — the property the backfill relies on for crash recovery.
func TestReconcileIndex_Idempotent(t *testing.T) {
	ctx := context.Background()
	store := memory.NewDatastore(ctx)
	c := testIndexCID(1)

	require.NoError(t, insertReconcileIndex(ctx, store, 1, 3, c))
	require.NoError(t, insertReconcileIndex(ctx, store, 1, 3, c))

	items, err := reconcileIndexItems(ctx, store, 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
}

// readReconcileIndex reads a collection's index items in a fresh read-only txn.
func readReconcileIndex(ctx context.Context, t *testing.T, db *DB, shortID uint32) []reconcileIndexItem {
	ctx, txn, err := ensureContextTxn(ctx, db, true)
	require.NoError(t, err)
	defer txn.Discard()
	items, err := reconcileIndexItems(ctx, txn.ReconcileIndex(), shortID)
	require.NoError(t, err)
	return items
}

// runEnsureReconcileIndex drives ensureReconcileIndex in its own committed txn.
func runEnsureReconcileIndex(ctx context.Context, t *testing.T, db *DB) {
	ctx, txn, err := ensureContextTxn(ctx, db, false)
	require.NoError(t, err)
	defer txn.Discard()
	require.NoError(t, db.ensureReconcileIndex(ctx))
	require.NoError(t, txn.Commit())
}

func collectionShortIDForTest(ctx context.Context, t *testing.T, db *DB, collectionID string) uint32 {
	ctx, txn, err := ensureContextTxn(ctx, db, true)
	require.NoError(t, err)
	defer txn.Discard()
	shortID, err := dbid.GetUncachedShortCollectionID(ctx, collectionID, txn.Systemstore())
	require.NoError(t, err)
	return shortID
}

// TestReconcileIndex_Backfill writes composite blocks with reconciliation OFF (so the
// maintenance hooks no-op and the index stays empty), then enables it and runs the
// backfill — which must populate the index with every composite block by walking the
// document DAGs.
func TestReconcileIndex_Backfill(t *testing.T) {
	ctx := context.Background()
	db, err := newBadgerDB(ctx) // reconciliation defaults OFF
	require.NoError(t, err)

	_, err = db.AddCollection(ctx, `type User { name: String value: Int }`)
	require.NoError(t, err)
	col, err := db.GetCollectionByName(ctx, "User")
	require.NoError(t, err)

	// 2 docs, 1 update each → 4 composite blocks (create + update per doc).
	for i := 0; i < 2; i++ {
		doc, err := client.NewDocFromJSON(ctx, []byte(fmt.Sprintf(`{"name":%q,"value":0}`, fmt.Sprintf("u%d", i))), col.Version())
		require.NoError(t, err)
		require.NoError(t, col.AddDocument(ctx, doc))
		require.NoError(t, doc.SetWithJSON(ctx, []byte(`{"value":1}`)))
		require.NoError(t, col.SaveDocument(ctx, doc))
	}

	shortID := collectionShortIDForTest(ctx, t, db, col.Version().CollectionID)

	// Flag off ⇒ hooks were no-ops ⇒ index empty.
	require.Empty(t, readReconcileIndex(ctx, t, db, shortID))

	// Enable + backfill ⇒ every composite block indexed.
	db.setReconciliationEnabled = true
	runEnsureReconcileIndex(ctx, t, db)
	require.Len(t, readReconcileIndex(ctx, t, db, shortID), 4)

	// Idempotent re-run (sentinel now set; also deterministic keys).
	runEnsureReconcileIndex(ctx, t, db)
	require.Len(t, readReconcileIndex(ctx, t, db, shortID), 4)
}

// TestReconcileIndex_Backfill_ToggleClearsSentinel verifies disabling reconciliation
// clears the "built" sentinel so a later enable rebuilds (capturing blocks written
// while it was off).
func TestReconcileIndex_Backfill_ToggleClearsSentinel(t *testing.T) {
	ctx := context.Background()
	db, err := newBadgerDB(ctx)
	require.NoError(t, err)

	_, err = db.AddCollection(ctx, `type User { name: String value: Int }`)
	require.NoError(t, err)
	col, err := db.GetCollectionByName(ctx, "User")
	require.NoError(t, err)
	shortID := collectionShortIDForTest(ctx, t, db, col.Version().CollectionID)

	// Enable, build (no docs yet) — sentinel set.
	db.setReconciliationEnabled = true
	runEnsureReconcileIndex(ctx, t, db)

	// Write a doc while DISABLED (hooks off ⇒ index misses it).
	db.setReconciliationEnabled = false
	doc, err := client.NewDocFromJSON(ctx, []byte(`{"name":"a","value":0}`), col.Version())
	require.NoError(t, err)
	require.NoError(t, col.AddDocument(ctx, doc))

	// A disabled init pass clears the sentinel...
	runEnsureReconcileIndex(ctx, t, db)
	// ...so re-enabling rebuilds and captures the doc written while off.
	db.setReconciliationEnabled = true
	runEnsureReconcileIndex(ctx, t, db)
	require.Len(t, readReconcileIndex(ctx, t, db, shortID), 1)
}
