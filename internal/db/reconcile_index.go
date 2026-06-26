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

	"github.com/ipfs/go-cid"

	"github.com/sourcenetwork/corekv"

	"github.com/sourcenetwork/defradb/errors"
	coreblock "github.com/sourcenetwork/defradb/internal/core/block"
	"github.com/sourcenetwork/defradb/internal/datastore"
	"github.com/sourcenetwork/defradb/internal/db/id"
	"github.com/sourcenetwork/defradb/internal/keys"
)

// reconcileIndexItem is one composite block in a collection's ordered reconcile
// index: its CRDT height and CID.
type reconcileIndexItem struct {
	Height uint64
	Cid    cid.Cid
}

// insertReconcileIndex records a composite block CID in the collection's ordered
// reconcile index. The key encodes (collectionShortID, height, cid) so a prefix scan
// enumerates the collection's blocks in reconciliation sort order; the value is empty.
// Writing through the caller's store keeps the insert in the same transaction as the
// block persist.
func insertReconcileIndex(
	ctx context.Context,
	store corekv.Writer,
	collectionShortID uint32,
	height uint64,
	c cid.Cid,
) error {
	key := keys.ReconcileIndexKey(collectionShortID, height, c.Bytes())
	return store.Set(ctx, key, nil)
}

// reconcileIndexItems returns the composite block items recorded for a collection, in
// reconciliation sort order (height then CID).
func reconcileIndexItems(
	ctx context.Context,
	store corekv.Reader,
	collectionShortID uint32,
) ([]reconcileIndexItem, error) {
	iter, err := store.Iterator(ctx, corekv.IterOptions{
		Prefix:   keys.ReconcileIndexScopePrefix(collectionShortID),
		KeysOnly: true,
	})
	if err != nil {
		return nil, err
	}

	var items []reconcileIndexItem
	for {
		hasNext, err := iter.Next()
		if err != nil {
			return nil, errors.Join(err, iter.Close())
		}
		if !hasNext {
			break
		}
		height, cidBytes, ok := keys.ReconcileIndexEntry(iter.Key())
		if !ok {
			continue
		}
		c, err := cid.Cast(cidBytes)
		if err != nil {
			return nil, errors.Join(err, iter.Close())
		}
		items = append(items, reconcileIndexItem{Height: height, Cid: c})
	}
	return items, iter.Close()
}

// indexMergedComposites records every composite block merged by mp into the
// collection's reconcile index, in the current transaction. It is a no-op when set
// reconciliation is disabled. This is the remote-merge maintenance hook.
func (db *DB) indexMergedComposites(ctx context.Context, col *collection, mp *mergeProcessor) error {
	if !db.setReconciliationEnabled {
		return nil
	}
	shortID, err := id.GetShortCollectionID(ctx, col.Version().CollectionID)
	if err != nil {
		return err
	}
	store := datastore.CtxMustGetTxn(ctx).ReconcileIndex()
	for e := mp.composites.Front(); e != nil; e = e.Next() {
		block, ok := e.Value.(*coreblock.Block)
		if !ok {
			continue
		}
		link, err := block.GenerateLink()
		if err != nil {
			return err
		}
		if err := insertReconcileIndex(ctx, store, shortID, block.Delta.GetPriority(), link.Cid); err != nil {
			return err
		}
	}
	return nil
}

// indexLocalComposite records a locally-created composite block (its marshalled bytes
// carry the height) into the collection's reconcile index, in the current
// transaction. No-op when set reconciliation is disabled. This is the local-commit
// maintenance hook.
func (db *DB) indexLocalComposite(ctx context.Context, collectionID string, c cid.Cid, blockBytes []byte) error {
	if !db.setReconciliationEnabled {
		return nil
	}
	block, err := coreblock.GetFromBytes(blockBytes)
	if err != nil {
		return err
	}
	shortID, err := id.GetShortCollectionID(ctx, collectionID)
	if err != nil {
		return err
	}
	store := datastore.CtxMustGetTxn(ctx).ReconcileIndex()
	return insertReconcileIndex(ctx, store, shortID, block.Delta.GetPriority(), c)
}
