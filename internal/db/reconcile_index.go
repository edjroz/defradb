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
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/corekv/blockstore"

	"github.com/sourcenetwork/defradb/errors"
	"github.com/sourcenetwork/defradb/internal/core"
	coreblock "github.com/sourcenetwork/defradb/internal/core/block"
	"github.com/sourcenetwork/defradb/internal/datastore"
	"github.com/sourcenetwork/defradb/internal/db/description"
	"github.com/sourcenetwork/defradb/internal/db/id"
	"github.com/sourcenetwork/defradb/internal/keys"
)

// reconcileIndexBuiltKey is the systemstore sentinel marking the reconcile index as
// backfilled (mirrors the "/init" marker). It is set after a successful backfill and
// cleared while reconciliation is disabled, so re-enabling rebuilds the index and
// captures any composite blocks written while it was off.
var reconcileIndexBuiltKey = []byte("/reconcile_index_built")

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

// ensureReconcileIndex keeps the reconcile-index backfill state consistent at startup,
// within the init transaction. When reconciliation is disabled it clears the "built"
// sentinel (so a later enable rebuilds); when enabled and not yet built it runs the
// backfill and sets the sentinel. It is a fast no-op on every subsequent start.
func (db *DB) ensureReconcileIndex(ctx context.Context) error {
	system := datastore.CtxMustGetTxn(ctx).Systemstore()

	if !db.setReconciliationEnabled {
		has, err := system.Has(ctx, reconcileIndexBuiltKey)
		if err != nil {
			return err
		}
		if has {
			return system.Delete(ctx, reconcileIndexBuiltKey)
		}
		return nil
	}

	built, err := system.Has(ctx, reconcileIndexBuiltKey)
	if err != nil {
		return err
	}
	if built {
		return nil
	}

	if err := db.backfillReconcileIndex(ctx); err != nil {
		return err
	}
	return system.Set(ctx, reconcileIndexBuiltKey, []byte{1})
}

// backfillReconcileIndex (re)builds the reconcile index from existing data by walking
// every document's composite DAG from its heads and inserting each composite block,
// in the current transaction. Idempotent — deterministic keys make a re-run (or
// crash-recovery) a no-op.
func (db *DB) backfillReconcileIndex(ctx context.Context) error {
	txn := datastore.CtxMustGetTxn(ctx)

	blockLS := cidlink.DefaultLinkSystem()
	blockLS.SetReadStorage(blockstore.NewIPLDStore(txn.Blockstore()))
	indexStore := txn.ReconcileIndex()

	visited := make(map[cid.Cid]struct{})
	shortIDByVersion := make(map[string]uint32)

	iter, err := txn.Headstore().Iterator(ctx, corekv.IterOptions{
		Prefix:   keys.HeadstoreDocKey{}.Bytes(),
		KeysOnly: true,
	})
	if err != nil {
		return err
	}

	for {
		hasNext, err := iter.Next()
		if err != nil {
			return errors.Join(err, iter.Close())
		}
		if !hasNext {
			break
		}
		headKey, err := keys.NewHeadstoreDocKey(string(iter.Key()))
		if err != nil {
			return errors.Join(err, iter.Close())
		}
		// Reconcile only the composite spine; field heads are skipped.
		if headKey.FieldID != core.COMPOSITE_NAMESPACE {
			continue
		}
		if err := db.indexCompositeDAG(ctx, &blockLS, indexStore, headKey.Cid, visited, shortIDByVersion); err != nil {
			return errors.Join(err, iter.Close())
		}
	}
	return iter.Close()
}

// indexCompositeDAG walks a composite DAG from head down to genesis with an explicit
// stack (deep DAGs preclude recursion), inserting each unvisited composite block. The
// visited set is shared across heads so shared ancestors are processed once.
func (db *DB) indexCompositeDAG(
	ctx context.Context,
	blockLS *linking.LinkSystem,
	indexStore corekv.Writer,
	head cid.Cid,
	visited map[cid.Cid]struct{},
	shortIDByVersion map[string]uint32,
) error {
	stack := []cid.Cid{head}
	for len(stack) > 0 {
		c := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := visited[c]; ok {
			continue
		}
		visited[c] = struct{}{}

		nd, err := blockLS.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, coreblock.BlockSchemaPrototype)
		if err != nil {
			return err
		}
		block, err := coreblock.GetFromNode(nd)
		if err != nil {
			return err
		}

		shortID, err := db.reconcileShortIDForVersion(ctx, block.Delta.GetCollectionVersionID(), shortIDByVersion)
		if err != nil {
			return err
		}
		if err := insertReconcileIndex(ctx, indexStore, shortID, block.Delta.GetPriority(), c); err != nil {
			return err
		}

		for _, h := range block.Heads {
			stack = append(stack, h.Cid)
		}
	}
	return nil
}

// reconcileShortIDForVersion resolves a block's collection-version ID to its
// collection short ID, caching by version ID (many blocks share a version).
func (db *DB) reconcileShortIDForVersion(
	ctx context.Context,
	versionID string,
	cache map[string]uint32,
) (uint32, error) {
	if shortID, ok := cache[versionID]; ok {
		return shortID, nil
	}
	col, err := description.GetCollectionByID(ctx, db.collectionRepository, versionID)
	if err != nil {
		return 0, err
	}
	system := datastore.CtxMustGetTxn(ctx).Systemstore()
	shortID, err := id.GetUncachedShortCollectionID(ctx, col.CollectionID, system)
	if err != nil {
		return 0, err
	}
	cache[versionID] = shortID
	return shortID, nil
}
