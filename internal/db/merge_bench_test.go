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
	"container/list"
	"context"
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/sourcenetwork/corekv/blockstore"
	"github.com/sourcenetwork/corekv/memory"
	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/internal/core"
	coreblock "github.com/sourcenetwork/defradb/internal/core/block"
	"github.com/sourcenetwork/defradb/internal/core/crdt"
	"github.com/sourcenetwork/defradb/internal/datastore"
	"github.com/sourcenetwork/defradb/internal/db/lock"
	"github.com/sourcenetwork/defradb/internal/keys"
)

// benchMergeDAGShapes are the synthetic DAG shapes walked by the merge benchmarks below.
//
// The synthetic DAG is a tree: every block links to `branch` distinct parents, and no two
// blocks are shared, so the number of blocks visited by a walk from the root is
// sum(branch^i) for i in [0, depth].  Branch factor 1 is a linear history; higher branch
// factors model a history that diverged and has not yet been re-converged.
var benchMergeDAGShapes = []struct {
	depth  int
	branch int
}{
	{depth: 1, branch: 1},
	{depth: 4, branch: 1},
	{depth: 16, branch: 1},
	{depth: 64, branch: 1},
	{depth: 2, branch: 2},
	{depth: 4, branch: 2},
	{depth: 8, branch: 2},
	{depth: 2, branch: 3},
	{depth: 4, branch: 3},
}

// newBenchMergeTxn returns a context carrying a transaction over a fresh in-memory rootstore.
func newBenchMergeTxn(b *testing.B) (context.Context, datastore.Txn) {
	b.Helper()

	ctx := context.Background()
	txn := datastore.NewTxnFrom(memory.NewDatastore(ctx), lock.NewLockSet(), 1, false, immutable.None[int]())

	return datastore.CtxSetTxn(ctx, txn), txn
}

// benchMergeBlockLinkSystem returns a link system that reads from, and writes to, the
// transaction's blockstore - the same storage `newMergeProcessor` reads from.
func benchMergeBlockLinkSystem(txn datastore.Txn) linking.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(blockstore.NewIPLDStore(txn.Blockstore()))
	lsys.SetWriteStorage(blockstore.NewIPLDStore(txn.Blockstore()))
	return lsys
}

// buildBenchMergeDAG writes a synthetic composite-block tree of the given shape and returns
// the root CID.
//
// Each block is given a distinct collection version ID so that sibling sub-trees do not
// collapse onto the same CID, which keeps the number of visited blocks predictable.
func buildBenchMergeDAG(
	b *testing.B,
	ctx context.Context,
	lsys *linking.LinkSystem,
	depth int,
	branch int,
) cid.Cid {
	b.Helper()

	nextID := 0

	var build func(level int) cid.Cid
	build = func(level int) cid.Cid {
		nextID++

		var heads []cidlink.Link
		if level > 0 {
			for i := 0; i < branch; i++ {
				heads = append(heads, cidlink.Link{Cid: build(level - 1)})
			}
		}

		block := &coreblock.Block{
			Delta: crdt.NewCRDT(&crdt.DocCompositeDelta{
				Priority:            uint64(level) + 1,
				CollectionVersionID: fmt.Sprintf("bench-block-%d", nextID),
				Status:              client.Active,
			}),
			Heads: heads,
		}

		link, err := lsys.Store(linking.LinkContext{Ctx: ctx}, coreblock.GetLinkPrototype(), block.GenerateNode())
		if err != nil {
			b.Fatal(err)
		}

		return link.(cidlink.Link).Cid
	}

	return build(depth)
}

// Benchmark_MergeProcessor_LoadComposites measures the merge-side DAG walk in isolation:
// loading and decoding every composite block reachable from the incoming block, down to the
// merge target.
//
// The merge target is empty here, so the walk always reaches the genesis block - this is the
// worst case, and the case taken when a peer receives a document it has never seen.
//
// Note that `loadComposites` does not memoise the blocks it has already visited, so a DAG in
// which sub-trees are shared is walked once per path to each shared block. The synthetic DAG
// used here deliberately avoids sharing so that the block count is exactly
// sum(branch^i) for i in [0, depth].
func Benchmark_MergeProcessor_LoadComposites(b *testing.B) {
	for _, shape := range benchMergeDAGShapes {
		b.Run(fmt.Sprintf("depth=%d/branch=%d", shape.depth, shape.branch), func(b *testing.B) {
			b.ReportAllocs()

			ctx, txn := newBenchMergeTxn(b)
			lsys := benchMergeBlockLinkSystem(txn)
			rootCid := buildBenchMergeDAG(b, ctx, &lsys, shape.depth, shape.branch)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// A merge processor is created per merge in the real code path, so the
				// (single) allocation of the composites list is included in the measurement.
				//
				// Only the fields `loadComposites` reads are populated - it does not touch
				// the collection or the DB.
				mp := &mergeProcessor{
					blockLS:    lsys,
					composites: list.New(),
				}

				if err := mp.loadComposites(ctx, rootCid, newMergeTarget()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_GetHeadsAsMergeTarget measures the head-set scan performed at the start of every
// merge: a prefix iteration over the headstore followed by a blockstore load and decode of
// each head block.
func Benchmark_GetHeadsAsMergeTarget(b *testing.B) {
	for _, headCount := range []int{1, 5, 20} {
		b.Run(fmt.Sprintf("heads=%d", headCount), func(b *testing.B) {
			b.ReportAllocs()

			ctx, txn := newBenchMergeTxn(b)
			lsys := benchMergeBlockLinkSystem(txn)

			headstoreKey := keys.HeadstoreDocKey{
				DocShortID: 1,
				FieldID:    core.COMPOSITE_NAMESPACE,
			}
			headset := coreblock.NewHeadSet(txn.Headstore(), headstoreKey)

			for i := 0; i < headCount; i++ {
				block := &coreblock.Block{
					Delta: crdt.NewCRDT(&crdt.DocCompositeDelta{
						Priority:            1,
						CollectionVersionID: fmt.Sprintf("bench-head-%d", i),
						Status:              client.Active,
					}),
				}

				link, err := lsys.Store(
					linking.LinkContext{Ctx: ctx},
					coreblock.GetLinkPrototype(),
					block.GenerateNode(),
				)
				if err != nil {
					b.Fatal(err)
				}

				if err := headset.Write(ctx, link.(cidlink.Link).Cid, 1); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if _, err := getHeadsAsMergeTarget(ctx, headstoreKey); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
