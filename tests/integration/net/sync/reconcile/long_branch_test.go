// Copyright 2026 Democratized Data Foundation
//
// This file is part of the DefraDB test suite.
//
// The DefraDB test suite is licensed under either:
//
//   (1) GNU Affero General Public License v3
//   (2) Business Source License 1.1
//
// See tests/LICENSE for details.

package reconcile_test

import (
	"fmt"
	"testing"

	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/sourcenetwork/defradb/tests/state"
)

// deepBranchLen is the number of sequential updates that make a branch "long".
// The existing reconcile tests top out at depth 1-2; this is deliberately deep
// enough to exercise the multi-height sort-key tiling, the tip-vs-interior split
// in fetchAndMergeCollectionNeed, and the deep parent-first merge walk, while
// still running quickly in CI.
const deepBranchLen = 30

// updateActions builds n sequential UpdateDoc actions against one doc on one node,
// each setting Age to base+i so every call adds exactly one composite block. The
// action framework has no loop helper, so a long history is composed in Go and
// spread into the TestCase.Actions slice.
func updateActions(nodeID, collectionID, docID, base, n int) []any {
	acts := make([]any, 0, n)
	for i := 1; i <= n; i++ {
		acts = append(acts, &action.UpdateDoc{
			NodeID:       immutable.Some(nodeID),
			CollectionID: collectionID,
			DocID:        docID,
			Doc:          fmt.Sprintf(`{ "Age": %d }`, base+i),
		})
	}
	return acts
}

// linearHeights returns the expected ascending composite-block height list for a
// single linear chain of one create plus n updates: [1, 2, ..., n+1].
func linearHeights(n int) []map[string]any {
	heights := make([]map[string]any, 0, n+1)
	for h := 1; h <= n+1; h++ {
		heights = append(heights, map[string]any{"height": int64(h)})
	}
	return heights
}

// forkHeights returns the expected ascending composite-block height list for a
// shared base (height 1) plus two independent branches of n updates each: every
// height in 2..n+1 appears twice, height 1 once. Total 2n+1 commits.
func forkHeights(n int) []map[string]any {
	heights := []map[string]any{{"height": int64(1)}}
	for h := 2; h <= n+1; h++ {
		heights = append(heights, map[string]any{"height": int64(h)})
		heights = append(heights, map[string]any{"height": int64(h)})
	}
	return heights
}

// allCompositeHeights queries every composite block's height in ascending order.
// Run with no NodeID it executes on every node, so an identical expected list
// asserts the nodes hold the same composite-block DAG shape (and size).
const allCompositeHeights = `query {
	_commits(filter: {fieldName: {_eq: "_C"}}, order: {height: ASC}) {
		height
	}
}`

// headCompositeCid queries the single highest composite head's CID.
const headCompositeCid = `query {
	_commits(filter: {fieldName: {_eq: "_C"}}, order: {height: DESC}, limit: 1) {
		cid
	}
}`

// A long linear history on one node (deepBranchLen sequential updates the peer
// lacks) is pulled and merged in causal order by a single document-scope
// reconciliation. Asserts both convergence of state AND an identical composite
// DAG (height shape + identical head CID) across both nodes.
func TestReconcile_DeepLinearChain_DocScope_Converges(t *testing.T) {
	sameHead := testUtils.NewSameValue()

	actions := []any{
		testUtils.RandomNetworkingConfig(),
		testUtils.RandomNetworkingConfig(),
		&action.AddCollection{SDL: usersSDL},
		// Shared base on both nodes (same initial head).
		&action.AddDoc{Doc: `{ "Name": "John", "Age": 21 }`},
		testUtils.ConnectPeers{SourceNodeID: 0, TargetNodeID: 1},
	}
	// Node 0 builds a long linear chain; node 1 stays at the base head.
	actions = append(actions, updateActions(0, 0, 0, 1000, deepBranchLen)...)
	actions = append(actions,
		// Node 1 reconciles the doc's single deep head against node 0 and pulls
		// the whole chain (fetch + merge through the existing DAG-sync path).
		testUtils.ReconcileDocument{NodeID: 1, PeerNodeID: 0, CollectionID: 0, DocID: 0},
		// State converges on both nodes to the last write.
		&action.Request{
			Request: `query { Users { Age } }`,
			Results: map[string]any{
				"Users": []map[string]any{{"Age": int64(1000 + deepBranchLen)}},
			},
		},
		// Identical DAG shape across both nodes.
		&action.Request{
			Request: allCompositeHeights,
			Results: map[string]any{"_commits": linearHeights(deepBranchLen)},
		},
		// Identical head CID across both nodes.
		&action.Request{
			Request: headCompositeCid,
			Results: map[string]any{"_commits": []map[string]any{{"cid": sameHead}}},
		},
	)

	testUtils.ExecuteTestCase(t, testUtils.TestCase{
		EnableSetReconciliation: true,
		SupportedClientTypes:    immutable.Some([]state.ClientType{state.GoClientType}),
		Actions:                 actions,
	})
}

// A node with none of a collection's data cold-starts a document that has a long
// linear history via one collection-scope reconciliation. The need-set is the
// whole chain, so this exercises fetchAndMergeCollectionNeed over an
// all-interior-but-tip block set and the deep merge walk.
func TestReconcileCollection_DeepLinearChain_ColdStart_Converges(t *testing.T) {
	sameHead := testUtils.NewSameValue()

	actions := []any{
		testUtils.RandomNetworkingConfig(),
		testUtils.RandomNetworkingConfig(),
		&action.AddCollection{SDL: usersSDL},
		// Only node 0 has the document.
		&action.AddDoc{NodeID: immutable.Some(0), Doc: `{ "Name": "John", "Age": 21 }`},
	}
	actions = append(actions, updateActions(0, 0, 0, 1000, deepBranchLen)...)
	actions = append(actions,
		testUtils.ConnectPeers{SourceNodeID: 0, TargetNodeID: 1},
		// Node 1 has the schema but none of the data; reconcile the collection.
		testUtils.ReconcileCollection{NodeID: 1, PeerNodeID: 0, CollectionID: 0},
		&action.Request{
			Request: `query { Users { Age } }`,
			Results: map[string]any{
				"Users": []map[string]any{{"Age": int64(1000 + deepBranchLen)}},
			},
		},
		&action.Request{
			Request: allCompositeHeights,
			Results: map[string]any{"_commits": linearHeights(deepBranchLen)},
		},
		&action.Request{
			Request: headCompositeCid,
			Results: map[string]any{"_commits": []map[string]any{{"cid": sameHead}}},
		},
	)

	testUtils.ExecuteTestCase(t, testUtils.TestCase{
		EnableSetReconciliation: true,
		SupportedClientTypes:    immutable.Some([]state.ClientType{state.GoClientType}),
		Actions:                 actions,
	})
}

// Two nodes each extend an independent long branch off a shared base, then
// converge via one document-scope reconciliation. Exercises a deep two-sided
// fork: multi-head merge and last-writer-wins resolution across deep branches.
func TestReconcile_DeepConcurrentFork_DocScope_Converges(t *testing.T) {
	actions := []any{
		testUtils.RandomNetworkingConfig(),
		testUtils.RandomNetworkingConfig(),
		&action.AddCollection{SDL: usersSDL},
		&action.AddDoc{Doc: `{ "Name": "John", "Age": 21 }`},
		testUtils.ConnectPeers{SourceNodeID: 0, TargetNodeID: 1},
	}
	// Each node extends its own deep branch from the shared base (no subscription,
	// so the updates never propagate until reconciliation).
	actions = append(actions, updateActions(0, 0, 0, 1000, deepBranchLen)...)
	actions = append(actions, updateActions(1, 0, 0, 2000, deepBranchLen)...)
	actions = append(actions,
		// A single document-scope reconciliation converges both nodes
		// (initiator pulls the peer's branch and pushes its own).
		testUtils.ReconcileDocument{NodeID: 0, PeerNodeID: 1, CollectionID: 0, DocID: 0},
		// LWW: every node agrees on one of the two branch tips.
		&action.Request{
			Request: `query { Users { Age } }`,
			Results: map[string]any{
				"Users": []map[string]any{
					{"Age": testUtils.AnyOf(int64(1000+deepBranchLen), int64(2000+deepBranchLen))},
				},
			},
		},
		// Identical two-branch DAG shape (2*deepBranchLen+1 composites) on both nodes.
		&action.Request{
			Request: allCompositeHeights,
			Results: map[string]any{"_commits": forkHeights(deepBranchLen)},
		},
	)

	testUtils.ExecuteTestCase(t, testUtils.TestCase{
		EnableSetReconciliation: true,
		SupportedClientTypes:    immutable.Some([]state.ClientType{state.GoClientType}),
		Actions:                 actions,
	})
}

// Same deep two-sided fork as above, converged via collection-scope
// reconciliation (the M2 path) rather than document heads.
func TestReconcileCollection_DeepConcurrentFork_Converges(t *testing.T) {
	actions := []any{
		testUtils.RandomNetworkingConfig(),
		testUtils.RandomNetworkingConfig(),
		&action.AddCollection{SDL: usersSDL},
		&action.AddDoc{Doc: `{ "Name": "John", "Age": 21 }`},
		testUtils.ConnectPeers{SourceNodeID: 0, TargetNodeID: 1},
	}
	actions = append(actions, updateActions(0, 0, 0, 1000, deepBranchLen)...)
	actions = append(actions, updateActions(1, 0, 0, 2000, deepBranchLen)...)
	actions = append(actions,
		testUtils.ReconcileCollection{NodeID: 0, PeerNodeID: 1, CollectionID: 0},
		&action.Request{
			Request: `query { Users { Age } }`,
			Results: map[string]any{
				"Users": []map[string]any{
					{"Age": testUtils.AnyOf(int64(1000+deepBranchLen), int64(2000+deepBranchLen))},
				},
			},
		},
		&action.Request{
			Request: allCompositeHeights,
			Results: map[string]any{"_commits": forkHeights(deepBranchLen)},
		},
	)

	testUtils.ExecuteTestCase(t, testUtils.TestCase{
		EnableSetReconciliation: true,
		SupportedClientTypes:    immutable.Some([]state.ClientType{state.GoClientType}),
		Actions:                 actions,
	})
}
