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
	"testing"

	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/sourcenetwork/defradb/tests/state"
)

// A node with none of a collection's data reconciles the whole collection from a
// peer in one session — the O(diff) cold-start path. Multiple docs with history
// exercise the full composite block set, not just heads.
func TestReconcileCollection_ColdStart(t *testing.T) {
	test := testUtils.TestCase{
		EnableSetReconciliation: true,
		SupportedClientTypes:    immutable.Some([]state.ClientType{state.GoClientType}),
		Actions: []any{
			testUtils.RandomNetworkingConfig(),
			testUtils.RandomNetworkingConfig(),
			&action.AddCollection{SDL: usersSDL},
			&action.AddDoc{
				NodeID: immutable.Some(0),
				Doc:    `{ "Name": "John", "Age": 21 }`,
			},
			&action.AddDoc{
				NodeID: immutable.Some(0),
				Doc:    `{ "Name": "Andy", "Age": 25 }`,
			},
			// Give John a multi-block history so the set is more than just heads.
			&action.UpdateDoc{
				NodeID: immutable.Some(0),
				DocID:  0,
				Doc:    `{ "Age": 22 }`,
			},
			testUtils.ConnectPeers{
				SourceNodeID: 0,
				TargetNodeID: 1,
			},
			// Node 1 has the schema but none of the data; reconcile the collection.
			testUtils.ReconcileCollection{
				NodeID:       1,
				PeerNodeID:   0,
				CollectionID: 0,
			},
			&action.Request{
				NodeID: immutable.Some(1),
				Request: `query {
					Users {
						Name
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"Name": "John", "Age": int64(22)},
						{"Name": "Andy", "Age": int64(25)},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}

// Both nodes share a base; node 0 diverges; node 1 catches up via collection
// reconciliation, fetching only the divergent blocks.
func TestReconcileCollection_PartialCatchUp(t *testing.T) {
	test := testUtils.TestCase{
		EnableSetReconciliation: true,
		SupportedClientTypes:    immutable.Some([]state.ClientType{state.GoClientType}),
		Actions: []any{
			testUtils.RandomNetworkingConfig(),
			testUtils.RandomNetworkingConfig(),
			&action.AddCollection{SDL: usersSDL},
			&action.AddDoc{
				// No NodeID: created on both nodes at the same initial head.
				Doc: `{ "Name": "John", "Age": 21 }`,
			},
			testUtils.ConnectPeers{
				SourceNodeID: 0,
				TargetNodeID: 1,
			},
			// Node 0 diverges locally (no subscription → not propagated).
			&action.UpdateDoc{
				NodeID: immutable.Some(0),
				DocID:  0,
				Doc:    `{ "Age": 60 }`,
			},
			testUtils.ReconcileCollection{
				NodeID:       1,
				PeerNodeID:   0,
				CollectionID: 0,
			},
			// Node 1 picks up node 0's update.
			&action.Request{
				NodeID: immutable.Some(1),
				Request: `query {
					Users {
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"Age": int64(60)},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}

// Identical nodes converge to a no-op (the session settles with an empty need-set).
func TestReconcileCollection_AlreadyConverged(t *testing.T) {
	test := testUtils.TestCase{
		EnableSetReconciliation: true,
		SupportedClientTypes:    immutable.Some([]state.ClientType{state.GoClientType}),
		Actions: []any{
			testUtils.RandomNetworkingConfig(),
			testUtils.RandomNetworkingConfig(),
			&action.AddCollection{SDL: usersSDL},
			&action.AddDoc{
				Doc: `{ "Name": "John", "Age": 21 }`,
			},
			testUtils.ConnectPeers{
				SourceNodeID: 0,
				TargetNodeID: 1,
			},
			testUtils.ReconcileCollection{
				NodeID:       1,
				PeerNodeID:   0,
				CollectionID: 0,
			},
			&action.Request{
				Request: `query {
					Users {
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{"Age": int64(21)},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}
