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

const usersSDL = `
	type Users {
		Name: String
		Age: Int
	}
`

// A document that exists only on the source node is pulled to the initiator by
// reconciliation, with no broadcast/subscription involved.
func TestReconcile_PullsMissingDocument(t *testing.T) {
	test := testUtils.TestCase{
		EnableSetReconciliation: true,
		// Experimental, Go-only surface for now (HTTP/CLI/JS wrappers exist but are
		// not exercised for this protocol yet).
		SupportedClientTypes: immutable.Some([]state.ClientType{state.GoClientType}),
		Actions: []any{
			testUtils.RandomNetworkingConfig(),
			testUtils.RandomNetworkingConfig(),
			&action.AddCollection{SDL: usersSDL},
			&action.AddDoc{
				NodeID: immutable.Some(0),
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.ConnectPeers{
				SourceNodeID: 0,
				TargetNodeID: 1,
			},
			// Node 1 reconciles the doc's heads against node 0 and pulls it.
			testUtils.ReconcileDocument{
				NodeID:       1,
				PeerNodeID:   0,
				CollectionID: 0,
				DocID:        0,
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
						{
							"Name": "John",
							"Age":  int64(21),
						},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}

// Two nodes that diverged the same document independently (no subscription, so the
// updates never propagated) converge to identical state after one reconciliation.
func TestReconcile_DivergentHeads_Converge(t *testing.T) {
	test := testUtils.TestCase{
		EnableSetReconciliation: true,
		SupportedClientTypes:    immutable.Some([]state.ClientType{state.GoClientType}),
		Actions: []any{
			testUtils.RandomNetworkingConfig(),
			testUtils.RandomNetworkingConfig(),
			&action.AddCollection{SDL: usersSDL},
			&action.AddDoc{
				// No NodeID: create John on both nodes at the same initial head.
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.ConnectPeers{
				SourceNodeID: 0,
				TargetNodeID: 1,
			},
			// Divergent updates. Without a subscription these stay local, so the two
			// nodes end up with different single heads.
			&action.UpdateDoc{
				NodeID: immutable.Some(0),
				Doc:    `{ "Age": 60 }`,
			},
			&action.UpdateDoc{
				NodeID: immutable.Some(1),
				Doc:    `{ "Age": 45 }`,
			},
			// One reconciliation converges both nodes to the same two-head DAG.
			testUtils.ReconcileDocument{
				NodeID:       0,
				PeerNodeID:   1,
				CollectionID: 0,
				DocID:        0,
			},
			// Both nodes (default: query every node) must agree on the LWW result.
			&action.Request{
				Request: `query {
					Users {
						Age
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"Age": testUtils.AnyOf(int64(45), int64(60)),
						},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}

// With the flag off the reconcile protocol is never registered, so ReconcileDocument
// fails fast rather than hanging — the precondition for old-peer fallback.
func TestReconcile_Disabled_ReturnsError(t *testing.T) {
	test := testUtils.TestCase{
		// EnableSetReconciliation deliberately left false.
		SupportedClientTypes: immutable.Some([]state.ClientType{state.GoClientType}),
		Actions: []any{
			testUtils.RandomNetworkingConfig(),
			testUtils.RandomNetworkingConfig(),
			&action.AddCollection{SDL: usersSDL},
			&action.AddDoc{
				NodeID: immutable.Some(0),
				Doc: `{
					"Name": "John",
					"Age": 21
				}`,
			},
			testUtils.ConnectPeers{
				SourceNodeID: 0,
				TargetNodeID: 1,
			},
			testUtils.ReconcileDocument{
				NodeID:        1,
				PeerNodeID:    0,
				CollectionID:  0,
				DocID:         0,
				ExpectedError: "set reconciliation is not enabled",
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}
