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

// A single ReconcileCollection call converges BOTH peers: the initiator pulls the
// blocks it lacks AND pushes the blocks the peer lacks. Node 0 has only John, node 1
// has only Andy; after one reconcile both nodes hold both documents.
func TestReconcileCollection_Bidirectional_ConvergesBothNodes(t *testing.T) {
	bothDocs := map[string]any{
		"Users": []map[string]any{
			{"Name": "Andy"},
			{"Name": "John"},
		},
	}

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
				NodeID: immutable.Some(1),
				Doc:    `{ "Name": "Andy", "Age": 25 }`,
			},
			testUtils.ConnectPeers{SourceNodeID: 0, TargetNodeID: 1},
			// One call from node 0 against node 1.
			testUtils.ReconcileCollection{
				NodeID:       0,
				PeerNodeID:   1,
				CollectionID: 0,
			},
			// The initiator has both (pulled Andy).
			&action.Request{
				NodeID:            immutable.Some(0),
				Request:           `query { Users { Name } }`,
				Results:           bothDocs,
				NonOrderedResults: true,
			},
			// The peer also has both (was pushed John) — this is the bidirectional
			// property that pull-only does not satisfy.
			&action.Request{
				NodeID:            immutable.Some(1),
				Request:           `query { Users { Name } }`,
				Results:           bothDocs,
				NonOrderedResults: true,
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}
