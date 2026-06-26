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
	"encoding/json"
	"strings"
	"testing"

	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/sourcenetwork/defradb/tests/state"
)

// dataContains reports whether the JSON encoding of a GQL result contains substr —
// a simple, type-agnostic convergence predicate for WaitForState.
func dataContains(substr string) func(any) bool {
	return func(data any) bool {
		b, err := json.Marshal(data)
		return err == nil && strings.Contains(string(b), substr)
	}
}

// Divergence created BEFORE the peers connect cannot propagate by pubsub. When they
// connect on a shared collection, the on-connect auto-trigger runs ReconcileCollection
// on its own — no manual call — and the catch-up node converges. WaitForState polls
// for that eventual convergence.
func TestReconcileCollection_AutoTriggerOnConnect(t *testing.T) {
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
			// Diverge before connecting — node 1 has none of this and pubsub cannot
			// carry it (no connection yet).
			&action.UpdateDoc{
				NodeID: immutable.Some(0),
				DocID:  0,
				Doc:    `{ "Age": 30 }`,
			},
			// Both share the collection (subscribe to its pubsub topic).
			testUtils.AddCollectionSubscription{NodeID: 0, CollectionIDs: []int{0}},
			testUtils.AddCollectionSubscription{NodeID: 1, CollectionIDs: []int{0}},
			testUtils.ConnectPeers{SourceNodeID: 0, TargetNodeID: 1},
			// No manual ReconcileCollection — the auto-trigger converges node 1.
			&action.WaitForState{
				NodeID:  1,
				Request: `query { Users { Name Age } }`,
				Check:   dataContains(`"Age":30`),
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}
