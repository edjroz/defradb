// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package p2p

import (
	"sort"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/internal/db/p2p/negentropy"
	"github.com/sourcenetwork/defradb/internal/db/p2p/protocol"
)

// testCID returns a deterministic CIDv1 for an integer seed.
func testCID(seed int) cid.Cid {
	h, err := multihash.Sum([]byte{byte(seed), byte(seed >> 8)}, multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	return cid.NewCidV1(cid.Raw, h)
}

func cidsOf(seeds ...int) []cid.Cid {
	out := make([]cid.Cid, len(seeds))
	for i, s := range seeds {
		out[i] = testCID(s)
	}
	return out
}

func byteSet(cids []cid.Cid) map[string]struct{} {
	m := make(map[string]struct{}, len(cids))
	for _, c := range cids {
		m[string(c.Bytes())] = struct{}{}
	}
	return m
}

func rawSet(ids [][]byte) map[string]struct{} {
	m := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		m[string(id)] = struct{}{}
	}
	return m
}

func TestVectorFromCIDs_Height0Order(t *testing.T) {
	cids := cidsOf(3, 1, 2)
	v, err := vectorFromCIDs(cids)
	require.NoError(t, err)
	require.Equal(t, len(cids), v.Size())

	// Items must be ordered by plain CID bytes (height 0), regardless of input order.
	want := make([][]byte, len(cids))
	for i, c := range cids {
		want[i] = c.Bytes()
	}
	sort.Slice(want, func(i, j int) bool { return string(want[i]) < string(want[j]) })

	for i := 0; i < v.Size(); i++ {
		require.Equal(t, want[i], v.ItemID(i))
	}
}

func TestRespondReconcile_MatchesEngine(t *testing.T) {
	local, err := vectorFromCIDs(cidsOf(1, 2, 3, 4))
	require.NoError(t, err)

	// An initiator's opening message from a different set.
	remote, err := vectorFromCIDs(cidsOf(3, 4, 5, 6))
	require.NoError(t, err)
	scope := protocol.ReconcileScope{Kind: protocol.ScopeDocHeads, ID: "doc"}
	req := protocol.ToWire(scope, negentropy.NewInitiator(remote).Initiate())

	got, err := respondReconcile(local, req)
	require.NoError(t, err)

	// The handler must be a thin stateless delegate to negentropy.Respond.
	incoming, err := protocol.FromWire(req)
	require.NoError(t, err)
	want, err := negentropy.Respond(local, incoming)
	require.NoError(t, err)
	require.Equal(t, protocol.ToWire(scope, want), got)
}

// driveWireSession runs a full reconciliation between two head sets through the
// real wire conversions and stateless responder, mirroring ReconcileDocument's
// loop without the network or datastore. It returns the initiator's need/have.
func driveWireSession(t *testing.T, a, b []cid.Cid) (need, have [][]byte) {
	t.Helper()
	localA, err := vectorFromCIDs(a)
	require.NoError(t, err)
	localB, err := vectorFromCIDs(b)
	require.NoError(t, err)

	scope := protocol.ReconcileScope{Kind: protocol.ScopeDocHeads, ID: "doc"}
	it := negentropy.NewInitiator(localA)
	msg := it.Initiate()

	for round := 0; ; round++ {
		require.Less(t, round, negentropy.MaxRounds+5, "session must converge")

		reply, err := respondReconcile(localB, protocol.ToWire(scope, msg))
		require.NoError(t, err)

		incoming, err := protocol.FromWire(reply)
		require.NoError(t, err)

		next, done := it.Reconcile(incoming)
		require.NoError(t, it.Err())
		if done {
			break
		}
		msg = next
	}
	return it.Need(), it.Have()
}

func TestReconcileSession_ConvergesOverWire(t *testing.T) {
	// A holds {1..4}, B holds {3..6}: A needs {5,6}, A has {1,2}.
	need, have := driveWireSession(t, cidsOf(1, 2, 3, 4), cidsOf(3, 4, 5, 6))

	require.Equal(t, byteSet(cidsOf(5, 6)), rawSet(need), "need = B \\ A")
	require.Equal(t, byteSet(cidsOf(1, 2)), rawSet(have), "have = A \\ B")
}

func TestReconcileSession_Identical_NoDiff(t *testing.T) {
	need, have := driveWireSession(t, cidsOf(1, 2, 3), cidsOf(1, 2, 3))
	require.Empty(t, need)
	require.Empty(t, have)
}

func TestReconcileSession_EmptyVsFull(t *testing.T) {
	need, have := driveWireSession(t, nil, cidsOf(1, 2, 3))
	require.Equal(t, byteSet(cidsOf(1, 2, 3)), rawSet(need))
	require.Empty(t, have)
}
