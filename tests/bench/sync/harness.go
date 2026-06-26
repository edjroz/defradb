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

package sync

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/node"
)

const (
	// syncTimeout bounds a single sync attempt. It must cover establishing a large
	// shared base (the untimed setup), which over loopback costs ~20ms/block, so a
	// 500–1000 doc base needs well over the original 30s. It bounds setup and the
	// timed op alike; a generous value only delays a genuine hang, so it is set high
	// to let the large-collection sweeps (docs500/docs1000) converge their base.
	// The default-sync 1000-doc base is the slowest (~370s of per-doc broadcast pull).
	syncTimeout = 15 * time.Minute
	// meshSettle gives the gossipsub topic mesh time to form after a transport
	// connection is established, before document sync is attempted. This happens
	// in untimed setup, so a generous value only affects setup wall-clock.
	meshSettle = 1 * time.Second
	// benchEnableEnv opts in to the network sync benchmarks. They start real
	// libp2p nodes and are heavier and more timing-sensitive than the other
	// micro-benchmarks, so they are skipped unless explicitly enabled to keep the
	// default `go test -bench=.` lane fast and stable.
	benchEnableEnv = "DEFRA_BENCH_SYNC"
)

// requireSyncBenchEnabled skips the benchmark unless DEFRA_BENCH_SYNC is set.
func requireSyncBenchEnabled(b *testing.B) {
	if os.Getenv(benchEnableEnv) == "" {
		b.Skipf("set %s=1 to run the network sync benchmarks (they start real libp2p nodes)", benchEnableEnv)
	}
}

// syncNode is a started, P2P-enabled DefraDB node for the sync benchmarks.
type syncNode struct {
	node *node.Node
	db   node.DB
	p2p  client.P2P
	// counters is non-nil only when the node was started instrumented; it holds
	// the per-protocol wire-traffic counts recorded by the CountingHost.
	counters *Counters
}

// nodeOpt mutates the node options builder before the node is started, letting a
// benchmark opt into extra features (e.g. set reconciliation) without changing the
// default-sync node shape.
type nodeOpt func(*options.NodeOptionsBuilder)

// withReconciliation enables the experimental set-reconciliation protocol on the
// node. The default-sync benchmarks do not pass it, so their node shape is
// unchanged.
func withReconciliation(b *options.NodeOptionsBuilder) {
	b.DB().SetEnableSetReconciliation(true)
}

// startSyncNode starts an in-memory, P2P-enabled node. When instrument is true
// the host is wrapped in a CountingHost via the HostDecorator seam.
func startSyncNode(ctx context.Context, tb testing.TB, instrument bool, extra ...nodeOpt) *syncNode {
	tb.Helper()

	var counters *Counters
	b := options.Node().
		SetDisableAPI(true).
		SetEnableDevelopment(true)
	for _, opt := range extra {
		opt(b)
	}
	// Give the node an identity. DocumentACP is enabled by default, so block
	// serving authenticates the requesting peer; without an identity the peer
	// returns an unparseable token that is never cached, forcing a fresh identity
	// round-trip per block that starves block transfer under load.
	ident, err := identity.Generate(crypto.KeyTypeEd25519)
	require.NoError(tb, err)
	b.DB().SetNodeIdentity(ident)
	// Use the in-memory corekv store (not badger-in-memory): it is not value-size
	// limited, so blocks are stored unchunked and the blockstore CID diff used for
	// metrics can enumerate them directly.
	b.Store().SetType(options.NodeMemoryStore)
	p2pb := b.P2P().
		SetListenAddresses("/ip4/127.0.0.1/tcp/0").
		SetEnablePubSub(true)
	if instrument {
		counters = NewCounters()
		p2pb.SetHostDecorator(Decorator(counters))
	}

	n, err := node.New(ctx, b)
	require.NoError(tb, err)
	require.NoError(tb, n.Start(ctx))

	p2p, ok := n.DB.(client.P2P)
	require.True(tb, ok, "node DB does not implement client.P2P")

	return &syncNode{node: n, db: n.DB, p2p: p2p, counters: counters}
}

func (sn *syncNode) close(ctx context.Context) {
	_ = sn.node.Close(ctx)
}

// peerID returns the node's libp2p peer ID, parsed from one of its addresses.
func (sn *syncNode) peerID(ctx context.Context, tb testing.TB) string {
	tb.Helper()
	addrs, err := sn.p2p.PeerInfo(ctx)
	require.NoError(tb, err)
	require.NotEmpty(tb, addrs)
	info, err := peer.AddrInfoFromString(addrs[0])
	require.NoError(tb, err)
	return info.ID.String()
}

// reconcileDocs runs set reconciliation for each document against peerID and
// returns the wall-clock for the whole batch. Reconciliation is synchronous, so
// the elapsed time already covers fetch + merge on both sides.
func (sn *syncNode) reconcileDocs(ctx context.Context, tb testing.TB, peerID string, docIDs []string) time.Duration {
	tb.Helper()
	recCtx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	start := time.Now()
	for _, docID := range docIDs {
		require.NoError(tb, sn.p2p.ReconcileDocument(recCtx, peerID, collectionName, docID))
	}
	return time.Since(start)
}

// reconcileCollection runs collection-scope set reconciliation against peerID,
// converging this node over the whole collection in one session, and returns the
// wall-clock (it is synchronous: fetch + merge complete before it returns).
func (sn *syncNode) reconcileCollection(ctx context.Context, tb testing.TB, peerID string) time.Duration {
	tb.Helper()
	recCtx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	start := time.Now()
	require.NoError(tb, sn.p2p.ReconcileCollection(recCtx, peerID, collectionName))
	return time.Since(start)
}

// addCollection registers the benchmark schema and returns its collection.
func (sn *syncNode) addCollection(ctx context.Context, tb testing.TB) client.Collection {
	tb.Helper()
	_, err := sn.db.AddCollection(ctx, schemaSDL)
	require.NoError(tb, err)
	col, err := sn.db.GetCollectionByName(ctx, collectionName)
	require.NoError(tb, err)
	return col
}

// connectNodes connects from -> to at the transport level and waits for the
// topic mesh to settle so subsequent pubsub-based document sync can find a peer.
func connectNodes(ctx context.Context, tb testing.TB, from, to *syncNode) {
	tb.Helper()
	addrs, err := to.p2p.PeerInfo(ctx)
	require.NoError(tb, err)
	require.NoError(tb, from.p2p.Connect(ctx, addrs))
	time.Sleep(meshSettle)
}

// measuredSync pulls the latest versions of the given documents from connected
// peers and returns the wall-clock from issuing the sync to the last of
// expectedMerges merge-complete events. Each synced document head produces one
// merge-complete event, so expectedMerges is the number of documents whose head
// is expected to change. The sync request runs in the background (it does not
// return promptly on completion — see quietPeriod) and is cancelled only once
// all expected merges have landed, so no in-flight merge is aborted early.
func (sn *syncNode) measuredSync(
	ctx context.Context,
	tb testing.TB,
	docIDs []string,
	expectedMerges int,
) time.Duration {
	tb.Helper()
	sub, err := sn.db.Events().Subscribe(event.MergeCompleteName)
	require.NoError(tb, err)
	defer sn.db.Events().Unsubscribe(sub)

	syncCtx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	start := time.Now()
	go func() { _ = sn.p2p.SyncDocuments(syncCtx, collectionName, docIDs) }()

	var last time.Time
	for i := 0; i < expectedMerges; i++ {
		select {
		case <-sub.Message():
			last = time.Now()
		case <-time.After(syncTimeout):
			tb.Fatalf("received %d/%d merge-complete events before timeout", i, expectedMerges)
		}
	}
	return last.Sub(start)
}
