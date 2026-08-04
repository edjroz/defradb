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

// Package p2p contains benchmarks covering peer-to-peer document replication
// between two nodes.
//
// Two replication paths are covered, as they have very different performance
// characteristics:
//
//   - The push path ([Benchmark_P2P_Replicate_Push]), in which a replicator is
//     configured up-front and document creations on the source node are pushed to the
//     target node as they happen. This is the path that reflects live replication
//     latency.
//
//   - The pull path ([Benchmark_P2P_Sync_Pull]), in which the target node explicitly
//     requests a set of documents from its peers via `SyncDocuments`.
package p2p

import (
	"fmt"
	"testing"
	"time"

	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
	"github.com/sourcenetwork/defradb/tests/state"
)

// benchSDL is the schema used by all peer-to-peer sync benchmarks in this package.
const benchSDL = `
	type Users {
		Name: String
		Age: Int
	}
`

// pullDocCounts are the document-set sizes that [Benchmark_P2P_Sync_Pull] is run
// against.
//
// Larger sizes cannot currently be benchmarked. `SyncDocuments` applies a default five
// second deadline when the caller supplies no deadline of its own, and on expiry it
// returns the heads it happened to collect with a nil error rather than reporting that
// the sync was incomplete. At around five hundred documents that deadline expires before
// every document has been transferred, so the documents that were dropped never produce
// a merge event on the target node and [testUtils.WaitForSync] blocks until it times out.
var pullDocCounts = []int{1, 50}

// pushDocCounts are the document-set sizes that [Benchmark_P2P_Replicate_Push] is run
// against.
//
// The push path currently cannot be benchmarked at larger sizes. Creating roughly ten or
// more documents in the same collection in quick succession causes the replicator to
// drop a subset of the pushes, logging:
//
//	Failed to send update after sync ... "topic already exists / failed to push log"
//
// The dropped documents never reach the target node, so [testUtils.WaitForSync] blocks
// until it times out. These sizes should be raised once the replicator no longer races on
// the creation of the per-collection pubsub topic.
var pushDocCounts = []int{1, 5}

// syncSpans holds the wall-clock spans captured by a single benchmark iteration.
//
// The spans are captured by [action.RunFunc] actions interleaved into the test case
// action list, so each span is bounded by the completion of a specific framework action
// rather than by the total cost of running the test case.
type syncSpans struct {
	// replicated is the time from the start of the replication trigger to
	// [testUtils.WaitForSync] returning.
	//
	// [testUtils.WaitForSync] blocks until the target node has emitted a `MergeComplete`
	// event for every DAG head recorded as expected for that node, so this span is
	// bounded by "every expected document DAG has been merged into the target node's
	// local store".
	replicated time.Duration

	// visible is the time from the start of the replication trigger to a GraphQL query
	// against the target node returning the replicated documents.
	//
	// This is always greater than or equal to [syncSpans.replicated], and additionally
	// covers the cost of the read path observing the merged data.
	visible time.Duration

	// localWrite is the time taken to create the documents on the source node. For the
	// push path this overlaps with replication, for the pull path it precedes it.
	localWrite time.Duration

	// observedDocs is the number of documents the target node actually returned. It is
	// reported as a metric so that an incomplete replication is visible in the benchmark
	// output rather than being hidden inside an averaged duration.
	observedDocs int
}

// Benchmark_P2P_Replicate_Push measures live replication latency and throughput.
//
// A replicator from node 0 to node 1 is configured before the timed span begins, so the
// documents created on node 0 are pushed to node 1 as they are written. The timed span
// therefore covers "first document written on the source node" through to "all documents
// readable on the target node".
//
// Node spin-up, schema creation and replicator configuration all happen outside the
// timed span.
func Benchmark_P2P_Replicate_Push(b *testing.B) {
	for _, docCount := range pushDocCounts {
		b.Run(fmt.Sprintf("docs=%d", docCount), func(b *testing.B) {
			runReplicationBench(b, docCount, pushActions, true)
		})
	}
}

// Benchmark_P2P_Sync_Pull measures the latency and throughput of the explicit document
// sync (pull) path.
//
// The documents are created on node 0 while the nodes are disconnected, the peers are
// then connected, and only the explicit [testUtils.SyncDocs] request is timed. Node
// spin-up, schema creation, document creation and peer connection all happen outside the
// timed span.
//
// Note that `SyncDocuments` waits for a response from every active peer and applies a
// default five second deadline when the caller supplies no deadline of its own, so this
// benchmark is expected to be dominated by that deadline rather than by the cost of
// moving the documents. The `docs-synced` metric reports how many documents actually
// arrived within it.
//
// For the same reason no throughput metric is reported here: with the measured duration
// pinned to the deadline, a documents-per-second figure would be `docs / 5s` by
// construction and would say nothing about how fast the pull path can move documents.
func Benchmark_P2P_Sync_Pull(b *testing.B) {
	for _, docCount := range pullDocCounts {
		b.Run(fmt.Sprintf("docs=%d", docCount), func(b *testing.B) {
			runReplicationBench(b, docCount, pullActions, false)
		})
	}
}

// actionBuilder builds the action list for one benchmark iteration.
//
// Implementations are responsible for starting the benchmark timer (via the supplied
// [action.RunFunc] hooks) at the point at which replication is triggered, and for
// recording the spans into the supplied [syncSpans].
type actionBuilder func(b *testing.B, docCount int, spans *syncSpans) []any

// runReplicationBench runs build's test case b.N times and reports the averaged spans.
//
// reportThroughput controls whether a documents-per-second metric is reported; it is only
// meaningful where the measured duration reflects the cost of moving the documents.
func runReplicationBench(b *testing.B, docCount int, build actionBuilder, reportThroughput bool) {
	var (
		totalReplicated time.Duration
		totalVisible    time.Duration
		totalLocalWrite time.Duration
		totalDocs       int
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// The two nodes have to be built afresh for every iteration, and there is no
		// way to hoist that out of the test case, so the timer is stopped for the
		// duration of the setup and restarted by an action within the test case.
		b.StopTimer()

		var spans syncSpans

		testUtils.ExecuteTestCase(b, testUtils.TestCase{
			// Restricting the client type keeps the benchmark measuring the embedded Go
			// client only - running the same case through the HTTP and CLI clients would
			// fold transport cost into the reported numbers.
			SupportedClientTypes: immutable.Some([]state.ClientType{state.GoClientType}),
			// Restricting the database type likewise pins the benchmark to a single
			// store. Without it, setting any of the `DEFRA_*` store environment
			// variables would make ExecuteTestCase run the case once per store within
			// a single timed iteration, summing their cost into `ns/op` while each
			// run overwrote the spans recorded by the last.
			SupportedDatabaseTypes: immutable.Some([]state.DatabaseType{testUtils.BadgerIMType}),
			Actions:                build(b, docCount, &spans),
		})

		totalReplicated += spans.replicated
		totalVisible += spans.visible
		totalLocalWrite += spans.localWrite
		totalDocs += spans.observedDocs
	}

	reportSyncMetrics(b, totalReplicated, totalVisible, totalLocalWrite, totalDocs, reportThroughput)
}

// pushActions builds a test case in which a replicator pushes documents from node 0 to
// node 1 as they are created.
func pushActions(b *testing.B, docCount int, spans *syncSpans) []any {
	var start time.Time
	var writeEnd time.Time

	actions := make([]any, 0, docCount+8)
	actions = append(actions,
		testUtils.RandomNetworkingConfig(),
		testUtils.RandomNetworkingConfig(),
		&action.AddCollection{SDL: benchSDL},
		// The replicator is configured before the timed span so that the benchmark
		// measures replication of writes, not the cost of establishing the relationship.
		testUtils.AddReplicator{
			SourceNodeID: 0,
			TargetNodeID: 1,
		},
		action.NewRunFunc(func() {
			start = time.Now()
			b.StartTimer()
		}),
	)

	actions = append(actions, addDocActions(docCount)...)

	actions = append(actions,
		action.NewRunFunc(func() {
			writeEnd = time.Now()
		}),
		testUtils.WaitForSync{},
		action.NewRunFunc(func() {
			spans.replicated = time.Since(start)
			spans.localWrite = writeEnd.Sub(start)
		}),
		targetNodeRequest(spans),
		action.NewRunFunc(func() {
			spans.visible = time.Since(start)
			b.StopTimer()
		}),
	)

	return actions
}

// pullActions builds a test case in which node 1 explicitly requests documents that
// already exist on node 0.
func pullActions(b *testing.B, docCount int, spans *syncSpans) []any {
	var start time.Time
	var writeStart time.Time

	docIDs := make([]int, docCount)
	sourceNodes := make([]int, docCount)
	for i := range docIDs {
		docIDs[i] = i
		// Every document originates on node 0.
		sourceNodes[i] = 0
	}

	actions := make([]any, 0, docCount+10)
	actions = append(actions,
		testUtils.RandomNetworkingConfig(),
		testUtils.RandomNetworkingConfig(),
		&action.AddCollection{SDL: benchSDL},
		action.NewRunFunc(func() {
			writeStart = time.Now()
		}),
	)

	actions = append(actions, addDocActions(docCount)...)

	actions = append(actions,
		action.NewRunFunc(func() {
			spans.localWrite = time.Since(writeStart)
		}),
		// Connecting the peers is deliberately kept outside of the timed span - we are
		// interested in the cost of replicating documents, not in the cost of
		// establishing a libp2p connection.
		testUtils.ConnectPeers{
			SourceNodeID: 0,
			TargetNodeID: 1,
		},
		action.NewRunFunc(func() {
			start = time.Now()
			b.StartTimer()
		}),
		testUtils.SyncDocs{
			NodeID:       1,
			CollectionID: 0,
			DocIDs:       docIDs,
			SourceNodes:  sourceNodes,
		},
		testUtils.WaitForSync{},
		action.NewRunFunc(func() {
			spans.replicated = time.Since(start)
		}),
		targetNodeRequest(spans),
		action.NewRunFunc(func() {
			spans.visible = time.Since(start)
			b.StopTimer()
		}),
	)

	return actions
}

// addDocActions builds the actions that create docCount documents on node 0.
func addDocActions(docCount int) []any {
	actions := make([]any, 0, docCount)
	for i := 0; i < docCount; i++ {
		actions = append(actions, &action.AddDoc{
			NodeID: immutable.Some(0),
			DocMap: map[string]any{
				"Name": fmt.Sprintf("User-%d", i),
				"Age":  i % 100,
			},
		})
	}

	return actions
}

// targetNodeRequest returns the request used to establish that the replicated documents
// are readable on the target node.
//
// The number of documents returned is recorded rather than asserted against, so that a
// replication path that only partially completes reports a reduced `docs-synced` metric
// instead of failing the benchmark. A request that returns nothing at all is a failure -
// there would be no meaningful duration to report.
func targetNodeRequest(spans *syncSpans) *action.Request {
	return &action.Request{
		NodeID: immutable.Some(1),
		Request: `query {
			Users {
				Name
			}
		}`,
		Asserter: action.ResultAsserterFunc(func(t testing.TB, result map[string]any) (bool, string) {
			users, ok := result["Users"].([]map[string]any)
			if !ok {
				t.Errorf("expected `Users` to be a document array, got %T", result["Users"])
				return false, ""
			}
			if len(users) == 0 {
				t.Errorf("no documents were replicated to the target node")
				return false, ""
			}

			spans.observedDocs = len(users)

			return true, ""
		}),
	}
}

// reportSyncMetrics reports the per-iteration averages of the captured spans.
//
// The metrics are reported in addition to the standard `ns/op`, which covers the timed
// span only.
func reportSyncMetrics(
	b *testing.B,
	totalReplicated time.Duration,
	totalVisible time.Duration,
	totalLocalWrite time.Duration,
	totalDocs int,
	reportThroughput bool,
) {
	if b.N == 0 {
		return
	}

	iterations := float64(b.N)
	replicatedSeconds := totalReplicated.Seconds() / iterations
	docs := float64(totalDocs) / iterations

	b.ReportMetric(replicatedSeconds*1e3, "ms/replicated")
	b.ReportMetric(totalVisible.Seconds()/iterations*1e3, "ms/visible")
	b.ReportMetric(totalLocalWrite.Seconds()/iterations*1e3, "ms/local-write")
	b.ReportMetric(docs, "docs-synced")

	if reportThroughput && replicatedSeconds > 0 {
		b.ReportMetric(docs/replicatedSeconds, "docs/s")
	}
}
