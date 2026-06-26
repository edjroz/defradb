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
	"testing"
)

// runReconcileManyHead mirrors runManyHead (post-partition divergence) but converges
// the receiver using range-based set reconciliation instead of the default
// broadcast doc-sync. Both peers share a base, then each independently applies k
// commits per doc; the timed region is the receiver reconciling every document
// against the source, which pulls the source's divergent heads and pushes its own.
//
// The CountingHost on the receiver captures the /defradb/reconcile_* and pushlog
// control traffic, so its ctrlBytes/ctrlMsgs are directly comparable to the
// equivalent Benchmark_Sync_ManyHead_* default-sync run. At these small
// per-document head-set sizes the figure measures per-session overhead and
// round-trips, not the asymptotic O(diff) win (that is the per-collection M2
// slice) — see tests/bench/reconcile/README.md.
func runReconcileManyHead(b *testing.B, docCount, baseUpdates, k int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false, withReconciliation)
		recv := startSyncNode(ctx, b, true, withReconciliation)
		srcCol := src.addCollection(ctx, b)
		recvCol := recv.addCollection(ctx, b)
		docIDs := seedDocs(ctx, b, srcCol, docCount, baseUpdates)
		connectNodes(ctx, b, recv, src)
		recv.measuredSync(ctx, b, docIDs, docCount) // untimed: establish shared base
		applyTail(ctx, b, srcCol, docIDs, k)
		applyTail(ctx, b, recvCol, docIDs, k)

		srcPeerID := src.peerID(ctx, b)
		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.reconcileDocs(ctx, b, srcPeerID, docIDs)
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// Direct contrast to Benchmark_Sync_ManyHead_docs10_base5_k3: same divergence
// shape, converged via reconciliation instead of broadcast doc-sync.
func Benchmark_Reconcile_ManyHead_docs10_base5_k3(b *testing.B) {
	runReconcileManyHead(b, 10, 5, 3)
}

// Single divergent document — isolates the per-session reconciliation overhead and
// round-trips with no per-document amortization.
func Benchmark_Reconcile_SingleDoc_base5_k3(b *testing.B) {
	runReconcileManyHead(b, 1, 5, 3)
}
