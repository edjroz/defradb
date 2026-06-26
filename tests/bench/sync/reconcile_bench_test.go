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

// runReconcileCollectionColdStart converges an empty receiver over a whole
// collection in ONE collection-scope reconciliation session (M2), versus the
// per-document broadcast doc-sync of Benchmark_Sync_ColdStart_*. This is the
// collection-scale measurement: one session discovers and fetches the full block
// set, so the control cost is a single logarithmic session rather than a
// per-document request list.
func runReconcileCollectionColdStart(b *testing.B, docCount, updatesPerDoc int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false, withReconciliation)
		recv := startSyncNode(ctx, b, true, withReconciliation)
		srcCol := src.addCollection(ctx, b)
		recv.addCollection(ctx, b)
		seedDocs(ctx, b, srcCol, docCount, updatesPerDoc)
		connectNodes(ctx, b, recv, src)

		srcPeerID := src.peerID(ctx, b)
		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.reconcileCollection(ctx, b, srcPeerID)
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// Direct contrast to Benchmark_Sync_ColdStart_*: the empty receiver pulls the whole
// collection, but via one reconciliation session instead of a doc-sync broadcast.
func Benchmark_Reconcile_Collection_ColdStart_docs10_upd0(b *testing.B) {
	runReconcileCollectionColdStart(b, 10, 0)
}

func Benchmark_Reconcile_Collection_ColdStart_docs50_upd0(b *testing.B) {
	runReconcileCollectionColdStart(b, 50, 0)
}

// runReconcileCollectionTail measures the partial-catch-up win: the receiver already
// shares a base, the source adds a small tail, and one collection-scope session
// fetches only the divergent blocks — the control cost should track the diff, not
// the collection size.
func runReconcileCollectionTail(b *testing.B, docCount, baseUpdates, tail int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false, withReconciliation)
		recv := startSyncNode(ctx, b, true, withReconciliation)
		srcCol := src.addCollection(ctx, b)
		recv.addCollection(ctx, b)
		docIDs := seedDocs(ctx, b, srcCol, docCount, baseUpdates)
		connectNodes(ctx, b, recv, src)
		recv.reconcileCollection(ctx, b, src.peerID(ctx, b)) // untimed: establish shared base
		applyTail(ctx, b, srcCol, docIDs, tail)

		srcPeerID := src.peerID(ctx, b)
		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.reconcileCollection(ctx, b, srcPeerID)
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// Large shared base, tiny tail: the regime where collection reconciliation's control
// cost tracks the diff rather than the collection size.
func Benchmark_Reconcile_Collection_Tail_docs50_base2_tail1(b *testing.B) {
	runReconcileCollectionTail(b, 50, 2, 1)
}

// runReconcileCollectionDiff is the asymptotic-win measurement: a shared base of
// docCount documents, of which diffCount diverge by one commit each. The receiver
// reconciles the whole collection in one session, so its control cost tracks
// O(diffCount · log docCount) — the range fingerprints prune the matching majority
// and only the divergent blocks are discovered + fetched. Its default contrast is
// runSyncDiff, where SyncDocuments must list all docCount docIDs (control ∝ docCount,
// independent of diffCount). Varying docCount (diff fixed) isolates the log-vs-linear
// behaviour; varying diffCount (docCount fixed) isolates the O(diff·log n) vs O(n)
// trade-off and the crossover where a large diff makes reconciliation lose.
func runReconcileCollectionDiff(b *testing.B, docCount, diffCount int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false, withReconciliation)
		recv := startSyncNode(ctx, b, true, withReconciliation)
		srcCol := src.addCollection(ctx, b)
		recv.addCollection(ctx, b)
		docIDs := seedDocs(ctx, b, srcCol, docCount, 0)
		connectNodes(ctx, b, recv, src)
		recv.reconcileCollection(ctx, b, src.peerID(ctx, b)) // untimed: establish shared base
		applyTail(ctx, b, srcCol, docIDs[:diffCount], 1)     // diverge diffCount docs

		srcPeerID := src.peerID(ctx, b)
		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.reconcileCollection(ctx, b, srcPeerID)
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// Vary collection size with the difference fixed at one doc: reconcile control
// should grow only ~log(docCount) while the default contrast grows ∝ docCount.
// Heavier setup at large sizes, so run with -benchtime=1x.
func Benchmark_Reconcile_Collection_OneDocChanged_docs50(b *testing.B) {
	runReconcileCollectionDiff(b, 50, 1)
}
func Benchmark_Reconcile_Collection_OneDocChanged_docs100(b *testing.B) {
	runReconcileCollectionDiff(b, 100, 1)
}
func Benchmark_Reconcile_Collection_OneDocChanged_docs200(b *testing.B) {
	runReconcileCollectionDiff(b, 200, 1)
}
func Benchmark_Reconcile_Collection_OneDocChanged_docs500(b *testing.B) {
	runReconcileCollectionDiff(b, 500, 1)
}
func Benchmark_Reconcile_Collection_OneDocChanged_docs1000(b *testing.B) {
	runReconcileCollectionDiff(b, 1000, 1)
}

// Vary the difference size with the collection fixed at 500 docs: reconcile control
// should grow ~O(diff · log n) and eventually cross above the (diff-independent)
// default broadcast once the diff is a large fraction of the collection.
func Benchmark_Reconcile_Collection_Diff_docs500_diff10(b *testing.B) {
	runReconcileCollectionDiff(b, 500, 10)
}
func Benchmark_Reconcile_Collection_Diff_docs500_diff50(b *testing.B) {
	runReconcileCollectionDiff(b, 500, 50)
}
func Benchmark_Reconcile_Collection_Diff_docs500_diff100(b *testing.B) {
	runReconcileCollectionDiff(b, 500, 100)
}
func Benchmark_Reconcile_Collection_Diff_docs500_diff250(b *testing.B) {
	runReconcileCollectionDiff(b, 500, 250)
}
