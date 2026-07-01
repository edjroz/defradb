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

//go:build !js

package sync

import (
	"context"
	"testing"
)

// These benchmarks baseline the EXISTING full-DAG-walk sync (SyncDocuments ->
// syncDAG -> db.Merge) across the three RFC workloads, so later reconciliation
// phases have a like-for-like comparison. Each measures, per op: wall-clock
// (native ns/op), control-message bytes/round-trips (CountingHost on the
// receiver), and blocks/payload-bytes transferred (receiver blockstore diff).
//
// Naming: Benchmark_Sync_<Scenario>_<shape>. Scenarios are intentionally small
// so they remain CI-friendly; sweep the parameters locally for fuller curves.

// runColdStart benchmarks a fresh receiver (no shared blocks) pulling a
// docCount-document history, each doc updatesPerDoc commits deep. This is the
// worst case for the current walk: nothing short-circuits at IsMerged.
func runColdStart(b *testing.B, docCount, updatesPerDoc int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false)
		recv := startSyncNode(ctx, b, true)
		srcCol := src.addCollection(ctx, b)
		recv.addCollection(ctx, b)
		docIDs := seedDocs(ctx, b, srcCol, docCount, updatesPerDoc)
		connectNodes(ctx, b, recv, src)

		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.measuredSync(ctx, b, docIDs, docCount)
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// runIncrementalTail benchmarks the live-update common case: receiver and source
// share a base history, then the source gets `tail` further commits per doc. Only
// the tail should transfer; the shared base short-circuits at IsMerged.
func runIncrementalTail(b *testing.B, docCount, baseUpdates, tail int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false)
		recv := startSyncNode(ctx, b, true)
		srcCol := src.addCollection(ctx, b)
		recv.addCollection(ctx, b)
		docIDs := seedDocs(ctx, b, srcCol, docCount, baseUpdates)
		connectNodes(ctx, b, recv, src)
		recv.measuredSync(ctx, b, docIDs, docCount) // untimed: establish shared base
		applyTail(ctx, b, srcCol, docIDs, tail)

		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.measuredSync(ctx, b, docIDs, docCount)
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// runManyHead benchmarks post-partition divergence: both nodes share a base, then
// each independently applies `k` commits per doc (divergent heads). The timed
// region is the receiver pulling and merging the source's divergent heads on top
// of its own.
func runManyHead(b *testing.B, docCount, baseUpdates, k int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false)
		recv := startSyncNode(ctx, b, true)
		srcCol := src.addCollection(ctx, b)
		recvCol := recv.addCollection(ctx, b)
		docIDs := seedDocs(ctx, b, srcCol, docCount, baseUpdates)
		connectNodes(ctx, b, recv, src)
		recv.measuredSync(ctx, b, docIDs, docCount) // untimed: establish shared base
		applyTail(ctx, b, srcCol, docIDs, k)
		applyTail(ctx, b, recvCol, docIDs, k)

		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.measuredSync(ctx, b, docIDs, docCount)
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// runSyncDiff is the default-broadcast contrast to runReconcileCollectionDiff: a
// docCount-document shared base, diffCount docs diverged by one commit, converged
// via SyncDocuments. diffCount merges land, but the sync request must enumerate ALL
// docCount docIDs (it cannot know which changed), so the control cost scales with
// the collection size and is independent of diffCount — the O(n) baseline against
// which reconciliation's O(diff·log n) is compared.
func runSyncDiff(b *testing.B, docCount, diffCount int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false)
		recv := startSyncNode(ctx, b, true)
		srcCol := src.addCollection(ctx, b)
		recv.addCollection(ctx, b)
		docIDs := seedDocs(ctx, b, srcCol, docCount, 0)
		connectNodes(ctx, b, recv, src)
		recv.measuredSync(ctx, b, docIDs, docCount)      // untimed: establish shared base
		applyTail(ctx, b, srcCol, docIDs[:diffCount], 1) // diverge diffCount docs

		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.measuredSync(ctx, b, docIDs, diffCount) // all docIDs listed, diffCount merges
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// Default-broadcast contrasts. Vary collection size (diff fixed at 1): control ∝ docCount.
func Benchmark_Sync_OneDocChanged_docs50(b *testing.B)   { runSyncDiff(b, 50, 1) }
func Benchmark_Sync_OneDocChanged_docs100(b *testing.B)  { runSyncDiff(b, 100, 1) }
func Benchmark_Sync_OneDocChanged_docs200(b *testing.B)  { runSyncDiff(b, 200, 1) }
func Benchmark_Sync_OneDocChanged_docs500(b *testing.B)  { runSyncDiff(b, 500, 1) }
func Benchmark_Sync_OneDocChanged_docs1000(b *testing.B) { runSyncDiff(b, 1000, 1) }

// Vary the difference size (collection fixed at 500): control stays ~flat in diff.
func Benchmark_Sync_Diff_docs500_diff10(b *testing.B)  { runSyncDiff(b, 500, 10) }
func Benchmark_Sync_Diff_docs500_diff50(b *testing.B)  { runSyncDiff(b, 500, 50) }
func Benchmark_Sync_Diff_docs500_diff100(b *testing.B) { runSyncDiff(b, 500, 100) }
func Benchmark_Sync_Diff_docs500_diff250(b *testing.B) { runSyncDiff(b, 500, 250) }

// Denser high-diff points matching the reconcile sweep. Default control is
// diff-independent (it lists every docID regardless), so these should stay ~flat —
// the flat line reconciliation eventually crosses above.
func Benchmark_Sync_Diff_docs500_diff25(b *testing.B)  { runSyncDiff(b, 500, 25) }
func Benchmark_Sync_Diff_docs500_diff150(b *testing.B) { runSyncDiff(b, 500, 150) }
func Benchmark_Sync_Diff_docs500_diff200(b *testing.B) { runSyncDiff(b, 500, 200) }
func Benchmark_Sync_Diff_docs500_diff300(b *testing.B) { runSyncDiff(b, 500, 300) }
func Benchmark_Sync_Diff_docs500_diff350(b *testing.B) { runSyncDiff(b, 500, 350) }
func Benchmark_Sync_Diff_docs500_diff400(b *testing.B) { runSyncDiff(b, 500, 400) }
func Benchmark_Sync_Diff_docs500_diff450(b *testing.B) { runSyncDiff(b, 500, 450) }
func Benchmark_Sync_Diff_docs500_diff500(b *testing.B) { runSyncDiff(b, 500, 500) }

// runSyncNewDocs is the default-broadcast contrast to runReconcileCollectionNewDocs —
// the measured mirror of the modelled chart-3 baseline. A baseCount shared base, then
// newCount brand-new docs on the source. The receiver cannot discover new docIDs on its
// own, so (like the model's generous full-identifier baseline) it is handed the whole
// base+new docID list; its control therefore scales with the TOTAL doc count and grows
// with the diff — unlike the update-churn case (runSyncDiff) where the docID list is
// fixed and the default stays flat.
func runSyncNewDocs(b *testing.B, baseCount, newCount int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()
	samples := make([]syncSample, 0, b.N)

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		src := startSyncNode(ctx, b, false)
		recv := startSyncNode(ctx, b, true)
		srcCol := src.addCollection(ctx, b)
		recv.addCollection(ctx, b)
		baseIDs := seedDocs(ctx, b, srcCol, baseCount, 0)
		connectNodes(ctx, b, recv, src)
		recv.measuredSync(ctx, b, baseIDs, baseCount) // untimed: establish shared base
		newIDs := seedNamedDocs(ctx, b, srcCol, "new", newCount)
		allIDs := append(append([]string{}, baseIDs...), newIDs...) // recv must be told every docID

		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.measuredSync(ctx, b, allIDs, newCount) // all docIDs listed, newCount merges land
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

func Benchmark_Sync_NewDocs_base500_new1(b *testing.B)   { runSyncNewDocs(b, 500, 1) }
func Benchmark_Sync_NewDocs_base500_new10(b *testing.B)  { runSyncNewDocs(b, 500, 10) }
func Benchmark_Sync_NewDocs_base500_new50(b *testing.B)  { runSyncNewDocs(b, 500, 50) }
func Benchmark_Sync_NewDocs_base500_new100(b *testing.B) { runSyncNewDocs(b, 500, 100) }
func Benchmark_Sync_NewDocs_base500_new250(b *testing.B) { runSyncNewDocs(b, 500, 250) }
func Benchmark_Sync_NewDocs_base500_new500(b *testing.B) { runSyncNewDocs(b, 500, 500) }

// sampleReceiver snapshots the receiver's counters and blockstore growth.
func sampleReceiver(
	ctx context.Context,
	b *testing.B,
	recv *syncNode,
	blocksBefore int,
	bytesBefore int64,
	heapBefore float64,
) syncSample {
	snap := recv.counters.Snapshot()
	blocksAfter, bytesAfter := blockstoreStats(ctx, b, recv)
	return syncSample{
		ctrlBytesSent: snap.TotalBytesSent,
		ctrlBytesRecv: snap.TotalBytesRecv,
		ctrlMsgs:      snap.TotalMsgsSent + snap.TotalMsgsRecv,
		blocks:        blocksAfter - blocksBefore,
		blockBytes:    bytesAfter - bytesBefore,
		heapDeltaMiB:  heapAllocMiB() - heapBefore,
	}
}

func Benchmark_Sync_ColdStart_docs10_upd0(b *testing.B)  { runColdStart(b, 10, 0) }
func Benchmark_Sync_ColdStart_docs10_upd10(b *testing.B) { runColdStart(b, 10, 10) }
func Benchmark_Sync_ColdStart_docs50_upd0(b *testing.B)  { runColdStart(b, 50, 0) }

func Benchmark_Sync_IncrementalTail_docs10_base5_tail1(b *testing.B) {
	runIncrementalTail(b, 10, 5, 1)
}

func Benchmark_Sync_ManyHead_docs10_base5_k3(b *testing.B) { runManyHead(b, 10, 5, 3) }

// Deep-branch contrasts to the Benchmark_Reconcile_*_k50 cases: the divergent
// branches are 50 commits deep instead of 3. Both sync and reconciliation discover
// divergence head-first, so their control cost is expected to be ~depth-invariant;
// these let the approval report assert that head-to-head rather than claim it.
func Benchmark_Sync_SingleDoc_base5_k3(b *testing.B)           { runManyHead(b, 1, 5, 3) }
func Benchmark_Sync_SingleDoc_DeepFork_base5_k50(b *testing.B) { runManyHead(b, 1, 5, 50) }
func Benchmark_Sync_ManyHead_docs10_base5_k50(b *testing.B)    { runManyHead(b, 10, 5, 50) }
