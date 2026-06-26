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

// runSyncOneDocChanged is the default-broadcast contrast to
// runReconcileCollectionOneDocChanged: a docCount-document shared base, exactly one
// doc diverged by one commit, converged via SyncDocuments. Only one merge lands,
// but the sync request must enumerate ALL docCount docIDs (it cannot know which one
// changed), so the control cost scales with the collection size while the
// reconcile path's tracks the single-block diff. Measuring both at docCount 50 and
// 200 shows the crossover.
func runSyncOneDocChanged(b *testing.B, docCount int) {
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
		recv.measuredSync(ctx, b, docIDs, docCount) // untimed: establish shared base
		applyTail(ctx, b, srcCol, docIDs[:1], 1)    // diverge exactly one doc

		recv.counters.Reset()
		blocksBefore, bytesBefore := blockstoreStats(ctx, b, recv)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		dur := recv.measuredSync(ctx, b, docIDs, 1) // all docIDs listed, one merge expected
		b.StopTimer()

		s := sampleReceiver(ctx, b, recv, blocksBefore, bytesBefore, heapBefore)
		s.syncMs = float64(dur.Microseconds()) / 1000
		samples = append(samples, s)
		recv.close(ctx)
		src.close(ctx)
	}
	report(b, samples)
}

// Default-broadcast contrasts to the OneDocChanged reconcile benchmarks. Run the
// docs200 case with -benchtime=1x.
func Benchmark_Sync_OneDocChanged_docs50(b *testing.B)  { runSyncOneDocChanged(b, 50) }
func Benchmark_Sync_OneDocChanged_docs100(b *testing.B) { runSyncOneDocChanged(b, 100) }
func Benchmark_Sync_OneDocChanged_docs200(b *testing.B) { runSyncOneDocChanged(b, 200) }

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
