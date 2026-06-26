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

// These benchmarks isolate the always-on cost of the set-reconciliation
// maintenance hooks: when reconciliation is enabled, every committed composite
// block also writes one ordered-index entry in the same transaction. The gate
// needs the "is it worth it" number — how much that index write adds to local
// write latency when nothing is syncing.
//
// They are local-only (no peer, no CountingHost): the timed region is purely the
// seed (create + update) write path, run with the flag off vs on. Compare with
// benchstat (ns/op + the reported heapDeltaMiB/op):
//
//	benchstat -filter '.name:/LocalWrite/' apple-m4-pro.txt
//
// or split the two and diff Off vs On directly.

// runLocalWrite times seeding docCount documents (each updatesPerDoc commits deep)
// into a fresh node, with set reconciliation off or on. With it on, each composite
// commit additionally writes its ordered-index entry, so the ns/op delta is the
// maintenance-hook overhead.
func runLocalWrite(b *testing.B, reconcile bool, docCount, updatesPerDoc int) {
	requireSyncBenchEnabled(b)
	ctx := context.Background()

	var heapTotal float64
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var node *syncNode
		if reconcile {
			node = startSyncNode(ctx, b, false, withReconciliation)
		} else {
			node = startSyncNode(ctx, b, false)
		}
		col := node.addCollection(ctx, b)
		heapBefore := heapAllocMiB()
		b.StartTimer()
		seedDocs(ctx, b, col, docCount, updatesPerDoc)
		b.StopTimer()
		heapTotal += heapAllocMiB() - heapBefore
		node.close(ctx)
	}
	b.ReportMetric(heapTotal/float64(b.N), "heapDeltaMiB/op")
}

// 50 docs, 5 updates each (~300 composite commits) — enough writes for a stable
// benchstat delta on the one extra KV Set per commit.
func Benchmark_LocalWrite_ReconcileOff_docs50_upd5(b *testing.B) {
	runLocalWrite(b, false, 50, 5)
}

func Benchmark_LocalWrite_ReconcileOn_docs50_upd5(b *testing.B) {
	runLocalWrite(b, true, 50, 5)
}
