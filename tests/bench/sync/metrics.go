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
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/internal/datastore"
)

// blockstoreStats counts the blocks and total block bytes currently held in the
// node's blockstore. Diffing it around a sync yields the blocks and payload bytes
// the receiver actually fetched and persisted — a robust measure of transfer that
// does not depend on instrumenting the content-addressed fetch path.
//
// The node's chunk size must match what it persists with: badger is value-size
// limited so blocks are stored chunked, and reading them back without the same
// chunk configuration yields unparseable keys and a zero count.
func blockstoreStats(ctx context.Context, tb testing.TB, sn *syncNode) (count int, bytes int64) {
	tb.Helper()
	bs := datastore.BlockstoreFrom(sn.db.Rootstore(), sn.node.Options().DB.ChunkSize)
	ch, err := bs.AllKeysChan(ctx)
	require.NoError(tb, err)
	for c := range ch {
		count++
		blk, err := bs.Get(ctx, c)
		if err != nil {
			continue
		}
		bytes += int64(len(blk.RawData()))
	}
	return count, bytes
}

// heapAllocMiB returns the current heap allocation in MiB after a GC, for a
// stable (if coarse) memory reading around the timed region.
func heapAllocMiB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1024 * 1024)
}

// syncSample holds the per-iteration measurements of one reconcile/sync.
type syncSample struct {
	syncMs        float64 // wall-clock from sync issue to last merge
	ctrlBytesSent int64   // control-message bytes the receiver sent (e.g. doc-sync request)
	ctrlBytesRecv int64   // control-message bytes the receiver received (e.g. heads reply)
	ctrlMsgs      int64   // control messages sent+received (round-trip proxy)
	blocks        int     // blocks added to the receiver's blockstore
	blockBytes    int64   // payload bytes added to the receiver's blockstore
	heapDeltaMiB  float64
}

// report aggregates samples and emits them as Go benchmark metrics, averaged per
// op. Wall-clock per op is reported natively by the framework (ns/op).
func report(b *testing.B, samples []syncSample) {
	if len(samples) == 0 {
		return
	}
	var ctrlSent, ctrlRecv, ctrlMsgs, blockBytes int64
	var blocks int
	var heap, syncMs float64
	for _, s := range samples {
		syncMs += s.syncMs
		ctrlSent += s.ctrlBytesSent
		ctrlRecv += s.ctrlBytesRecv
		ctrlMsgs += s.ctrlMsgs
		blocks += s.blocks
		blockBytes += s.blockBytes
		heap += s.heapDeltaMiB
	}
	n := float64(len(samples))
	b.ReportMetric(syncMs/n, "syncMs/op")
	b.ReportMetric(float64(ctrlSent+ctrlRecv)/n, "ctrlBytes/op")
	b.ReportMetric(float64(ctrlMsgs)/n, "ctrlMsgs/op")
	b.ReportMetric(float64(blocks)/n, "blocks/op")
	b.ReportMetric(float64(blockBytes)/n, "blockBytes/op")
	b.ReportMetric(heap/n, "heapDeltaMiB/op")
}
