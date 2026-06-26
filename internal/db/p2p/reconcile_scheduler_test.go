// Copyright 2026 Democratized Data Foundation
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
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// A duplicate (peer, collection) job already pending/running is dropped, so a burst
// of JOIN events for the same scope triggers a single reconciliation.
func TestReconcileScheduler_DedupsInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	count := 0
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	s := newReconcileScheduler(ctx, 2, func(_ context.Context, peerID, collectionID string) {
		mu.Lock()
		count++
		mu.Unlock()
		started <- struct{}{}
		<-release // hold the worker so the duplicate enqueues see it pending
	})

	s.enqueue("peer", "col")
	<-started // first job is now running and marked pending

	// Duplicates while the job is in flight are dropped.
	s.enqueue("peer", "col")
	s.enqueue("peer", "col")

	mu.Lock()
	require.Equal(t, 1, count, "duplicate jobs must be deduped while one is in flight")
	mu.Unlock()

	close(release)
}

// Distinct (peer, collection) jobs run concurrently up to the worker count.
func TestReconcileScheduler_DistinctJobsRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	seen := make(chan string, 2)

	s := newReconcileScheduler(ctx, 2, func(_ context.Context, peerID, collectionID string) {
		seen <- collectionID
		wg.Done()
	})

	s.enqueue("peer", "colA")
	s.enqueue("peer", "colB")

	wg.Wait()
	close(seen)
	got := map[string]bool{}
	for c := range seen {
		got[c] = true
	}
	require.Equal(t, map[string]bool{"colA": true, "colB": true}, got)
}
