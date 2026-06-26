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
)

const (
	// reconcileWorkerCount bounds the number of concurrent auto-triggered
	// reconciliation sessions, so a burst of peer joins cannot storm the network.
	reconcileWorkerCount = 4
	// reconcileQueueSize bounds the pending-job buffer.
	reconcileQueueSize = 256
)

// reconcileJob identifies an auto-triggered reconciliation: a peer and the scope
// (collection) to reconcile with it.
type reconcileJob struct {
	peerID       string
	collectionID string
}

// reconcileScheduler debounces and bounds auto-triggered reconciliation. A job that
// is already pending or running is dropped (so a burst of joins for the same scope
// triggers one session), and at most reconcileWorkerCount sessions run concurrently.
type reconcileScheduler struct {
	ctx       context.Context
	reconcile func(ctx context.Context, peerID, collectionID string)
	jobs      chan reconcileJob

	mu      sync.Mutex
	pending map[reconcileJob]struct{}
}

// newReconcileScheduler starts workers draining the job queue, each invoking
// reconcile. Workers stop when ctx is cancelled.
func newReconcileScheduler(
	ctx context.Context,
	workers int,
	reconcile func(ctx context.Context, peerID, collectionID string),
) *reconcileScheduler {
	s := &reconcileScheduler{
		ctx:       ctx,
		reconcile: reconcile,
		jobs:      make(chan reconcileJob, reconcileQueueSize),
		pending:   make(map[reconcileJob]struct{}),
	}
	for i := 0; i < workers; i++ {
		go s.worker()
	}
	return s
}

// enqueue schedules a reconciliation for (peerID, collectionID), unless an identical
// job is already pending or running (debounce). It never blocks the caller beyond the
// queue capacity; if the queue is full or the scheduler is shutting down it drops the
// job (a later join, or the periodic scheduler, will retry).
func (s *reconcileScheduler) enqueue(peerID, collectionID string) {
	job := reconcileJob{peerID: peerID, collectionID: collectionID}

	s.mu.Lock()
	if _, ok := s.pending[job]; ok {
		s.mu.Unlock()
		return
	}
	s.pending[job] = struct{}{}
	s.mu.Unlock()

	select {
	case s.jobs <- job:
	default:
		// Queue full — drop and clear pending so a future event can re-enqueue.
		s.mu.Lock()
		delete(s.pending, job)
		s.mu.Unlock()
	}
}

func (s *reconcileScheduler) worker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case job := <-s.jobs:
			s.reconcile(s.ctx, job.peerID, job.collectionID)
			s.mu.Lock()
			delete(s.pending, job)
			s.mu.Unlock()
		}
	}
}
