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
	"time"

	"github.com/sourcenetwork/corelog"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/internal/keys"
)

const (
	// autoReconcileMaxAttempts bounds retries of an auto-triggered session. The
	// JOIN event can arrive before the libp2p connection is ready to open a new
	// stream, so a transient failure is retried after a short backoff.
	autoReconcileMaxAttempts = 3
	// autoReconcileRetryDelay is the backoff between auto-reconcile attempts.
	autoReconcileRetryDelay = 500 * time.Millisecond
)

// startReconcileAutoTrigger subscribes to peer-join events and auto-runs
// ReconcileCollection for shared collections, debounced behind a bounded scheduler.
// The subscriber goroutine runs until ctx is cancelled. It is only started when set
// reconciliation is enabled.
//
// Both peers receive a JOINED event when they connect on a shared collection topic,
// so each pulls independently and they converge mutually — no push is required.
func (p *P2P) startReconcileAutoTrigger(ctx context.Context) error {
	sub, err := p.db.Events().Subscribe(event.TopicPeerEventName)
	if err != nil {
		return err
	}
	scheduler := newReconcileScheduler(ctx, reconcileWorkerCount, p.autoReconcileCollection)

	go func() {
		defer p.db.Events().Unsubscribe(sub)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-sub.Message():
				if !ok {
					return
				}
				evt, ok := msg.Data.(event.TopicPeerEvent)
				if !ok || evt.EventType != client.PeerEventTypeJoined {
					continue
				}
				// For a collection pubsub topic, Topic == collectionID. Only
				// auto-reconcile collections we share; per-block ACP is still
				// enforced at merge/serve time (public/replicator scope).
				shared, err := p.sharesP2PCollection(ctx, evt.Topic)
				if err != nil || !shared {
					continue
				}
				scheduler.enqueue(evt.PeerID, evt.Topic)
			}
		}
	}()
	return nil
}

// sharesP2PCollection reports whether the node shares (subscribes to) the given
// collection over P2P.
func (p *P2P) sharesP2PCollection(ctx context.Context, collectionID string) (bool, error) {
	return p.db.Multistore().Systemstore().Has(ctx, keys.NewP2PCollectionKey(collectionID).Bytes())
}

// autoReconcileCollection runs a pull-only reconciliation session against the peer.
// It is pull-only because both peers receive the JOIN and pull independently, so they
// converge mutually without push. Errors are logged, never fatal (e.g. the peer may
// be an old/disabled node that rejects the protocol), and the join can precede stream
// readiness, so a transient failure is retried after a short backoff.
func (p *P2P) autoReconcileCollection(ctx context.Context, peerID, collectionID string) {
	var lastErr error
	for attempt := 0; attempt < autoReconcileMaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(autoReconcileRetryDelay):
			}
		}

		lastErr = p.reconcileCollectionScope(ctx, peerID, collectionID, false)
		if lastErr == nil {
			return
		}
	}

	log.ErrorE("Auto-reconcile failed", lastErr,
		corelog.String("PeerID", peerID),
		corelog.String("CollectionID", collectionID))
}
