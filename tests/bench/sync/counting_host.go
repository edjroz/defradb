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

// Package sync contains the Phase 0 benchmark harness for P2P sync: a counting
// host that instruments wire traffic, DAG-shape scenario seeds, a metrics
// recorder, and baselines of the existing full-DAG-walk sync. See README.md.
package sync

import (
	"context"
	"io"
	"sort"
	"sync"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/node"
)

// ProtoCounter holds byte and message counts for a single protocol (or pubsub
// topic), split by direction.
type ProtoCounter struct {
	BytesSent int64
	BytesRecv int64
	MsgsSent  int64
	MsgsRecv  int64
}

// Counters accumulates per-protocol wire traffic observed by a CountingHost.
// It is safe for concurrent use: the p2p stack invokes Send and stream/pubsub
// handlers from many goroutines.
type Counters struct {
	mu     sync.Mutex
	protos map[string]*ProtoCounter
}

// NewCounters returns an empty, ready-to-use Counters.
func NewCounters() *Counters {
	return &Counters{protos: make(map[string]*ProtoCounter)}
}

// must be called with c.mu held.
func (c *Counters) at(key string) *ProtoCounter {
	pc := c.protos[key]
	if pc == nil {
		pc = &ProtoCounter{}
		c.protos[key] = pc
	}
	return pc
}

func (c *Counters) addSent(key string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pc := c.at(key)
	pc.BytesSent += int64(n)
	pc.MsgsSent++
}

func (c *Counters) addRecv(key string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pc := c.at(key)
	pc.BytesRecv += int64(n)
	pc.MsgsRecv++
}

// Reset clears all accumulated counts.
func (c *Counters) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.protos = make(map[string]*ProtoCounter)
}

// CountersSnapshot is an immutable view of a Counters at a point in time.
type CountersSnapshot struct {
	// Protos maps protocol-ID / "pubsub:<topic>" to its counts.
	Protos map[string]ProtoCounter
	// Aggregates across all protocols.
	TotalBytesSent int64
	TotalBytesRecv int64
	TotalMsgsSent  int64
	TotalMsgsRecv  int64
}

// TotalBytes returns sent+received bytes across all protocols. For two peers
// reconciling, summing both peers' TotalBytes double-counts the link; use a
// single peer's view (or one direction) as the comparison metric.
func (s CountersSnapshot) TotalBytes() int64 {
	return s.TotalBytesSent + s.TotalBytesRecv
}

// SortedProtos returns the protocol keys in deterministic order, for stable
// reporting.
func (s CountersSnapshot) SortedProtos() []string {
	keys := make([]string, 0, len(s.Protos))
	for k := range s.Protos {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Snapshot returns a deep copy of the current counts.
func (c *Counters) Snapshot() CountersSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := CountersSnapshot{Protos: make(map[string]ProtoCounter, len(c.protos))}
	for k, pc := range c.protos {
		out.Protos[k] = *pc
		out.TotalBytesSent += pc.BytesSent
		out.TotalBytesRecv += pc.BytesRecv
		out.TotalMsgsSent += pc.MsgsSent
		out.TotalMsgsRecv += pc.MsgsRecv
	}
	return out
}

// pubsubKey namespaces pubsub topics so they don't collide with stream protocol
// IDs in the same map.
func pubsubKey(topic string) string { return "pubsub:" + topic }

// CountingHost wraps a node.Peer and records per-protocol wire traffic without
// altering behaviour. It instruments the control-message surface used by the
// db/p2p layer: Send (outbound stream requests/responses), SetStreamHandler
// (inbound stream bytes), and the pubsub publish/subscribe paths.
//
// Block-transfer bytes are deliberately NOT counted here. Blocks move over the
// content-addressed block service (bitswap), whose reads mix local and remote
// fetches; the receiver's blockstore growth (see metrics.go BlockstoreStats) is
// a cleaner, less fragile measure of bytes actually transferred. The counting
// host therefore captures control/coordination overhead; the blockstore diff
// captures payload.
type CountingHost struct {
	node.Peer
	counters *Counters
}

// NewCountingHost wraps inner, recording traffic into counters.
func NewCountingHost(inner node.Peer, counters *Counters) *CountingHost {
	return &CountingHost{Peer: inner, counters: counters}
}

// Decorator returns an options.P2PHostDecorator that wraps the node's host in a
// CountingHost recording into counters. Pass it via
// options.NodeP2P().SetHostDecorator(sync.Decorator(c)).
func Decorator(counters *Counters) options.P2PHostDecorator {
	return func(host any) any {
		if p, ok := host.(node.Peer); ok {
			return NewCountingHost(p, counters)
		}
		// Unexpected type: leave the host untouched (node ignores non-Peer returns).
		return host
	}
}

// Send records the outbound payload size against protocolID, counting one
// message (≈ one round-trip initiation) per call.
func (h *CountingHost) Send(ctx context.Context, data []byte, peerID string, protocolID string) error {
	h.counters.addSent(protocolID, len(data))
	return h.Peer.Send(ctx, data, peerID, protocolID)
}

// SetStreamHandler wraps the handler's reader so inbound bytes for protocolID
// are counted as the real handler consumes them.
func (h *CountingHost) SetStreamHandler(protocolID string, handler client.StreamHandler) {
	h.Peer.SetStreamHandler(protocolID, func(stream io.Reader, peerID string) {
		cr := &countingReader{r: stream}
		handler(cr, peerID)
		h.counters.addRecv(protocolID, int(cr.n))
	})
}

// AddPubSubTopic wraps the message handler so inbound pubsub bytes (and any
// synchronous response bytes) are attributed to the topic.
func (h *CountingHost) AddPubSubTopic(
	topic string,
	subscribe bool,
	handler client.PubsubMessageHandler,
	eventHandler client.PeerEventHandler,
) error {
	wrapped := func(from string, t string, msg []byte) ([]byte, error) {
		h.counters.addRecv(pubsubKey(t), len(msg))
		resp, err := handler(from, t, msg)
		if len(resp) > 0 {
			h.counters.addSent(pubsubKey(t), len(resp))
		}
		return resp, err
	}
	return h.Peer.AddPubSubTopic(topic, subscribe, wrapped, eventHandler)
}

// PublishToTopicAsync records the published payload size against the topic.
func (h *CountingHost) PublishToTopicAsync(ctx context.Context, topic string, data []byte) error {
	h.counters.addSent(pubsubKey(topic), len(data))
	return h.Peer.PublishToTopicAsync(ctx, topic, data)
}

// PublishToTopic records the published payload size against the topic.
func (h *CountingHost) PublishToTopic(
	ctx context.Context,
	topic string,
	data []byte,
	withMultiResponse bool,
) (<-chan client.PubsubResponse, error) {
	h.counters.addSent(pubsubKey(topic), len(data))
	return h.Peer.PublishToTopic(ctx, topic, data, withMultiResponse)
}

// countingReader tallies bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.n += int64(n)
	return n, err
}
