// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !js

package node

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/sourcenetwork/go-p2p"
	"github.com/sourcenetwork/go-p2p/wifiaware"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/event"
)

func TestBuildP2POpts_WifiAware(t *testing.T) {
	opts := options.NodeP2POptions{EnableWifiAware: true}

	applied := p2p.DefaultOptions()
	for _, opt := range buildP2POpts(&opts) {
		opt(applied)
	}

	assert.True(t, applied.EnableWifiAware)
}

// receiveMessage waits for a bus message or fails the test on timeout.
func receiveMessage(t *testing.T, sub event.Subscription) event.Message {
	t.Helper()
	select {
	case msg := <-sub.Message():
		return msg
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bus message")
		return event.Message{}
	}
}

func TestForwardWifiAwareEvents_PublishesPeerFound(t *testing.T) {
	bus := event.NewChannelBus(16, 16)
	defer bus.Close()
	sub, err := bus.Subscribe(event.WifiAwarePeerName)
	require.NoError(t, err)

	events := make(chan wifiaware.Event, 1)
	done := make(chan struct{})
	go func() {
		forwardWifiAwareEvents(bus, events)
		close(done)
	}()

	addr := ma.StringCast("/ip4/192.168.1.5/tcp/9171")
	events <- wifiaware.Event{
		Type: wifiaware.PeerFound,
		Peer: peer.AddrInfo{ID: peer.ID("peer-x"), Addrs: []ma.Multiaddr{addr}},
	}

	msg := receiveMessage(t, sub)
	data, ok := msg.Data.(event.WifiAwarePeer)
	require.True(t, ok, "expected WifiAwarePeer payload, got %T", msg.Data)
	assert.Equal(t, "FOUND", data.EventType)
	assert.Equal(t, peer.ID("peer-x").String(), data.PeerID)
	assert.Equal(t, []string{addr.String()}, data.Addresses)

	close(events)
	<-done
}

func TestBuildP2POpts_WifiAwareDisabledByDefault(t *testing.T) {
	opts := options.NodeP2POptions{}

	applied := p2p.DefaultOptions()
	for _, opt := range buildP2POpts(&opts) {
		opt(applied)
	}

	assert.False(t, applied.EnableWifiAware)
}
