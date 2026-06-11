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
	"context"
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

func TestForwardWifiAwareEvents_PublishesStatusOnStartAndClose(t *testing.T) {
	bus := event.NewChannelBus(16, 16)
	defer bus.Close()
	sub, err := bus.Subscribe(event.WifiAwareStatusName)
	require.NoError(t, err)

	events := make(chan wifiaware.Event)
	done := make(chan struct{})
	go func() {
		forwardWifiAwareEvents(bus, events)
		close(done)
	}()

	msg := receiveMessage(t, sub)
	status, ok := msg.Data.(event.WifiAwareStatus)
	require.True(t, ok, "expected WifiAwareStatus payload, got %T", msg.Data)
	assert.True(t, status.Running)

	close(events)
	msg = receiveMessage(t, sub)
	status, ok = msg.Data.(event.WifiAwareStatus)
	require.True(t, ok, "expected WifiAwareStatus payload, got %T", msg.Data)
	assert.False(t, status.Running)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("forwarder did not exit after source channel closed")
	}
}

func TestForwardWifiAwareEvents_PublishesPeerLost(t *testing.T) {
	bus := event.NewChannelBus(16, 16)
	defer bus.Close()
	sub, err := bus.Subscribe(event.WifiAwarePeerName)
	require.NoError(t, err)

	events := make(chan wifiaware.Event, 1)
	go forwardWifiAwareEvents(bus, events)

	events <- wifiaware.Event{
		Type: wifiaware.PeerLost,
		Peer: peer.AddrInfo{ID: peer.ID("peer-x")},
	}

	msg := receiveMessage(t, sub)
	data, ok := msg.Data.(event.WifiAwarePeer)
	require.True(t, ok, "expected WifiAwarePeer payload, got %T", msg.Data)
	assert.Equal(t, "LOST", data.EventType)

	close(events)
}

func TestNode_Start_WithWifiAware_ForwardsEvents(t *testing.T) {
	ctx := context.Background()
	n, err := New(ctx,
		options.Node().
			SetDisableAPI(true).
			P2P().
			SetListenAddresses("/ip4/127.0.0.1/tcp/0").
			SetEnableWifiAware(true).
			Node().
			Store().SetPath(t.TempDir()).
			Node(),
	)
	require.NoError(t, err)
	require.NoError(t, n.Start(ctx))

	// Whether the subscriber catches the initial Running:true depends on
	// goroutine scheduling (covered deterministically by the forwarder unit
	// tests). The lifecycle guarantee asserted here is the terminal state:
	// closing the node terminates the forwarder, which publishes
	// Running:false before the bus shuts down.
	sub, err := n.DB.Events().Subscribe(event.WifiAwareStatusName)
	require.NoError(t, err)

	require.NoError(t, n.Close(ctx))

	for {
		msg := receiveMessage(t, sub)
		status, ok := msg.Data.(event.WifiAwareStatus)
		require.True(t, ok, "expected WifiAwareStatus payload, got %T", msg.Data)
		if !status.Running {
			return
		}
	}
}

func TestBuildP2POpts_WifiAwareDisabledByDefault(t *testing.T) {
	opts := options.NodeP2POptions{}

	applied := p2p.DefaultOptions()
	for _, opt := range buildP2POpts(&opts) {
		opt(applied)
	}

	assert.False(t, applied.EnableWifiAware)
}
