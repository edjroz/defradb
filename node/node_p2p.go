// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// P2P networking stack does not work in JS builds.
//
//go:build !js

package node

import (
	"context"

	"github.com/sourcenetwork/corekv"
	"github.com/sourcenetwork/go-p2p"
	"github.com/sourcenetwork/go-p2p/wifiaware"
	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/event"
	"github.com/sourcenetwork/defradb/internal/datastore"
)

// forwardWifiAwareEvents republishes Wi-Fi Aware discovery events onto the
// DefraDB event bus, returning when the source channel closes. Status events
// bracket the stream: Running true on entry, false on exit.
func forwardWifiAwareEvents(bus event.Bus, events <-chan wifiaware.Event) {
	bus.Publish(event.NewMessage(event.WifiAwareStatusName, event.WifiAwareStatus{Running: true}))
	defer bus.Publish(event.NewMessage(event.WifiAwareStatusName, event.WifiAwareStatus{Running: false}))
	for ev := range events {
		addrs := make([]string, len(ev.Peer.Addrs))
		for i, addr := range ev.Peer.Addrs {
			addrs[i] = addr.String()
		}
		bus.Publish(event.NewMessage(event.WifiAwarePeerName, event.WifiAwarePeer{
			PeerID:    ev.Peer.ID.String(),
			Addresses: addrs,
			EventType: string(ev.Type),
		}))
	}
}

// buildP2POpts translates node P2P options into go-p2p node options.
func buildP2POpts(opts *options.NodeP2POptions) []p2p.NodeOpt {
	var p2pOpts []p2p.NodeOpt
	if len(opts.ListenAddresses) > 0 {
		p2pOpts = append(p2pOpts, p2p.WithListenAddresses(opts.ListenAddresses...))
	}
	if len(opts.BootstrapPeers) > 0 {
		p2pOpts = append(p2pOpts, p2p.WithBootstrapPeers(opts.BootstrapPeers...))
	}
	p2pOpts = append(p2pOpts, p2p.WithEnablePubSub(opts.EnablePubSub))
	if opts.EnableRelay {
		p2pOpts = append(p2pOpts, p2p.WithEnableRelay(true))
	}
	if opts.EnableClearBackoffOnRetry {
		p2pOpts = append(p2pOpts, p2p.WithClearBackoffOnRetry(true))
	}
	if len(opts.PrivateKey) > 0 {
		p2pOpts = append(p2pOpts, p2p.WithPrivateKey(opts.PrivateKey))
	}
	if opts.EnableWifiAware {
		p2pOpts = append(p2pOpts, p2p.WithEnableWifiAware(true))
	}
	return p2pOpts
}

func (n *Node) startP2P(ctx context.Context, store corekv.ReaderWriter, chunkSize immutable.Option[int]) error {
	if n.opts.DisableP2P {
		return nil
	}

	p2pOpts := buildP2POpts(&n.opts.P2P)
	p2pOpts = append(p2pOpts, p2p.WithBlockstore(datastore.P2PBlockstoreFrom(store, chunkSize)))

	peer, err := p2p.NewPeer(ctx, p2pOpts...)
	if err != nil {
		return err
	}
	n.peer = peer
	return nil
}
