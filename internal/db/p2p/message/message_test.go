// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package message

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/client"
)

func TestReceive_StreamLargerThanMax_ReturnsErrMessageTooLarge(t *testing.T) {
	stream := bytes.NewReader(make([]byte, maxMessageSize+1))
	err := Receive(stream, "some peer ID", nil, &MetaData{})
	require.ErrorIs(t, err, ErrMessageTooLarge)
}

// errOnSendHost is a minimal client.Host that satisfies the signing path and, on
// Send, delivers a responder error reply into the caller's pending response
// channel — simulating a peer whose handler set an ErrMessage.
type errOnSendHost struct {
	client.Host // embedded; unused methods panic if ever called
	proto       *fakeProto
	errMsg      string
}

func (h *errOnSendHost) ID() string                       { return "test-peer" }
func (h *errOnSendHost) Pubkey() ([]byte, error)          { return []byte("pubkey"), nil }
func (h *errOnSendHost) Sign(data []byte) ([]byte, error) { return []byte("sig"), nil }

func (h *errOnSendHost) Send(_ context.Context, _ []byte, _ string, _ string) error {
	// By the time send() runs, Send registered the response channel. Deliver an
	// error reply onto it, the way a real responder's onRequest error path would.
	h.proto.deliverToPending(&MetaData{ErrMessage: h.errMsg})
	return nil
}

// fakeProto is an in-memory proto: it stores response channels by message ID and
// can push a reply onto the single pending one.
type fakeProto struct {
	host  client.Host
	mu    sync.Mutex
	chans map[string]chan Message
}

func (p *fakeProto) Host() client.Host { return p.host }

func (p *fakeProto) SetResponseChan(id string, ch chan Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chans[id] = ch
}

func (p *fakeProto) GetResponseChan(id string) (chan Message, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch, ok := p.chans[id]
	return ch, ok
}

func (p *fakeProto) DeleteResponseChan(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.chans, id)
}

func (p *fakeProto) deliverToPending(m Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ch := range p.chans {
		ch <- m
	}
}

// TestSend_PropagatesResponderError guards the fix at message.go:175: a reply
// carrying an ErrMessage (set by the responder) must surface as an error to the
// caller of Send, not be silently swallowed as a successful empty response.
func TestSend_PropagatesResponderError(t *testing.T) {
	proto := &fakeProto{chans: make(map[string]chan Message)}
	proto.host = &errOnSendHost{proto: proto, errMsg: "boom from responder"}

	_, err := Send[*MetaData](context.Background(), proto, &MetaData{}, "test-peer", "/proto")
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom from responder")
}
