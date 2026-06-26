// Copyright 2025 Democratized Data Foundation
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

	"github.com/ipfs/go-cid"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/internal/core"
	coreblock "github.com/sourcenetwork/defradb/internal/core/block"
	"github.com/sourcenetwork/defradb/internal/db/p2p/negentropy"
	"github.com/sourcenetwork/defradb/internal/db/p2p/protocol"
	iIdentity "github.com/sourcenetwork/defradb/internal/identity"
	"github.com/sourcenetwork/defradb/internal/keys"
)

// reconcileSessionTimeout bounds an entire reconciliation session (up to
// negentropy.MaxRounds round-trips). It fixes the otherwise-unbounded session by
// giving the whole driver a deadline distinct from each round's timeout.
const reconcileSessionTimeout = negentropy.MaxRounds * networkRequestTimeout

// reconcileCommProcessor implements CommProcessor for range-based set
// reconciliation. It is stateless per the Negentropy design: each request is
// answered purely from the local set for the requested scope, so the responder
// holds no per-session state.
type reconcileCommProcessor struct {
	p2p *P2P
}

func (proc *reconcileCommProcessor) ProcessRequest(
	ctx context.Context,
	req protocol.ReconcileMessage,
) (protocol.ReconcileMessage, error) {
	if req.Scope.Kind != protocol.ScopeDocHeads {
		return protocol.ReconcileMessage{}, NewErrUnsupportedReconcileScope(req.Scope.Kind)
	}

	// The inbound stream handler hands us a context.Background(); bound the local
	// read so a slow store cannot pin the stream open. (Full context plumbing
	// from the stream handler is a later milestone.)
	ctx, cancel := context.WithTimeout(ctx, networkRequestTimeout)
	defer cancel()

	local, err := proc.p2p.localVectorForDoc(ctx, req.Scope.ID)
	if err != nil {
		return protocol.ReconcileMessage{}, err
	}

	return respondReconcile(local, req)
}

// respondReconcile answers one reconcile request from a local set, translating in
// and out of the wire form. It is the stateless core of the responder, factored
// out so it can be driven without a datastore.
func respondReconcile(local negentropy.Storage, req protocol.ReconcileMessage) (protocol.ReconcileMessage, error) {
	incoming, err := protocol.FromWire(req)
	if err != nil {
		return protocol.ReconcileMessage{}, err
	}

	out, err := negentropy.Respond(local, incoming)
	if err != nil {
		return protocol.ReconcileMessage{}, err
	}

	return protocol.ToWire(req.Scope, out), nil
}

// localVectorForDoc builds the reconciliation set for a document: its composite
// heads as a sealed [negentropy.Vector]. Heads are ordered by plain CID bytes
// (height == 0), which both peers compute identically for any shared CID, so the
// sort keys agree cross-peer without loading the head blocks.
func (p *P2P) localVectorForDoc(ctx context.Context, docID string) (*negentropy.Vector, error) {
	key := keys.HeadstoreDocKey{
		DocID:   docID,
		FieldID: core.COMPOSITE_NAMESPACE,
	}

	cids, _, err := coreblock.NewHeadSet(p.db.Multistore().Headstore(), key).List(ctx)
	if err != nil {
		return nil, NewErrGetDocHeads(err, docID)
	}
	return vectorFromCIDs(cids)
}

// vectorFromCIDs seals a list of head CIDs into a reconciliation [negentropy.Vector]
// at height 0 (plain CID-byte order), the cross-peer-stable ordering used for the
// heads-only slice.
func vectorFromCIDs(cids []cid.Cid) (*negentropy.Vector, error) {
	builder := negentropy.NewVectorBuilder(len(cids))
	for _, c := range cids {
		builder.Add(0, c.Bytes())
	}
	return builder.Build()
}

// ReconcileDocument runs a range-based set reconciliation session against peerID
// for a single document's heads, then converges both nodes: the local node pulls
// the heads it is missing (fetch + merge through the existing DAG-sync path) and
// pushes the heads the peer is missing (through the existing pushlog path).
//
// The set reconciled is small (a document's heads), so the win here is the
// mechanism, not asymptotic bandwidth; per-collection full-set reconciliation is
// a later milestone. Reconciliation only discovers the divergent CIDs — causal
// validation stays with syncDAG/Merge.
func (p *P2P) ReconcileDocument(
	ctx context.Context,
	peerID string,
	collectionName string,
	docID string,
) error {
	if p.reconcileProtocol == nil {
		return ErrSetReconciliationDisabled
	}

	cols, err := p.db.GetCollections(
		ctx,
		options.WithIdentity(
			options.GetCollections().SetCollectionName(collectionName),
			iIdentity.FromContext(ctx),
		),
	)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return client.NewErrCollectionNotFoundForName(collectionName)
	}
	collectionID := cols[0].Version().CollectionID

	sessionCtx, cancel := context.WithTimeout(ctx, reconcileSessionTimeout)
	defer cancel()

	local, err := p.localVectorForDoc(sessionCtx, docID)
	if err != nil {
		return err
	}

	scope := protocol.ReconcileScope{Kind: protocol.ScopeDocHeads, ID: docID}
	it := negentropy.NewInitiator(local)
	msg := it.Initiate()

	for {
		reply, err := p.reconcileRound(sessionCtx, peerID, scope, msg)
		if err != nil {
			return err
		}

		incoming, err := protocol.FromWire(reply)
		if err != nil {
			return err
		}

		next, done := it.Reconcile(incoming)
		if it.Err() != nil {
			return it.Err() // e.g. negentropy.ErrRoundCapExceeded — bounded, never hangs
		}
		if done {
			break
		}
		msg = next
	}

	if err := p.pullReconcileNeed(sessionCtx, peerID, collectionID, docID, it.Need()); err != nil {
		return err
	}
	return p.pushReconcileHave(sessionCtx, peerID, collectionID, docID, it.Have())
}

// reconcileRound performs one request/reply round, bounded by a per-round timeout.
// Each round mints a fresh message ID, so the stateless responder answers it in
// isolation.
func (p *P2P) reconcileRound(
	ctx context.Context,
	peerID string,
	scope protocol.ReconcileScope,
	msg negentropy.Message,
) (protocol.ReconcileMessage, error) {
	roundCtx, cancel := context.WithTimeout(ctx, networkRequestTimeout)
	defer cancel()

	reply, err := p.reconcileProtocol.SendRequest(roundCtx, protocol.ToWire(scope, msg), peerID)
	if err != nil {
		return protocol.ReconcileMessage{}, err
	}
	// Defensive guard: a responder that errored returns a reply carrying an
	// ErrMessage. (message.Send now surfaces this directly too, but the guard
	// keeps reconciliation correct regardless of that path.)
	if reply.GetErrMessage() != "" {
		return protocol.ReconcileMessage{}, NewErrReconcileResponder(reply.GetErrMessage())
	}
	return reply, nil
}

// pullReconcileNeed fetches and merges the heads the local node is missing, using
// the same DAG-sync + merge path as broadcast head sync. The IsMerged
// short-circuit keeps each walk proportional to the diff.
func (p *P2P) pullReconcileNeed(
	ctx context.Context,
	peerID, collectionID, docID string,
	need [][]byte,
) error {
	for _, idBytes := range need {
		c, err := cid.Cast(idBytes)
		if err != nil {
			return err
		}
		if err := p.syncDocumentAndMerge(ctx, peerID, collectionID, docID, c); err != nil {
			return err
		}
	}
	return nil
}

// pushReconcileHave pushes the heads the peer is missing, reusing the pushlog
// path. Only the divergent heads are sent (not every head), keeping the pushed
// bytes proportional to the diff; the peer's pushlog handler merges them.
func (p *P2P) pushReconcileHave(
	ctx context.Context,
	peerID, collectionID, docID string,
	have [][]byte,
) error {
	if len(have) == 0 {
		return nil
	}

	wanted := make(map[string]struct{}, len(have))
	for _, idBytes := range have {
		wanted[string(idBytes)] = struct{}{}
	}

	heads, err := p.getHeads(ctx, docID)
	if err != nil {
		return err
	}

	for _, h := range heads {
		if _, ok := wanted[string(h.cid.Bytes())]; !ok {
			continue
		}
		rawblock, err := h.block.Marshal()
		if err != nil {
			return NewErrMarshalBlock(err, docID, h.cid.String())
		}

		roundCtx, cancel := context.WithTimeout(ctx, networkRequestTimeout)
		pushReq := protocol.PushLogRequest{
			DocID:        docID,
			CID:          h.cid.Bytes(),
			CollectionID: collectionID,
			Creator:      p.host.ID(),
			Block:        rawblock,
		}
		_, err = p.replicatorProtocol.SendRequest(roundCtx, pushReq, peerID)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}
