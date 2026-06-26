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
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"

	"github.com/sourcenetwork/corekv"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/errors"
	"github.com/sourcenetwork/defradb/internal/core"
	coreblock "github.com/sourcenetwork/defradb/internal/core/block"
	dbid "github.com/sourcenetwork/defradb/internal/db/id"
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
	// The inbound stream handler hands us a context.Background(); bound the local
	// read so a slow store cannot pin the stream open. (Full context plumbing
	// from the stream handler is a later milestone.)
	ctx, cancel := context.WithTimeout(ctx, networkRequestTimeout)
	defer cancel()

	local, err := proc.p2p.localStorageForScope(ctx, req.Scope)
	if err != nil {
		return protocol.ReconcileMessage{}, err
	}

	return respondReconcile(local, req)
}

// localStorageForScope builds the local reconciliation set for a scope.
func (p *P2P) localStorageForScope(ctx context.Context, scope protocol.ReconcileScope) (negentropy.Storage, error) {
	switch scope.Kind {
	case protocol.ScopeDocHeads:
		return p.localVectorForDoc(ctx, scope.ID)
	case protocol.ScopeCollectionBlocks:
		return p.localStorageForCollection(ctx, scope.ID)
	default:
		return nil, NewErrUnsupportedReconcileScope(scope.Kind)
	}
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

// collectionShortID resolves a collectionID to its local short ID via the
// systemstore (no transaction required).
func (p *P2P) collectionShortID(ctx context.Context, collectionID string) (uint32, error) {
	return dbid.GetUncachedShortCollectionID(ctx, collectionID, p.db.Multistore().Systemstore())
}

// localStorageForCollection builds the reconciliation set for a collection: every
// composite block CID recorded in the maintained reconcile index, ordered by real
// CRDT height then CID. Both peers store the same (height, cid) per shared block, so
// the sort keys agree cross-peer.
//
// It returns a [negentropy.SegmentTree] (built once from the index) so the many
// per-range fingerprints during the multi-round session are O(log n) rather than the
// Vector's O(n) scan.
func (p *P2P) localStorageForCollection(ctx context.Context, collectionID string) (negentropy.Storage, error) {
	shortID, err := p.collectionShortID(ctx, collectionID)
	if err != nil {
		return nil, err
	}

	iter, err := p.db.Multistore().ReconcileIndex().Iterator(ctx, corekv.IterOptions{
		Prefix:   keys.ReconcileIndexScopePrefix(shortID),
		KeysOnly: true,
	})
	if err != nil {
		return nil, err
	}

	builder := negentropy.NewSegmentTreeBuilder(0)
	for {
		hasNext, err := iter.Next()
		if err != nil {
			return nil, errors.Join(err, iter.Close())
		}
		if !hasNext {
			break
		}
		height, cidBytes, ok := keys.ReconcileIndexEntry(iter.Key())
		if !ok {
			continue
		}
		// Copy: the iterator may reuse the key buffer across Next calls.
		builder.Add(height, append([]byte(nil), cidBytes...))
	}
	if err := iter.Close(); err != nil {
		return nil, err
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

// ReconcileCollection runs a range-based set reconciliation session against peerID
// over the full set of composite block CIDs in a collection, then converges the
// local node by fetching the blocks it is missing and merging them. This is the
// O(diff) cold-start/catch-up path: the missing block set is discovered up front and
// fetched directly, so no full-DAG walk is required.
//
// It is pull-only: the local (initiating) node converges; the peer converges when it
// runs its own ReconcileCollection. Reconciliation only discovers the divergent CIDs
// — causal validation stays with the existing merge.
func (p *P2P) ReconcileCollection(
	ctx context.Context,
	peerID string,
	collectionName string,
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

	// The manual API converges both peers in one call: pull need + push have.
	return p.reconcileCollectionScope(ctx, peerID, cols[0].Version().CollectionID, true)
}

// reconcileCollectionScope runs a collection-scope reconciliation session against
// peerID, then pulls the blocks it is missing. When pushHave is true it also pushes
// the blocks the peer is missing, so a single call converges both peers. The
// on-connect auto-trigger uses pushHave=false because both peers pull on connect.
func (p *P2P) reconcileCollectionScope(
	ctx context.Context,
	peerID, collectionID string,
	pushHave bool,
) error {
	if p.reconcileProtocol == nil {
		return ErrSetReconciliationDisabled
	}

	sessionCtx, cancel := context.WithTimeout(ctx, reconcileSessionTimeout)
	defer cancel()

	local, err := p.localStorageForCollection(sessionCtx, collectionID)
	if err != nil {
		return err
	}

	scope := protocol.ReconcileScope{Kind: protocol.ScopeCollectionBlocks, ID: collectionID}
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
			return it.Err()
		}
		if done {
			break
		}
		msg = next
	}

	if err := p.fetchAndMergeCollectionNeed(sessionCtx, peerID, collectionID, it.Need()); err != nil {
		return err
	}
	if pushHave {
		return p.pushCollectionHave(sessionCtx, peerID, collectionID, it.Have())
	}
	return nil
}

// fetchedComposite captures the metadata of a fetched composite block needed to
// compute merge tips and issue per-document merges.
type fetchedComposite struct {
	cid   cid.Cid
	docID string
}

// fetchAndMergeCollectionNeed fetches each missing composite block directly by CID
// (no DAG walk), persists it, then merges from the "tips" of the fetched set — the
// blocks not referenced as a parent by any other fetched block. In a Merkle-CRDT,
// lacking a parent implies lacking all its descendants, so the need-set's tips are
// exactly the peer's new heads; merging from them walks the now-local blocks
// parent-first via the existing merge.
func (p *P2P) fetchAndMergeCollectionNeed(
	ctx context.Context,
	peerID, collectionID string,
	need [][]byte,
) error {
	if len(need) == 0 {
		return nil
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sessionCtx = p.host.ContextWithSession(sessionCtx)
	linkSys := makeLinkSystem(p.host.IPLDStore())

	blocks := make([]fetchedComposite, 0, len(need))
	referenced := make(map[string]struct{}) // composite CIDs that are some block's parent

	for _, idBytes := range need {
		c, err := cid.Cast(idBytes)
		if err != nil {
			return err
		}

		nd, err := linkSys.Load(linking.LinkContext{Ctx: sessionCtx}, cidlink.Link{Cid: c}, coreblock.BlockSchemaPrototype)
		if err != nil {
			return err
		}
		block, err := coreblock.GetFromNode(nd)
		if err != nil {
			return err
		}
		if block.Signature != nil {
			if _, err := coreblock.VerifyBlockSignature(block, &linkSys); err != nil {
				return NewErrVerifyBlockSig(err)
			}
		}
		// Persist the fetched block so the subsequent merge walks it locally.
		if _, err := linkSys.Store(linking.LinkContext{Ctx: sessionCtx}, coreblock.GetLinkPrototype(), block.GenerateNode()); err != nil {
			return NewErrStoreBlockDAGSync(err)
		}

		for _, h := range block.Heads {
			parentCID := h.Cid
			referenced[string(parentCID.Bytes())] = struct{}{}
		}
		blocks = append(blocks, fetchedComposite{cid: c, docID: string(block.Delta.GetDocID())})
	}

	for _, b := range blocks {
		if _, isParent := referenced[string(b.cid.Bytes())]; isParent {
			continue // not a tip — it will be reached by the merge walk from a tip
		}
		// syncDocumentAndMerge walks the tip's DAG (all links, including field
		// blocks) via the network link system, fetching what is missing and
		// stopping at already-merged blocks, then merges. The composite blocks are
		// already local from the pre-fetch above, so only the field blocks for the
		// divergent sub-DAG are fetched here — keeping it diff-proportional.
		if err := p.syncDocumentAndMerge(sessionCtx, peerID, collectionID, b.docID, b.cid); err != nil {
			return err
		}
	}
	return nil
}

// pushCollectionHave pushes the composite blocks the peer is missing (the initiator's
// have-set), so a single collection reconciliation converges both peers. Only the
// tips of the have sub-DAG are pushed; the peer's pushlog handler walks each tip's
// DAG to pull and merge the rest. The blocks are already local on the initiator.
func (p *P2P) pushCollectionHave(
	ctx context.Context,
	peerID, collectionID string,
	have [][]byte,
) error {
	if len(have) == 0 {
		return nil
	}

	linkSys := makeLinkSystem(p.host.IPLDStore())

	type haveBlock struct {
		cid   cid.Cid
		docID string
		raw   []byte
	}
	blocks := make([]haveBlock, 0, len(have))
	referenced := make(map[string]struct{})

	for _, idBytes := range have {
		c, err := cid.Cast(idBytes)
		if err != nil {
			return err
		}
		nd, err := linkSys.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, coreblock.BlockSchemaPrototype)
		if err != nil {
			return err
		}
		block, err := coreblock.GetFromNode(nd)
		if err != nil {
			return err
		}
		raw, err := block.Marshal()
		if err != nil {
			return NewErrMarshalBlock(err, string(block.Delta.GetDocID()), c.String())
		}

		for _, h := range block.Heads {
			parentCID := h.Cid
			referenced[string(parentCID.Bytes())] = struct{}{}
		}
		blocks = append(blocks, haveBlock{cid: c, docID: string(block.Delta.GetDocID()), raw: raw})
	}

	for _, b := range blocks {
		if _, isParent := referenced[string(b.cid.Bytes())]; isParent {
			continue // not a tip — the peer pulls it via the merge walk from a tip
		}
		roundCtx, cancel := context.WithTimeout(ctx, networkRequestTimeout)
		pushReq := protocol.PushLogRequest{
			DocID:        b.docID,
			CID:          b.cid.Bytes(),
			CollectionID: collectionID,
			Creator:      p.host.ID(),
			Block:        b.raw,
		}
		_, err := p.replicatorProtocol.SendRequest(roundCtx, pushReq, peerID)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
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
