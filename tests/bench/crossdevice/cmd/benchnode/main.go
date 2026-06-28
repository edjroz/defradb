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

// Command benchnode is a single DefraDB node, instrumented for the cross-device
// reconciliation benchmark and driven over a thin HTTP control API.
//
// It builds the node exactly the way the in-process sync harness does
// (tests/bench/sync/harness.go: startSyncNode) — identity, in-memory store,
// pubsub, and the CountingHost decorator that records per-protocol wire traffic —
// then exposes seed / connect / reconcile / sync / counters / blockstats over
// HTTP so an external orchestrator (run.py) can wire any number of these into an
// arbitrary topology across machines. Because it reuses the same CountingHost, the
// control-byte metric it reports is identical in shape to the in-process bench,
// but now sourced from separate devices over a real network.
//
// A stock `defradb` binary exposes none of this (no per-protocol counters, no
// bandwidth reporter, no /metrics endpoint), which is why the cross-device
// benchmark ships its own node rather than driving the shipping CLI. See
// tests/bench/crossdevice/README.md.
//
// This is a benchmark tool, not a product binary: it has no auth, binds wherever
// it is told, and should only be run on trusted benchmark hosts.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/sourcenetwork/defradb/acp/identity"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
	"github.com/sourcenetwork/defradb/crypto"
	"github.com/sourcenetwork/defradb/internal/datastore"
	"github.com/sourcenetwork/defradb/node"
	benchsync "github.com/sourcenetwork/defradb/tests/bench/sync"
)

// syncTimeout bounds a background SyncDocuments call. SyncDocuments does not return
// promptly on completion (it waits out its deadline), so /sync fires it in the
// background and the orchestrator detects convergence by polling /blockstats.
const syncTimeout = 30 * time.Second

func main() {
	control := flag.String("control", "127.0.0.1:7000", "address for the HTTP control API")
	p2pAddr := flag.String("p2p", "/ip4/0.0.0.0/tcp/0", "libp2p listen multiaddress")
	reconciliation := flag.Bool("reconciliation", false, "enable range-based set reconciliation")
	store := flag.String("store", "memory", "store backend: 'memory' or 'badger=<path>'")
	flag.Parse()

	ctx := context.Background()
	srv, err := newServer(ctx, *p2pAddr, *reconciliation, *store)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchnode: start failed: %v\n", err)
		os.Exit(1)
	}
	defer srv.close(ctx)

	mux := http.NewServeMux()
	srv.routes(mux)

	// Announce readiness on stdout so the orchestrator can wait for it (and learn
	// the chosen port when binding :0 — though run.py assigns fixed ports).
	fmt.Printf("benchnode ready control=%s reconciliation=%v\n", *control, *reconciliation)
	if err := http.ListenAndServe(*control, mux); err != nil { //nolint:gosec // benchmark tool, trusted hosts
		fmt.Fprintf(os.Stderr, "benchnode: control server exited: %v\n", err)
		os.Exit(1)
	}
}

// server holds the running node and the instrumentation behind the control API.
type server struct {
	node     *node.Node
	db       node.DB
	p2p      client.P2P
	counters *benchsync.Counters

	mu   sync.Mutex
	cols map[string]client.Collection // collection cache by name
}

// newServer builds and starts an instrumented node, mirroring startSyncNode.
func newServer(ctx context.Context, p2pAddr string, reconciliation bool, store string) (*server, error) {
	counters := benchsync.NewCounters()

	b := options.Node().
		SetDisableAPI(true). // we serve our own control API; no defradb HTTP API
		SetEnableDevelopment(true)
	if reconciliation {
		b.DB().SetEnableSetReconciliation(true)
	}

	// Identity: DocumentACP is on by default, so block serving authenticates the
	// requesting peer; without an identity every block forces a fresh identity
	// round-trip (see harness.go).
	ident, err := identity.Generate(crypto.KeyTypeEd25519)
	if err != nil {
		return nil, fmt.Errorf("generate identity: %w", err)
	}
	b.DB().SetNodeIdentity(ident)

	switch {
	case store == "memory":
		b.Store().SetType(options.NodeMemoryStore)
	case strings.HasPrefix(store, "badger="):
		b.Store().SetType(options.NodeBadgerStore).SetPath(strings.TrimPrefix(store, "badger="))
	default:
		return nil, fmt.Errorf("invalid --store %q (want 'memory' or 'badger=<path>')", store)
	}

	b.P2P().
		SetListenAddresses(p2pAddr).
		SetEnablePubSub(true).
		SetHostDecorator(benchsync.Decorator(counters))

	n, err := node.New(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("node.New: %w", err)
	}
	if err := n.Start(ctx); err != nil {
		return nil, fmt.Errorf("node.Start: %w", err)
	}
	p2p, ok := n.DB.(client.P2P)
	if !ok {
		_ = n.Close(ctx)
		return nil, fmt.Errorf("node DB does not implement client.P2P")
	}

	return &server{node: n, db: n.DB, p2p: p2p, counters: counters, cols: map[string]client.Collection{}}, nil
}

func (s *server) close(ctx context.Context) { _ = s.node.Close(ctx) }

func (s *server) routes(mux *http.ServeMux) {
	mux.HandleFunc("/peerinfo", s.handlePeerInfo)
	mux.HandleFunc("/schema", s.handleSchema)
	mux.HandleFunc("/seed", s.handleSeed)
	mux.HandleFunc("/update", s.handleUpdate)
	mux.HandleFunc("/connect", s.handleConnect)
	mux.HandleFunc("/reconcile", s.handleReconcile)
	mux.HandleFunc("/sync", s.handleSync)
	mux.HandleFunc("/counters", s.handleCounters)
	mux.HandleFunc("/counters/reset", s.handleCountersReset)
	mux.HandleFunc("/blockstats", s.handleBlockstats)
	mux.HandleFunc("/query", s.handleQuery)
}

// --- handlers -------------------------------------------------------------

func (s *server) handlePeerInfo(w http.ResponseWriter, r *http.Request) {
	addrs, err := s.p2p.PeerInfo(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	if len(addrs) == 0 {
		writeErr(w, fmt.Errorf("no peer addresses"))
		return
	}
	info, err := peer.AddrInfoFromString(addrs[0])
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"peerID": info.ID.String(), "addrs": addrs})
}

func (s *server) handleSchema(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SDL string `json:"sdl"`
	}
	if !decode(w, r, &req) {
		return
	}
	if _, err := s.db.AddCollection(r.Context(), req.SDL); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *server) handleSeed(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Collection string `json:"collection"`
		Prefix     string `json:"prefix"`
		Count      int    `json:"count"`
		Updates    int    `json:"updates"`
	}
	if !decode(w, r, &req) {
		return
	}
	col, err := s.collection(r.Context(), req.Collection)
	if err != nil {
		writeErr(w, err)
		return
	}
	ctx := r.Context()
	docIDs := make([]string, 0, req.Count)
	for i := 0; i < req.Count; i++ {
		name := fmt.Sprintf("%s%d", req.Prefix, i)
		doc, err := client.NewDocFromJSON(ctx, []byte(fmt.Sprintf(`{"name":%q,"value":0}`, name)), col.Version())
		if err != nil {
			writeErr(w, err)
			return
		}
		if err := col.AddDocument(ctx, doc); err != nil {
			writeErr(w, err)
			return
		}
		for u := 1; u <= req.Updates; u++ {
			if err := s.updateValue(ctx, col, doc.ID(), u); err != nil {
				writeErr(w, err)
				return
			}
		}
		docIDs = append(docIDs, doc.ID().String())
	}
	writeJSON(w, map[string]any{"docIDs": docIDs})
}

func (s *server) handleUpdate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Collection string `json:"collection"`
		DocID      string `json:"docID"`
		Value      int    `json:"value"`
	}
	if !decode(w, r, &req) {
		return
	}
	col, err := s.collection(r.Context(), req.Collection)
	if err != nil {
		writeErr(w, err)
		return
	}
	docID, err := client.NewDocIDFromString(req.DocID)
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := s.updateValue(r.Context(), col, docID, req.Value); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *server) handleConnect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Addrs []string `json:"addrs"`
	}
	if !decode(w, r, &req) {
		return
	}
	if err := s.p2p.Connect(r.Context(), req.Addrs); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (s *server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PeerID     string `json:"peerID"`
		Collection string `json:"collection"`
	}
	if !decode(w, r, &req) {
		return
	}
	start := time.Now()
	err := s.p2p.ReconcileCollection(r.Context(), req.PeerID, req.Collection)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, map[string]any{"wallMs": float64(time.Since(start).Microseconds()) / 1000})
}

// handleSync fires a SyncDocuments in the background (it does not return on
// completion) and returns immediately; the orchestrator polls /blockstats for
// convergence. The orchestrator supplies the docID list because the default path
// cannot discover which docIDs exist — that asymmetry is exactly the cost the
// reconcile path removes.
func (s *server) handleSync(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Collection string   `json:"collection"`
		DocIDs     []string `json:"docIDs"`
	}
	if !decode(w, r, &req) {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), syncTimeout)
		defer cancel()
		_ = s.p2p.SyncDocuments(ctx, req.Collection, req.DocIDs)
	}()
	writeJSON(w, map[string]any{"accepted": true})
}

func (s *server) handleCounters(w http.ResponseWriter, r *http.Request) {
	snap := s.counters.Snapshot()
	protos := make(map[string]any, len(snap.Protos))
	for k, pc := range snap.Protos {
		protos[k] = map[string]any{
			"bytesSent": pc.BytesSent, "bytesRecv": pc.BytesRecv,
			"msgsSent": pc.MsgsSent, "msgsRecv": pc.MsgsRecv,
		}
	}
	writeJSON(w, map[string]any{
		"totalBytesSent": snap.TotalBytesSent,
		"totalBytesRecv": snap.TotalBytesRecv,
		"totalMsgsSent":  snap.TotalMsgsSent,
		"totalMsgsRecv":  snap.TotalMsgsRecv,
		"protos":         protos,
	})
}

func (s *server) handleCountersReset(w http.ResponseWriter, r *http.Request) {
	s.counters.Reset()
	writeJSON(w, map[string]any{"ok": true})
}

// handleBlockstats walks the node's blockstore and reports the block count and
// total payload bytes. After full convergence every node holds the identical block
// set, so equal (count,bytes) across all nodes is the orchestrator's identity-free
// convergence + correctness signal.
func (s *server) handleBlockstats(w http.ResponseWriter, r *http.Request) {
	bs := datastore.BlockstoreFrom(s.db.Rootstore(), s.node.Options().DB.ChunkSize)
	ch, err := bs.AllKeysChan(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	var count int
	var bytes int64
	for c := range ch {
		count++
		blk, err := bs.Get(r.Context(), c)
		if err != nil {
			continue
		}
		bytes += int64(len(blk.RawData()))
	}
	writeJSON(w, map[string]any{"blocks": count, "blockBytes": bytes})
}

// handleQuery runs a GraphQL read request against the node's DB and returns its
// data. The orchestrator queries every node with the same request after
// convergence and asserts the results are identical — a document-level state
// check that is strictly stronger than the block-count equality of
// /blockstats (same blocks present does not by itself prove the same documents
// resolve). It is read-only; the node identity set at startup flows via the
// request context for any ACP checks.
func (s *server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}
	if !decode(w, r, &req) {
		return
	}
	res := s.db.ExecRequest(r.Context(), req.Query)
	if len(res.GQL.Errors) > 0 {
		writeErr(w, res.GQL.Errors[0])
		return
	}
	writeJSON(w, map[string]any{"data": res.GQL.Data})
}

// --- helpers --------------------------------------------------------------

func (s *server) collection(ctx context.Context, name string) (client.Collection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if col, ok := s.cols[name]; ok {
		return col, nil
	}
	col, err := s.db.GetCollectionByName(ctx, name)
	if err != nil {
		return nil, err
	}
	s.cols[name] = col
	return col, nil
}

func (s *server) updateValue(ctx context.Context, col client.Collection, docID client.DocID, value int) error {
	doc, err := col.GetDocument(ctx, docID, options.GetDocument())
	if err != nil {
		return err
	}
	if err := doc.SetWithJSON(ctx, []byte(fmt.Sprintf(`{"value":%d}`, value))); err != nil {
		return err
	}
	return col.SaveDocument(ctx, doc)
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, fmt.Errorf("decode request: %w", err))
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
}
