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

// Package api contains benchmarks that isolate the cost of the API layer
// (HTTP transport, JSON codec, routing and middleware) from the cost of
// actually executing a request against the database.
//
// Every benchmark in this file runs the *same* GraphQL request against the
// *same* database over a different call path, so the delta between them is
// attributable purely to the layers being added:
//
//	Direct      db.ExecRequest                       (execution only)
//	Handler     chi mux -> middleware -> ExecRequest  (+ routing, JSON, middleware, auth)
//	Server      loopback socket -> the above          (+ TCP, net/http server & client)
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sourcenetwork/defradb/client"
	defrahttp "github.com/sourcenetwork/defradb/http"
	"github.com/sourcenetwork/defradb/node"
	benchutils "github.com/sourcenetwork/defradb/tests/bench"
	"github.com/sourcenetwork/defradb/tests/bench/fixtures"
)

// userSimpleQuery is a small read of every field on the `user_simple` fixture.
//
// It is deliberately cheap to execute: this suite measures per-request
// overhead, not scan cost, so execution should not dominate the delta.
const userSimpleQuery = `query {
	User {
		_docID
		Name
		Age
		Points
		Verified
	}
}`

// graphQLEndpoint is the versioned GraphQL route bound in
// (*storeHandler).bindRoutes.
const graphQLEndpoint = "/api/" + defrahttp.Version + "/graphql"

// benchFixture holds a database backfilled with docCount documents and the
// pieces needed to drive it over each of the three call paths.
type benchFixture struct {
	db      node.DB
	handler http.Handler
	server  *httptest.Server
	body    []byte
}

// setupFixture builds a memory-backed database with the `user_simple` fixture,
// backfills it with docCount documents, and wraps it in both an in-process
// http.Handler and a loopback httptest server.
func setupFixture(b *testing.B, ctx context.Context, docCount int) *benchFixture {
	b.Helper()

	fixture := fixtures.ForCollection(ctx, "user_simple")

	db, collections, err := benchutils.SetupDBAndCollections(b, ctx, fixture)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(db.Close)

	if _, err := benchutils.BackfillBenchmarkDB(
		b,
		ctx,
		collections,
		fixture,
		docCount,
		0,
		false,
	); err != nil {
		b.Fatal(err)
	}

	handler, err := defrahttp.NewHandler(db, nil)
	if err != nil {
		b.Fatal(err)
	}
	handlerWithCtx := defrahttp.InjectServerContext(ctx)(handler)

	server := httptest.NewServer(handlerWithCtx)
	b.Cleanup(server.Close)

	body, err := json.Marshal(defrahttp.GraphQLRequest{Query: userSimpleQuery})
	if err != nil {
		b.Fatal(err)
	}

	return &benchFixture{
		db:      db,
		handler: handlerWithCtx,
		server:  server,
		body:    body,
	}
}

// newGraphQLRequest builds a fresh POST request for the GraphQL endpoint.
//
// A new request is built per iteration because the body reader is consumed by
// the handler; the cost of doing so is part of what a real client pays.
func (f *benchFixture) newGraphQLRequest(ctx context.Context) *http.Request {
	req := httptest.NewRequest(http.MethodPost, graphQLEndpoint, bytes.NewReader(f.body))
	req.Header.Set("Content-Type", "application/json")
	return req.WithContext(ctx)
}

// assertGQLResult fails the benchmark if the request did not return data, so
// that a silently failing request can never be reported as a fast one.
func assertGQLResult(b *testing.B, result *client.RequestResult) {
	b.Helper()
	if len(result.GQL.Errors) > 0 {
		b.Fatalf("request returned errors: %v", result.GQL.Errors)
	}
	if result.GQL.Data == nil {
		b.Fatal("request returned no data")
	}
}

// assertHTTPResult fails the benchmark unless the response is a 200 carrying a
// GraphQL payload with no errors.
func assertHTTPResult(b *testing.B, statusCode int, payload []byte) {
	b.Helper()
	if statusCode != http.StatusOK {
		b.Fatalf("unexpected status %d: %s", statusCode, payload)
	}
	var result client.GQLResult
	if err := json.Unmarshal(payload, &result); err != nil {
		b.Fatalf("failed to decode response %s: %v", payload, err)
	}
	if len(result.Errors) > 0 {
		b.Fatalf("request returned errors: %v", result.Errors)
	}
	if result.Data == nil {
		b.Fatal("request returned no data")
	}
}

// benchmarkDirect measures db.ExecRequest with no API layer at all. This is
// the baseline every other path in this file is compared against.
func benchmarkDirect(b *testing.B, docCount int) {
	ctx := context.Background()
	f := setupFixture(b, ctx, docCount)

	assertGQLResult(b, f.db.ExecRequest(ctx, userSimpleQuery))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := f.db.ExecRequest(ctx, userSimpleQuery)
		if result.GQL.Data == nil {
			b.Fatal("request returned no data")
		}
	}
}

// benchmarkHandler measures the full chi mux: routing, the API/transaction/auth
// middleware chain, JSON request decoding and JSON response encoding - but
// without a socket. The delta against benchmarkDirect is the in-process cost of
// the API layer.
func benchmarkHandler(b *testing.B, docCount int) {
	ctx := context.Background()
	f := setupFixture(b, ctx, docCount)

	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, f.newGraphQLRequest(ctx))
	assertHTTPResult(b, rec.Code, rec.Body.Bytes())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		f.handler.ServeHTTP(rec, f.newGraphQLRequest(ctx))
		if rec.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", rec.Code)
		}
	}
}

// benchmarkServer measures a real request over the loopback interface using
// net/http on both ends. The delta against benchmarkHandler is the cost of the
// socket plus the net/http server and client machinery.
func benchmarkServer(b *testing.B, docCount int) {
	ctx := context.Background()
	f := setupFixture(b, ctx, docCount)

	exec := func() (int, []byte) {
		res, err := f.server.Client().Post(
			f.server.URL+graphQLEndpoint,
			"application/json",
			bytes.NewReader(f.body),
		)
		if err != nil {
			b.Fatal(err)
		}
		defer res.Body.Close()
		payload, err := io.ReadAll(res.Body)
		if err != nil {
			b.Fatal(err)
		}
		return res.StatusCode, payload
	}

	statusCode, payload := exec()
	assertHTTPResult(b, statusCode, payload)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if statusCode, _ := exec(); statusCode != http.StatusOK {
			b.Fatalf("unexpected status %d", statusCode)
		}
	}
}

// docCounts are kept small on purpose. The point of this suite is per-request
// overhead; a large collection would let scan cost swamp the transport delta.
var docCounts = []int{10, 100}

func Benchmark_API_Transport_Direct(b *testing.B) {
	for _, docCount := range docCounts {
		b.Run(fmt.Sprintf("docs=%d", docCount), func(b *testing.B) {
			benchmarkDirect(b, docCount)
		})
	}
}

func Benchmark_API_Transport_HTTPHandler(b *testing.B) {
	for _, docCount := range docCounts {
		b.Run(fmt.Sprintf("docs=%d", docCount), func(b *testing.B) {
			benchmarkHandler(b, docCount)
		})
	}
}

func Benchmark_API_Transport_HTTPServer(b *testing.B) {
	for _, docCount := range docCounts {
		b.Run(fmt.Sprintf("docs=%d", docCount), func(b *testing.B) {
			benchmarkServer(b, docCount)
		})
	}
}
