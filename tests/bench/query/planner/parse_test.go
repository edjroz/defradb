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

package planner

import (
	"context"
	"fmt"
	"testing"

	gql "github.com/sourcenetwork/graphql-go"

	"github.com/sourcenetwork/defradb/internal/core"
	"github.com/sourcenetwork/defradb/tests/bench/fixtures"
)

// Every DefraDB request is parsed *and* re-validated from scratch: there is no
// AST cache and no validated-document cache anywhere on the read path
// (internal/request/graphql/parser.go BuildRequestAST + Parse). Validation
// walks the whole document with graphql-go's visitor, which profiling has
// shown to be a large share of read-path allocations.
//
// The three benchmarks below split that per-request cost into its stages so
// the share attributable to validation can be measured rather than guessed:
//
//	BuildRequestAST   lex + parse the request string into an ast.Document
//	ValidateDocument  gql.ValidateDocument(schema, ast, nil)
//	Parse             ValidateDocument + lowering into a request.Request
//
// Parse is a superset of ValidateDocument, so:
//
//	validation share = ValidateDocument / (BuildRequestAST + Parse)
//
// A cache would have to be keyed on more than the request string. Parse
// re-resolves the schema per transaction (parser.go looks up a per-txn schema
// manager before validating), so a correct cache key must include the schema
// version in force for the request - a document validated against one schema
// version is not necessarily valid against the next.

// parseQueries covers a spread of request shapes, because parse and validation
// costs scale with different things: parse with the size of the request text,
// validation with the size of the selection set and the number of type and
// argument lookups it forces.
var parseQueries = []struct {
	name  string
	query string
}{
	{
		// Smallest useful request: one type, one field.
		name: "flat",
		query: `query {
			User {
				Name
			}
		}`,
	},
	{
		// Every field on the fixture type.
		name: "manyFields",
		query: `query {
			User {
				_docID
				Name
				Age
				Points
				Verified
			}
		}`,
	},
	{
		// Arguments force validation to resolve input object types and coerce
		// literals, which parsing does not do at all.
		name: "filtered",
		query: `query {
			User(
				filter: {Age: {_gt: 30}, Verified: {_eq: true}, Points: {_geq: 10.0}},
				order: {Age: DESC},
				limit: 10,
				offset: 5
			) {
				Name
				Age
				Points
			}
		}`,
	},
	{
		// Aliases multiply the selection set without multiplying the schema,
		// isolating per-selection validation cost.
		name: "aliased",
		query: `query {
			young: User(filter: {Age: {_lt: 30}}) {
				Name
				Age
			}
			old: User(filter: {Age: {_geq: 30}}) {
				Name
				Age
			}
			verified: User(filter: {Verified: {_eq: true}}) {
				Name
				Points
			}
		}`,
	},
	{
		// Nested selection sets via grouping and commit metadata. These are the
		// deepest documents a request on a relation-free collection can produce.
		name: "nested",
		query: `query {
			User(groupBy: [Verified]) {
				Verified
				GROUP {
					Name
					Age
					_version {
						cid
						height
					}
				}
			}
		}`,
	},
}

// runParseStageBench runs fn once per iteration against a parser and schema
// built from the `user_simple` fixture.
//
// The whole document is built and validated once before the timer starts, so a
// request that silently fails to validate can never be reported as a fast one.
func runParseStageBench(
	b *testing.B,
	query string,
	fn func(b *testing.B, parserAndSchema parseHarness, query string),
) {
	ctx := context.Background()
	fixture := fixtures.ForCollection(ctx, "user_simple")

	parser, schema, err := buildParser(ctx, fixture)
	if err != nil {
		b.Fatal(err)
	}
	harness := parseHarness{ctx: ctx, parser: parser, schema: schema}

	astDoc, err := parser.BuildRequestAST(ctx, query)
	if err != nil {
		b.Fatal(err)
	}
	if _, errs := parseRequest(schema, astDoc); errs != nil {
		b.Fatalf("query failed to parse: %v", errs)
	}

	b.ReportAllocs()
	b.ResetTimer()
	fn(b, harness, query)
}

// parseHarness bundles the per-benchmark setup so the stage functions stay
// single-purpose.
type parseHarness struct {
	ctx    context.Context
	parser core.Parser
	schema *gql.Schema
}

func benchmarkParseStages(b *testing.B, stage func(b *testing.B, h parseHarness, query string)) {
	for _, q := range parseQueries {
		b.Run(fmt.Sprintf("query=%s", q.name), func(b *testing.B) {
			runParseStageBench(b, q.query, stage)
		})
	}
}

// Benchmark_Planner_UserSimple_BuildRequestAST measures only lexing and
// parsing the request string into an AST. No schema is consulted.
func Benchmark_Planner_UserSimple_BuildRequestAST(b *testing.B) {
	benchmarkParseStages(b, func(b *testing.B, h parseHarness, query string) {
		for i := 0; i < b.N; i++ {
			if _, err := h.parser.BuildRequestAST(h.ctx, query); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark_Planner_UserSimple_ValidateDocument measures only
// gql.ValidateDocument against an AST that has already been built. This is the
// graphql-go visitor walk that every request pays for, every time, because
// nothing caches the validated document.
func Benchmark_Planner_UserSimple_ValidateDocument(b *testing.B) {
	benchmarkParseStages(b, func(b *testing.B, h parseHarness, query string) {
		astDoc, err := h.parser.BuildRequestAST(h.ctx, query)
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			result := gql.ValidateDocument(h.schema, astDoc, nil)
			if !result.IsValid {
				b.Fatalf("document failed validation: %v", result.Errors)
			}
		}
	})
}

// Benchmark_Planner_UserSimple_Parse measures validation plus lowering the AST
// into a request.Request - everything (*parser).Parse does after the AST
// exists. The difference against ValidateDocument is the lowering cost.
func Benchmark_Planner_UserSimple_Parse(b *testing.B) {
	benchmarkParseStages(b, func(b *testing.B, h parseHarness, query string) {
		astDoc, err := h.parser.BuildRequestAST(h.ctx, query)
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, errs := parseRequest(h.schema, astDoc); errs != nil {
				b.Fatalf("failed to parse query: %v", errs)
			}
		}
	})
}
