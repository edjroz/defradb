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
	"github.com/sourcenetwork/graphql-go/language/ast"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/request"
	"github.com/sourcenetwork/defradb/errors"
	"github.com/sourcenetwork/defradb/internal/core"
	"github.com/sourcenetwork/defradb/internal/request/graphql"
	defrap "github.com/sourcenetwork/defradb/internal/request/graphql/parser"
	"github.com/sourcenetwork/defradb/internal/request/graphql/schema"
	benchutils "github.com/sourcenetwork/defradb/tests/bench"
	"github.com/sourcenetwork/defradb/tests/bench/fixtures"
)

func runQueryParserBench(
	b *testing.B,
	ctx context.Context,
	fixture fixtures.Generator,
	query string,
) error {
	parser, schema, err := buildParser(ctx, fixture)
	if err != nil {
		return err
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ast, _ := parser.BuildRequestAST(ctx, query)
		_, errs := parseRequest(schema, ast)
		if errs != nil {
			return errors.Wrap("failed to parse query string", errors.New(fmt.Sprintf("%v", errs)))
		}
	}
	b.StopTimer()

	return nil
}

// parseRequest mirrors (*parser).Parse for a parser that has no active
// transaction: validate the document against the schema, then lower it into a
// request.Request.
//
// It is spelled out here rather than calling (*parser).Parse because these
// benchmarks build a schema without a database, and so have no transaction to
// hang a schema manager off - see buildParser.
func parseRequest(schema *gql.Schema, astDoc *ast.Document) (*request.Request, []error) {
	validationResult := gql.ValidateDocument(schema, astDoc, nil)
	if !validationResult.IsValid {
		errs := make([]error, len(validationResult.Errors))
		for i, err := range validationResult.Errors {
			errs[i] = err
		}
		return nil, errs
	}

	return defrap.ParseRequest(*schema, astDoc, &client.GQLOptions{})
}

// buildParser returns a parser and the GraphQL schema generated from the given
// fixture, without standing up a database.
//
// The schema is generated directly off a schema.SchemaManager rather than via
// (*parser).SetSchema because SetSchema requires a transaction on the context
// (it defers installing the new schema manager to the transaction's OnSuccess
// hook), and there is no transaction to give it here. Generating through the
// manager runs the exact same generator, so the resulting schema is the one a
// live parser would validate against.
func buildParser(
	ctx context.Context,
	fixture fixtures.Generator,
) (core.Parser, *gql.Schema, error) {
	sdl, err := benchutils.ConstructSDL(fixture)
	if err != nil {
		return nil, nil, err
	}

	parser, err := graphql.NewParser(false)
	if err != nil {
		return nil, nil, err
	}

	schemaManager, err := schema.NewSchemaManager(false)
	if err != nil {
		return nil, nil, err
	}

	collectionVersions, err := schemaManager.ParseSDL(sdl)
	if err != nil {
		return nil, nil, err
	}

	collections := make([]client.CollectionVersion, len(collectionVersions))
	for i, collectionVersion := range collectionVersions {
		collections[i] = collectionVersion.Definition
	}

	if _, err := schemaManager.Generator.Generate(ctx, collections); err != nil {
		return nil, nil, err
	}

	return parser, schemaManager.Schema(), nil
}
