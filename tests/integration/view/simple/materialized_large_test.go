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

// This file guards against regression of issue #4386 / PR #4889: refreshing a
// materialized view whose cache rebuild exceeds the storage engine's ~11 MB
// transaction size limit must succeed. Coverage requested by #4890.
//
// largeDocCount × largeValuePadLen ≈ 12.5 MB of view-selected field data so the
// rebuilt view cache cannot fit in a single transaction, while each value stays
// below typical per-key size caps. The name is unique per doc and derived from
// the doc index so filter spot-checks are deterministic.

package simple

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/tests/action"
	"github.com/sourcenetwork/defradb/tests/gen"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"
)

const (
	largeViewDocCount    = 250
	largeViewValuePadLen = 50 * 1024
)

func largeViewNameValue(i int) string {
	return fmt.Sprintf("%04d", i) + strings.Repeat("a", largeViewValuePadLen)
}

// TestView_SimpleMaterialized_RefreshOverLargeDatasetSucceeds creates a
// materialized view, bulk-loads ~12.5 MB of source docs, explicitly refreshes the
// view (truncate + rebuild of the cache), and verifies correct results via two
// filtered spot-checks without harness auto-refresh.
func TestView_SimpleMaterialized_RefreshOverLargeDatasetSucceeds(t *testing.T) {
	spotA := largeViewNameValue(42)
	spotB := largeViewNameValue(0)
	reqA := fmt.Sprintf(
		`query { UserView(filter: {name: {_eq: %q}}) { name age } }`,
		spotA,
	)
	reqB := fmt.Sprintf(
		`query { UserView(filter: {name: {_eq: %q}}) { name age } }`,
		spotB,
	)

	test := testUtils.TestCase{
		SupportedViewTypes: immutable.Some([]testUtils.ViewType{
			testUtils.MaterializedViewType,
		}),
		Actions: []any{
			&action.AddCollection{
				SDL: `
					type User {
						name: String
						age:  Int
					}
				`,
			},
			// Create the view before bulk insert so CollectionNames includes UserView
			// (GenerateDocs panics on nil slots if the view is only declared later).
			// AddView auto-refreshes an empty cache first.
			&action.AddView{
				Query: `
					User {
						name
						age
					}
				`,
				SDL: `
					type UserView {
						name: String
						age:  Int
					}
				`,
			},
			// Each doc's name is a unique ~50 KB value selected by the view, so the
			// materialized cache rebuild exceeds the ~11 MB transaction class.
			testUtils.GenerateDocs{
				ForCollections: []string{"User"},
				Options: []gen.Option{
					gen.WithTypeDemand("User", largeViewDocCount),
					gen.WithFieldGenerator("User", "name", func(i int, _ func() any) any {
						return largeViewNameValue(i)
					}),
					gen.WithFieldGenerator("User", "age", func(i int, _ func() any) any {
						return i
					}),
				},
			},
			// Explicit large refresh: truncate (empty/small) + rebuild past the ~11 MB
			// transaction class (the #4889 / #4386 failure mode).
			&action.RefreshViews{},
			// Second refresh: truncate + rebuild of an already-large cache.
			&action.RefreshViews{},
			&action.Request{
				DoNotRefreshViews: true,
				Request:           reqA,
				Results: map[string]any{
					"UserView": []map[string]any{
						{
							"name": spotA,
							"age":  int64(42),
						},
					},
				},
			},
			&action.Request{
				DoNotRefreshViews: true,
				Request:           reqB,
				Results: map[string]any{
					"UserView": []map[string]any{
						{
							"name": spotB,
							"age":  int64(0),
						},
					},
				},
			},
		},
	}

	testUtils.ExecuteTestCase(t, test)
}
