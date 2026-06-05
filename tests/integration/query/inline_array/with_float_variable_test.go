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

package inline_array

import (
	"testing"

	"github.com/sourcenetwork/defradb/tests/action"
	testUtils "github.com/sourcenetwork/defradb/tests/integration"

	"github.com/sourcenetwork/immutable"
)

func TestQueryInlineFloatArray_WithNillableFloatListVariable(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddDoc{
				Doc: `{
					"name": "John",
					"pageRatings": [3.5, 4.0, 4.8]
				}`,
			},
			&action.AddDoc{
				Doc: `{
					"name": "Bob",
					"pageRatings": [1.0, 2.5]
				}`,
			},
			&action.Request{
				Variables: immutable.Some(map[string]any{
					"ratings": []any{float64(3.5), float64(4.0), float64(4.8)},
				}),
				Request: `query($ratings: [Float]) {
					Users(filter: {pageRatings: {_eq: $ratings}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "John",
						},
					},
				},
			},
		},
	}

	executeTestCase(t, test)
}

func TestQueryInlineFloatArray_WithNonNullFloatListVariable(t *testing.T) {
	test := testUtils.TestCase{
		Actions: []any{
			&action.AddDoc{
				Doc: `{
					"name": "John",
					"favouriteFloats": [3.5, 4.0, 4.8]
				}`,
			},
			&action.AddDoc{
				Doc: `{
					"name": "Bob",
					"favouriteFloats": [1.0, 2.5]
				}`,
			},
			&action.Request{
				Variables: immutable.Some(map[string]any{
					"ratings": []any{float64(3.5), float64(4.0), float64(4.8)},
				}),
				Request: `query($ratings: [Float!]) {
					Users(filter: {favouriteFloats: {_eq: $ratings}}) {
						name
					}
				}`,
				Results: map[string]any{
					"Users": []map[string]any{
						{
							"name": "John",
						},
					},
				},
			},
		},
	}

	executeTestCase(t, test)
}
