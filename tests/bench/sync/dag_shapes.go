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

package sync

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/client/options"
)

const collectionName = "Item"

const schemaSDL = `
type Item {
	name: String
	value: Int
}
`

// createDoc adds a fresh document (one composite commit) and returns it.
func createDoc(ctx context.Context, tb testing.TB, col client.Collection, name string) *client.Document {
	tb.Helper()
	doc, err := client.NewDocFromJSON(ctx, []byte(fmt.Sprintf(`{"name":%q,"value":0}`, name)), col.Version())
	require.NoError(tb, err)
	require.NoError(tb, col.AddDocument(ctx, doc))
	return doc
}

// updateDoc applies a single update, producing one new commit (DAG block).
func updateDoc(ctx context.Context, tb testing.TB, col client.Collection, docID client.DocID, value int) {
	tb.Helper()
	doc, err := col.GetDocument(ctx, docID, options.GetDocument())
	require.NoError(tb, err)
	require.NoError(tb, doc.SetWithJSON(ctx, []byte(fmt.Sprintf(`{"value":%d}`, value))))
	require.NoError(tb, col.SaveDocument(ctx, doc))
}

// seedDocs creates docCount documents on col, each followed by updatesPerDoc
// updates. It returns the docID strings. Total commits across the collection are
// approximately docCount*(1+updatesPerDoc), giving a deterministic DAG size.
func seedDocs(ctx context.Context, tb testing.TB, col client.Collection, docCount, updatesPerDoc int) []string {
	tb.Helper()
	ids := make([]string, 0, docCount)
	for d := 0; d < docCount; d++ {
		doc := createDoc(ctx, tb, col, fmt.Sprintf("doc-%d", d))
		for u := 1; u <= updatesPerDoc; u++ {
			updateDoc(ctx, tb, col, doc.ID(), u)
		}
		ids = append(ids, doc.ID().String())
	}
	return ids
}

// applyTail extends the DAGs of the given documents by `extra` further commits
// each, creating an incremental divergence of known size on top of a shared base.
func applyTail(ctx context.Context, tb testing.TB, col client.Collection, docIDs []string, extra int) {
	tb.Helper()
	for _, idStr := range docIDs {
		docID, err := client.NewDocIDFromString(idStr)
		require.NoError(tb, err)
		for u := 0; u < extra; u++ {
			updateDoc(ctx, tb, col, docID, 1000+u)
		}
	}
}
