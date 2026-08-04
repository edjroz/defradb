// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coreblock

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/sourcenetwork/corekv/memory"
	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/internal/core"
	"github.com/sourcenetwork/defradb/internal/core/crdt"
	"github.com/sourcenetwork/defradb/internal/datastore"
	"github.com/sourcenetwork/defradb/internal/db/lock"
	"github.com/sourcenetwork/defradb/internal/keys"
)

const benchCollectionVersionID = "bafkbenchcollectionversion"

const benchCollectionShortID = 1

var benchFieldCounts = []int{2, 8, 32}

// benchFieldNames is pre-computed so that name formatting is not counted against the
// benchmarks below.
var benchFieldNames = func() []string {
	names := make([]string, 32)
	for i := range names {
		names[i] = fmt.Sprintf("field_%d", i)
	}
	return names
}()

// newBenchTxn returns a context carrying a transaction over a fresh in-memory rootstore.
//
// Note that the transaction is never committed, so its write-set grows for the duration of
// the benchmark - the memory cost of a very long run is therefore not flat.
func newBenchTxn(b *testing.B) (context.Context, datastore.Txn) {
	b.Helper()

	ctx := context.Background()
	txn := datastore.NewTxnFrom(memory.NewDatastore(ctx), lock.NewLockSet(), 1, false, immutable.None[int]())

	return datastore.CtxSetTxn(ctx, txn), txn
}

func benchFieldKey(docShortID uint64, fieldIndex int) keys.DataStoreKey {
	return keys.DataStoreKey{
		CollectionShortID: benchCollectionShortID,
		DocShortID:        docShortID,
		FieldID:           strconv.Itoa(fieldIndex + 1),
	}
}

func benchDocKey(docShortID uint64) keys.DataStoreKey {
	return keys.DataStoreKey{
		CollectionShortID: benchCollectionShortID,
		DocShortID:        docShortID,
		FieldID:           core.COMPOSITE_NAMESPACE,
	}
}

// benchDocShortID maps an iteration index onto a document short ID.
//
// Short IDs are one-based - a zero doc short ID is omitted entirely by
// [keys.HeadstoreDocKey.Bytes], which makes the resulting head key unparseable.
func benchDocShortID(i int) uint64 {
	return uint64(i) + 1
}

// benchAddFieldDelta runs the full single-field write pipeline: read the current heads, build
// and store the block, merge the delta, and update the heads.
func benchAddFieldDelta(
	b *testing.B,
	ctx context.Context,
	txn datastore.Txn,
	docShortID uint64,
	fieldIndex int,
	value []byte,
) DAGLink {
	b.Helper()

	fieldName := benchFieldNames[fieldIndex]

	lww := crdt.NewLWW(
		txn.Datastore(),
		benchCollectionVersionID,
		benchFieldKey(docShortID, fieldIndex),
		fieldName,
	)

	link, _, err := AddDelta(ctx, lww, &crdt.LWWDelta{
		FieldName:           fieldName,
		CollectionVersionID: benchCollectionVersionID,
		Data:                value,
	})
	if err != nil {
		b.Fatal(err)
	}

	return NewDAGLink(fieldName, link)
}

// benchWriteDocument writes one document: a field block per field, followed by the composite
// block that links them.  This mirrors what the document write path does per mutation.
func benchWriteDocument(
	b *testing.B,
	ctx context.Context,
	txn datastore.Txn,
	docShortID uint64,
	fieldCount int,
	value []byte,
) {
	b.Helper()

	links := make([]DAGLink, 0, fieldCount)
	for fieldIndex := 0; fieldIndex < fieldCount; fieldIndex++ {
		links = append(links, benchAddFieldDelta(b, ctx, txn, docShortID, fieldIndex, value))
	}

	composite := crdt.NewDocComposite(txn.Datastore(), benchCollectionVersionID, benchDocKey(docShortID))
	if _, _, err := AddDelta(ctx, composite, composite.Delta(), links...); err != nil {
		b.Fatal(err)
	}
}

// Benchmark_AddDelta_Document measures the whole block write pipeline for a document -
// [AddDelta] per field plus the composite - across field count and value size.
//
// The create sub-benchmarks write a fresh document each iteration, so every block has no
// heads.  The update sub-benchmarks rewrite the same document, so every block has exactly one
// head per CRDT; note that the presence of a head also makes `determineBlockEncryption` read
// and decode the previous block, even when encryption is disabled.
func Benchmark_AddDelta_Document(b *testing.B) {
	for _, fieldCount := range benchFieldCounts {
		for _, valueSize := range benchValueSizes {
			for _, isUpdate := range []bool{false, true} {
				operation := "create"
				if isUpdate {
					operation = "update"
				}

				name := fmt.Sprintf(
					"fields=%d/value=%s/%s",
					fieldCount,
					benchValueLabels[valueSize],
					operation,
				)

				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()

					ctx, txn := newBenchTxn(b)
					value := bytes.Repeat([]byte("a"), valueSize)

					if isUpdate {
						// Seed the document so that every timed iteration is an update.
						benchWriteDocument(b, ctx, txn, benchDocShortID(0), fieldCount, value)
					}

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						docShortID := benchDocShortID(0)
						if !isUpdate {
							docShortID = benchDocShortID(i)
						}
						benchWriteDocument(b, ctx, txn, docShortID, fieldCount, value)
					}
				})
			}
		}
	}
}

// Benchmark_AddDelta_Field isolates a single field-level [AddDelta], without the composite
// block, so that the per-field cost can be separated from the per-document cost.
func Benchmark_AddDelta_Field(b *testing.B) {
	for _, valueSize := range benchValueSizes {
		for _, isUpdate := range []bool{false, true} {
			operation := "create"
			if isUpdate {
				operation = "update"
			}

			b.Run(fmt.Sprintf("value=%s/%s", benchValueLabels[valueSize], operation), func(b *testing.B) {
				b.ReportAllocs()

				ctx, txn := newBenchTxn(b)
				value := bytes.Repeat([]byte("a"), valueSize)

				if isUpdate {
					benchAddFieldDelta(b, ctx, txn, benchDocShortID(0), 0, value)
				}

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					docShortID := benchDocShortID(0)
					if !isUpdate {
						docShortID = benchDocShortID(i)
					}
					benchAddFieldDelta(b, ctx, txn, docShortID, 0, value)
				}
			})
		}
	}
}

// Benchmark_ProcessBlock measures the state-update half of the write pipeline in isolation:
// the CRDT merge plus the head update, without block encoding or blockstore writes.
func Benchmark_ProcessBlock(b *testing.B) {
	for _, valueSize := range benchValueSizes {
		b.Run(fmt.Sprintf("value=%s", benchValueLabels[valueSize]), func(b *testing.B) {
			b.ReportAllocs()

			ctx, txn := newBenchTxn(b)
			value := bytes.Repeat([]byte("a"), valueSize)

			lww := crdt.NewLWW(
				txn.Datastore(),
				benchCollectionVersionID,
				benchFieldKey(benchDocShortID(0), 0),
				benchFieldNames[0],
			)

			block := New(crdt.NewCRDT(&crdt.LWWDelta{
				FieldName:           benchFieldNames[0],
				Priority:            1,
				CollectionVersionID: benchCollectionVersionID,
				Data:                value,
			}), nil)

			link, err := block.GenerateLink()
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := ProcessBlock(ctx, lww, block, link); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
