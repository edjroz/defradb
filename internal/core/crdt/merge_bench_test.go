// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package crdt

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/fxamacker/cbor/v2"

	"github.com/sourcenetwork/corekv/memory"
	"github.com/sourcenetwork/immutable"

	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/internal/datastore"
	"github.com/sourcenetwork/defradb/internal/db/base"
	"github.com/sourcenetwork/defradb/internal/db/lock"
	"github.com/sourcenetwork/defradb/internal/keys"
)

const benchCollectionVersionID = "bafkbenchcollectionversion"

var benchValueSizes = []int{16, 1024}

var benchValueLabels = map[int]string{16: "16B", 1024: "1KiB"}

// newBenchStore returns an in-memory datastore and a context carrying a transaction.
//
// The transaction is only required so that the datastore's collection lock-set has something
// to key locks against - the store itself writes straight through to the memory rootstore, so
// that a long benchmark loop does not accumulate an unbounded transaction write-set.
func newBenchStore(b *testing.B) (context.Context, datastore.Keyedstore) {
	b.Helper()

	ctx := context.Background()
	rootstore := memory.NewDatastore(ctx)
	lockSet := lock.NewLockSet()

	multistore := datastore.NewMultistore(rootstore, lockSet, immutable.None[int]())
	ctx = datastore.CtxSetTxn(ctx, datastore.NewTxnFrom(rootstore, lockSet, 1, false, immutable.None[int]()))

	return ctx, multistore.Datastore()
}

func benchDataStoreKey() keys.DataStoreKey {
	return keys.DataStoreKey{
		CollectionShortID: 1,
		DocShortID:        1,
		FieldID:           "1",
	}
}

// The three benchmarks below cover the three branches of [LWW.setValue]:
//
//   - priority_winner: the incoming priority is strictly greater than the stored one, so the
//     value is written and the priority is bumped. This is the common path for a linear history.
//   - priority_loser:  the incoming priority is lower than the stored one, so the merge is a
//     no-op after reading the priority and the delete marker. This is the common path when
//     re-receiving already-superseded updates over P2P.
//   - priority_tie:    the priorities are equal, so the merge falls through to the deterministic
//     value tie-break, which costs an extra read of the current value plus a byte comparison.
//     This is the path taken by concurrent writes at the same height.

func Benchmark_LWW_Merge_PriorityWinner(b *testing.B) {
	for _, valueSize := range benchValueSizes {
		b.Run(fmt.Sprintf("value=%s", benchValueLabels[valueSize]), func(b *testing.B) {
			b.ReportAllocs()

			ctx, store := newBenchStore(b)
			lww := NewLWW(store, benchCollectionVersionID, benchDataStoreKey(), "name")
			delta := &LWWDelta{
				FieldName:           "name",
				CollectionVersionID: benchCollectionVersionID,
				Data:                bytes.Repeat([]byte("a"), valueSize),
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// A strictly increasing priority keeps every iteration on the clear-winner path.
				delta.Priority = uint64(i) + 1
				if err := lww.Merge(ctx, delta); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Benchmark_LWW_Merge_PriorityLoser(b *testing.B) {
	for _, valueSize := range benchValueSizes {
		b.Run(fmt.Sprintf("value=%s", benchValueLabels[valueSize]), func(b *testing.B) {
			b.ReportAllocs()

			ctx, store := newBenchStore(b)
			key := benchDataStoreKey()
			lww := NewLWW(store, benchCollectionVersionID, key, "name")

			// Seed a high stored priority so that every merged delta loses.
			if err := setPriority(ctx, store, key, 1_000_000); err != nil {
				b.Fatal(err)
			}

			delta := &LWWDelta{
				FieldName:           "name",
				Priority:            1,
				CollectionVersionID: benchCollectionVersionID,
				Data:                bytes.Repeat([]byte("a"), valueSize),
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := lww.Merge(ctx, delta); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Benchmark_LWW_Merge_PriorityTie(b *testing.B) {
	for _, valueSize := range benchValueSizes {
		b.Run(fmt.Sprintf("value=%s", benchValueLabels[valueSize]), func(b *testing.B) {
			b.ReportAllocs()

			ctx, store := newBenchStore(b)
			key := benchDataStoreKey()
			lww := NewLWW(store, benchCollectionVersionID, key, "name")

			value := bytes.Repeat([]byte("a"), valueSize)

			// Seed the store with the same priority and the same value, so that every
			// iteration takes the tie-break branch and leaves the state unchanged.
			const priority = 10
			if err := setPriority(ctx, store, key, priority); err != nil {
				b.Fatal(err)
			}
			if err := store.Set(ctx, key.WithValueFlag(), value); err != nil {
				b.Fatal(err)
			}

			delta := &LWWDelta{
				FieldName:           "name",
				Priority:            priority,
				CollectionVersionID: benchCollectionVersionID,
				Data:                value,
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := lww.Merge(ctx, delta); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Counter merges are commutative, so unlike LWW there is no priority-dependent branching -
// every merge reads the current value, decodes it, adds, re-encodes and writes.
// The interesting axis is therefore the numeric type, which selects a different
// instantiation of the generic [validateAndIncrement].
func Benchmark_Counter_Merge(b *testing.B) {
	testCases := []struct {
		name  string
		kind  client.ScalarKind
		value any
	}{
		{name: "int64", kind: client.FieldKind_NILLABLE_INT, value: int64(1)},
		{name: "float64", kind: client.FieldKind_NILLABLE_FLOAT64, value: float64(1)},
	}

	for _, testCase := range testCases {
		for _, allowDecrement := range []bool{false, true} {
			cType := "P_COUNTER"
			if allowDecrement {
				cType = "PN_COUNTER"
			}

			b.Run(fmt.Sprintf("%s/%s", testCase.name, cType), func(b *testing.B) {
				b.ReportAllocs()

				ctx, store := newBenchStore(b)
				counter := NewCounter(
					store,
					benchCollectionVersionID,
					benchDataStoreKey(),
					"points",
					allowDecrement,
					testCase.kind,
				)

				encoded, err := cbor.Marshal(testCase.value)
				if err != nil {
					b.Fatal(err)
				}

				delta := &CounterDelta{
					FieldName:           "points",
					CollectionVersionID: benchCollectionVersionID,
					Data:                encoded,
				}

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					delta.Priority = uint64(i) + 1
					if err := counter.Merge(ctx, delta); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// Benchmark_DocComposite_Merge_Create merges a composite delta for a document that does not
// yet have an object marker - the create path, which writes both the version key and the marker.
func Benchmark_DocComposite_Merge_Create(b *testing.B) {
	b.ReportAllocs()

	ctx, store := newBenchStore(b)
	composite := NewDocComposite(store, benchCollectionVersionID, benchDataStoreKey())
	delta := &DocCompositeDelta{
		Priority:            1,
		CollectionVersionID: benchCollectionVersionID,
		Status:              client.Active,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Point at a fresh document each iteration so that the object marker is always absent.
		composite.key.DocShortID = uint64(i)
		if err := composite.Merge(ctx, delta); err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_DocComposite_Merge_Update merges a composite delta for a document that already
// has an object marker - the update path, which only rewrites the version key.
func Benchmark_DocComposite_Merge_Update(b *testing.B) {
	b.ReportAllocs()

	ctx, store := newBenchStore(b)
	key := benchDataStoreKey()
	composite := NewDocComposite(store, benchCollectionVersionID, key)

	if err := store.Set(ctx, key.ToPrimaryDataStoreKey(), []byte{base.ObjectMarker}); err != nil {
		b.Fatal(err)
	}

	delta := &DocCompositeDelta{
		Priority:            1,
		CollectionVersionID: benchCollectionVersionID,
		Status:              client.Active,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := composite.Merge(ctx, delta); err != nil {
			b.Fatal(err)
		}
	}
}
