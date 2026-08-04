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
	"fmt"
	"testing"

	ipld "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/bindnode"

	"github.com/sourcenetwork/defradb/internal/core/crdt"
)

// The block shapes benched here are chosen to span the range seen in practice:
//   - heads: 0 (create), 1 (linear update), 5 (heavily diverged history)
//   - links: 0 (field block), 5 (small document composite), 20 (wide document composite)
//   - value: 16 bytes (a scalar) and 1 KiB (a small blob or long string)
var (
	benchHeadCounts  = []int{0, 1, 5}
	benchLinkCounts  = []int{0, 5, 20}
	benchValueSizes  = []int{16, 1024}
	benchValueLabels = map[int]string{16: "16B", 1024: "1KiB"}
)

// benchLink returns a deterministic, distinct link for use as a head or DAG link.
func benchLink(b *testing.B, seed int) cidlink.Link {
	b.Helper()

	block := &Block{
		Delta: crdt.CRDT{
			LWWDelta: &crdt.LWWDelta{
				FieldName:           "seed",
				Priority:            uint64(seed),
				CollectionVersionID: "bafkbenchcollectionversion",
				Data:                []byte(fmt.Sprintf("seed-%d", seed)),
			},
		},
	}

	link, err := block.GenerateLink()
	if err != nil {
		b.Fatal(err)
	}
	return link
}

// newBenchBlock builds an LWW field block of the given shape.
func newBenchBlock(b *testing.B, headCount int, linkCount int, valueSize int) *Block {
	b.Helper()

	var heads []cidlink.Link
	for i := 0; i < headCount; i++ {
		heads = append(heads, benchLink(b, i))
	}

	var links []DAGLink
	for i := 0; i < linkCount; i++ {
		links = append(links, DAGLink{
			Name: fmt.Sprintf("field_%d", i),
			Link: benchLink(b, 1_000_000+i),
		})
	}

	return &Block{
		Delta: crdt.CRDT{
			LWWDelta: &crdt.LWWDelta{
				FieldName:           "name",
				Priority:            1,
				CollectionVersionID: "bafkbenchcollectionversion",
				Data:                bytes.Repeat([]byte("a"), valueSize),
			},
		},
		Heads: heads,
		Links: links,
	}
}

// forEachBlockShape runs fn once per point of the head/link/value-size matrix.
//
// fn is responsible for any per-shape setup it needs and for calling [testing.B.ResetTimer]
// once that setup is done.
func forEachBlockShape(b *testing.B, fn func(b *testing.B, block *Block)) {
	for _, headCount := range benchHeadCounts {
		for _, linkCount := range benchLinkCounts {
			for _, valueSize := range benchValueSizes {
				name := fmt.Sprintf(
					"heads=%d/links=%d/value=%s",
					headCount,
					linkCount,
					benchValueLabels[valueSize],
				)

				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					block := newBenchBlock(b, headCount, linkCount, valueSize)
					b.ResetTimer()

					fn(b, block)
				})
			}
		}
	}
}

func Benchmark_Block_Marshal_LWW(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		for i := 0; i < b.N; i++ {
			if _, err := block.Marshal(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Block_Unmarshal_LWW(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		b.StopTimer()
		encoded, err := block.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			var decoded Block
			if err := decoded.Unmarshal(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Block_GetFromBytes_LWW(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		b.StopTimer()
		encoded, err := block.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			if _, err := GetFromBytes(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func Benchmark_Block_GenerateNode_LWW(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		for i := 0; i < b.N; i++ {
			_ = block.GenerateNode()
		}
	})
}

func Benchmark_Block_GenerateLink_LWW(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		for i := 0; i < b.N; i++ {
			if _, err := block.GenerateLink(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// The benchmarks below isolate the cost of the schema compatibility check that
// [bindnode.Wrap] performs on every single call.
//
// `bindnode.Wrap(ptrVal, schemaType)` unconditionally calls the (unexported)
// `verifyCompatibility`, which allocates a fresh `map[seenEntry]bool` and then
// recursively reflect-walks the Go type against the schema type. See
// go-ipld-prime/node/bindnode/api.go (`Wrap`) - the package's own TODO there notes that
// this could be skipped when the caller already went through `Prototype`.
//
// DefraDB already validates once, at package init: `mustSetSchema` calls
// `bindnode.Prototype` and panics if [Block] and the schema disagree. Every subsequent
// `Wrap` re-derives an answer that cannot have changed.
//
// The delta between Benchmark_Bindnode_Wrap_PerCall and Benchmark_Bindnode_Prototype_Reuse
// is therefore the per-call cost that is removable by caching, and it is paid by every
// [Block.Marshal], [Block.GenerateNode] and [Block.GenerateLink] - and twice by every
// [Block.Unmarshal], because `ipld.Unmarshal` calls `bindnode.Prototype` and then
// `bindnode.Wrap` again to re-bind the result.

// Benchmark_Bindnode_Wrap_PerCall measures the current path: schema validation on every call.
func Benchmark_Bindnode_Wrap_PerCall(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		for i := 0; i < b.N; i++ {
			_ = bindnode.Wrap(block, BlockSchema).Representation()
		}
	})
}

// Benchmark_Bindnode_Prototype_Reuse measures the same work with the validation hoisted
// out of the loop, as it already is in [mustSetSchema].
func Benchmark_Bindnode_Prototype_Reuse(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		b.StopTimer()
		// The validating call, made once - this is what package init already does.
		proto := bindnode.Prototype(&Block{}, BlockSchema)
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			_ = proto.Representation()
		}
	})
}

// Benchmark_Block_Decode_WrapPerCall is the current decode path, via [GetFromBytes].
//
// It pays for schema validation twice: `ipld.Unmarshal` builds a `bindnode.Prototype`
// (validate #1), decodes into it, reflect-copies the result back out, and then calls
// `bindnode.Wrap` (validate #2) to re-bind.
func Benchmark_Block_Decode_WrapPerCall(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		b.StopTimer()
		encoded, err := block.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			if _, err := GetFromBytes(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Benchmark_Block_Decode_PrototypeReuse decodes through [BlockSchemaPrototype], which is
// built once at package init, and unwraps the result with [GetFromNode].
//
// This performs no per-call schema validation and no reflect copy-back, and is reachable
// through the existing public API - it is the same shape as the `lsys.Load` +
// [GetFromNode] path already used elsewhere in the codebase.
func Benchmark_Block_Decode_PrototypeReuse(b *testing.B) {
	forEachBlockShape(b, func(b *testing.B, block *Block) {
		b.StopTimer()
		encoded, err := block.Marshal()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		for i := 0; i < b.N; i++ {
			node, err := ipld.DecodeUsingPrototype(encoded, dagcbor.Decode, BlockSchemaPrototype)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := GetFromNode(node); err != nil {
				b.Fatal(err)
			}
		}
	})
}
