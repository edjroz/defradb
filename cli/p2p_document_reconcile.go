// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/sourcenetwork/defradb/client/options"
	iIdentity "github.com/sourcenetwork/defradb/internal/identity"
)

func MakeP2PDocumentReconcileCommand(ctx context.Context) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "reconcile <peerID> <collection-name> <docID>",
		Short: "Reconcile a document's heads with a peer (experimental)",
		Long: `Reconcile a document's heads with a peer using range-based set reconciliation.

This runs the experimental, default-off set-reconciliation protocol against a single
peer to converge a document's heads, pulling the heads this node is missing and
pushing the heads the peer is missing. It returns an error if set reconciliation is
not enabled on the node.`,
		Args: cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			peerID := args[0]
			collectionName := args[1]
			docID := args[2]

			ctx := cmd.Context()
			if timeout, _ := cmd.Flags().GetDuration("timeout"); timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			cliClient := mustGetContextCLIClient(cmd)
			opt := options.WithIdentity(options.ReconcileDocument(), iIdentity.FromContext(cmd.Context()))
			return cliClient.ReconcileDocument(ctx, peerID, collectionName, docID, opt)
		},
	}

	EmbedCLIExample(ctx, cmd, "reconcile a document with a peer",
		`defradb client p2p document reconcile 12D3Koo... Users bae123`)

	cmd.Flags().Duration("timeout", 0, "Timeout for the reconciliation session")
	return cmd
}
