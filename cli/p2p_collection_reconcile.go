// Copyright 2026 Democratized Data Foundation
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

func MakeP2PCollectionReconcileCommand(ctx context.Context) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "reconcile <peerID> <collection-name>",
		Short: "Reconcile a collection's block set with a peer (experimental)",
		Long: `Reconcile a collection's full composite block set with a peer using range-based
set reconciliation.

This runs the experimental, default-off set-reconciliation protocol against a single
peer to converge a collection: the missing blocks are discovered up front and fetched
directly, giving O(diff) cold-start. It returns an error if set reconciliation is not
enabled on the node.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			peerID := args[0]
			collectionName := args[1]

			ctx := cmd.Context()
			if timeout, _ := cmd.Flags().GetDuration("timeout"); timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			cliClient := mustGetContextCLIClient(cmd)
			opt := options.WithIdentity(options.ReconcileCollection(), iIdentity.FromContext(cmd.Context()))
			return cliClient.ReconcileCollection(ctx, peerID, collectionName, opt)
		},
	}

	EmbedCLIExample(ctx, cmd, "reconcile a collection with a peer",
		`defradb client p2p collection reconcile 12D3Koo... Users`)

	cmd.Flags().Duration("timeout", 0, "Timeout for the reconciliation session")
	return cmd
}
