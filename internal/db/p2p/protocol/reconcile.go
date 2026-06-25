// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protocol

import (
	"github.com/sourcenetwork/defradb/internal/db/p2p/message"
)

// ReconcileRequest is the message exchanged during range-based set reconciliation
// (Negentropy). It carries range fingerprints so two peers can discover their
// divergent subset without transferring the full DAG.
//
// Phase 1 ships a placeholder shape so the protocol can be registered behind the
// EnableSetReconciliation flag; the scope/range/fingerprint fields are added in the
// reconciler phases.
type ReconcileRequest struct {
	message.MetaData
}

// ReconcileReply is the response to a [ReconcileRequest].
type ReconcileReply struct {
	message.MetaData
}
