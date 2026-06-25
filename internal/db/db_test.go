// Copyright 2022 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package db

import (
	"context"
	"testing"

	badgerds "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/corekv/badger"

	acpDB "github.com/sourcenetwork/defradb/internal/db/acp"
	intOpts "github.com/sourcenetwork/defradb/internal/options"
)

func newBadgerDB(ctx context.Context) (*DB, error) {
	rootstore, err := badger.NewDatastore("", badgerds.DefaultOptions("").WithInMemory(true))
	if err != nil {
		return nil, err
	}

	adminInfo, err := acpDB.NewNACInfo(ctx, "", false)
	if err != nil {
		return nil, err
	}
	return newDB(ctx, rootstore, adminInfo)
}

func TestNewDB(t *testing.T) {
	ctx := context.Background()
	rootstore, err := badger.NewDatastore("", badgerds.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)

	adminInfo, err := acpDB.NewNACInfo(ctx, "", false)
	require.NoError(t, err)

	_, err = NewDB(ctx, rootstore, adminInfo)
	require.NoError(t, err)
}

// TestNewDB_SetReconciliationEnabled verifies the EnableSetReconciliation DB option
// threads from NodeDBOptions through to the DB.P2PSetReconciliationEnabled() method
// that internal/db/p2p reads when deciding whether to register the reconcile protocol.
func TestNewDB_SetReconciliationEnabled(t *testing.T) {
	ctx := context.Background()

	// Defaults to false.
	db, err := newBadgerDB(ctx)
	require.NoError(t, err)
	require.False(t, db.P2PSetReconciliationEnabled())

	// When set via the DB options it is reported by the method.
	rootstore, err := badger.NewDatastore("", badgerds.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)
	adminInfo, err := acpDB.NewNACInfo(ctx, "", false)
	require.NoError(t, err)

	cfg := defaultDBConfig().NodeDBOptions
	cfg.EnableSetReconciliation = true
	enabledDB, err := newDB(ctx, rootstore, adminInfo, intOpts.DB().SetNodeDBOptions(cfg))
	require.NoError(t, err)
	require.True(t, enabledDB.P2PSetReconciliationEnabled())
}
