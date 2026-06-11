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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakeStartCommand_HasEnableWifiAwareFlag(t *testing.T) {
	cmd := MakeStartCommand(context.Background())
	flag := cmd.PersistentFlags().Lookup("enable-wifi-aware")
	require.NotNil(t, flag, "start command should define --enable-wifi-aware")
	assert.Equal(t, "false", flag.DefValue)
}
