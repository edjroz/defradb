// Copyright 2026 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:build !js

package node

import (
	"testing"

	"github.com/sourcenetwork/go-p2p"
	"github.com/stretchr/testify/assert"

	"github.com/sourcenetwork/defradb/client/options"
)

func TestBuildP2POpts_WifiAware(t *testing.T) {
	opts := options.NodeP2POptions{EnableWifiAware: true}

	applied := p2p.DefaultOptions()
	for _, opt := range buildP2POpts(&opts) {
		opt(applied)
	}

	assert.True(t, applied.EnableWifiAware)
}

func TestBuildP2POpts_WifiAwareDisabledByDefault(t *testing.T) {
	opts := options.NodeP2POptions{}

	applied := p2p.DefaultOptions()
	for _, opt := range buildP2POpts(&opts) {
		opt(applied)
	}

	assert.False(t, applied.EnableWifiAware)
}
