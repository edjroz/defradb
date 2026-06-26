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

package action

import (
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sourcenetwork/immutable"
)

// defaultWaitForStateTimeout bounds how long WaitForState polls before failing.
const (
	defaultWaitForStateTimeout = 20 * time.Second
	waitForStatePollInterval   = 200 * time.Millisecond
)

// WaitForState polls a node with a GQL request until the result satisfies Check, or
// fails the test after Timeout. It is the eventual-consistency assertion for
// asynchronous flows (e.g. on-connect auto-reconciliation) that the head-tracking
// WaitForSync cannot express — it simply re-queries until the desired state is seen.
type WaitForState struct {
	stateful

	// NodeID is the node to poll.
	NodeID int

	// Request is the GQL query to execute each poll.
	Request string

	// Check reports whether the polled GQL result data is the desired state.
	Check func(data any) bool

	// Timeout bounds polling. Zero uses defaultWaitForStateTimeout.
	Timeout immutable.Option[time.Duration]
}

var _ Action = (*WaitForState)(nil)
var _ Stateful = (*WaitForState)(nil)

// Execute polls until Check passes or the timeout elapses.
func (a *WaitForState) Execute() {
	timeout := defaultWaitForStateTimeout
	if a.Timeout.HasValue() {
		timeout = a.Timeout.Value()
	}

	node := a.s.Nodes[a.NodeID]
	deadline := time.Now().Add(timeout)

	var lastData any
	for {
		result := node.ExecRequest(a.s.Ctx, a.Request)
		if len(result.GQL.Errors) == 0 {
			lastData = result.GQL.Data
			if a.Check(lastData) {
				return
			}
		}
		if time.Now().After(deadline) {
			require.Failf(a.s.T, "WaitForState timed out",
				"node %d did not reach the expected state within %s; last data: %v",
				a.NodeID, timeout, lastData)
			return
		}
		time.Sleep(waitForStatePollInterval)
	}
}
