// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package negentropy

import (
	"github.com/sourcenetwork/defradb/errors"
)

const (
	errDuplicateItem    string = "duplicate item sort key in reconciliation vector"
	errRoundCapExceeded string = "reconciliation exceeded the maximum number of rounds"
	errMalformedBound   string = "reconcile range upper bound precedes its lower bound"
)

// NewErrDuplicateItem indicates that a [VectorBuilder] was given two items with
// the same sort key, violating the set (duplicate-free) semantics the
// fingerprint relies on.
func NewErrDuplicateItem(sortKey []byte) error {
	return errors.New(errDuplicateItem, errors.NewKV("SortKey", sortKey))
}

// NewErrRoundCapExceeded indicates a session hit [MaxRounds] without converging,
// typically because the peer returned fingerprints that never agree. The
// initiator terminates and exposes any partial have/need it learned.
func NewErrRoundCapExceeded(rounds int) error {
	return errors.New(errRoundCapExceeded, errors.NewKV("Rounds", rounds))
}

// NewErrMalformedBound indicates an incoming range whose upper bound precedes its
// lower bound, so it cannot map to a valid index window.
func NewErrMalformedBound(lower, upper []byte) error {
	return errors.New(errMalformedBound, errors.NewKV("Lower", lower), errors.NewKV("Upper", upper))
}
