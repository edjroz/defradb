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
	"github.com/sourcenetwork/defradb/errors"
	"github.com/sourcenetwork/defradb/internal/db/p2p/negentropy"
)

var (
	// ErrBadFingerprintLen indicates a reconcile range carried a fingerprint of
	// the wrong length (it must be exactly negentropy.FingerprintLen bytes).
	ErrBadFingerprintLen = errors.New("reconcile fingerprint has wrong length")
)

// NewErrBadFingerprintLen reports a fingerprint of length got that is not the
// required negentropy.FingerprintLen.
func NewErrBadFingerprintLen(got int) error {
	return errors.WithStack(
		ErrBadFingerprintLen,
		errors.NewKV("Got", got),
		errors.NewKV("Want", negentropy.FingerprintLen),
	)
}
