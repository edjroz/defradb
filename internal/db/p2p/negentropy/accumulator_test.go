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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcc256AddCarryPropagation(t *testing.T) {
	var a acc256
	a[31] = 0xFF
	var one [32]byte
	one[31] = 0x01

	a.addInto(&one)

	require.Equal(t, byte(0x00), a[31], "low byte wraps to 0x00")
	require.Equal(t, byte(0x01), a[30], "carry propagates into the next byte")
}

func TestAcc256OverflowWrapsModulo2_256(t *testing.T) {
	var a acc256
	for i := range a {
		a[i] = 0xFF // a == 2^256 - 1
	}
	var one [32]byte
	one[31] = 0x01

	a.addInto(&one)

	require.Equal(t, acc256{}, a, "(2^256-1) + 1 wraps to 0 modulo 2^256")
}

func TestAcc256AddCommutative(t *testing.T) {
	var x, y [32]byte
	for i := range x {
		x[i] = byte(i * 7)
		y[i] = byte(255 - i*3)
	}

	var ax, ay acc256
	ax.addInto(&x)
	ax.addInto(&y)
	ay.addInto(&y)
	ay.addInto(&x)

	require.Equal(t, ax, ay, "modular addition is commutative")
}

func TestAcc256AddAccMatchesAddInto(t *testing.T) {
	var x [32]byte
	for i := range x {
		x[i] = byte(i + 1)
	}

	var viaInto acc256
	viaInto.addInto(&x)

	var src acc256
	src.addInto(&x)
	var viaAcc acc256
	viaAcc.addAcc(&src)

	require.Equal(t, viaInto, viaAcc, "addAcc matches addInto for the same value")
}
