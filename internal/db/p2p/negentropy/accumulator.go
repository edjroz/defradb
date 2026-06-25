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

// acc256 is a 256-bit unsigned integer stored big-endian (most-significant byte
// first) supporting addition modulo 2^256 via byte-wise add-with-carry. It is
// used to sum per-item SHA-256 digests when fingerprinting a range, without
// pulling in math/big.
//
// Addition mod 2^256 is associative and commutative, which is what makes range
// fingerprints combinable: the sum over A∪B equals the sum over A plus the sum
// over B (carry out of the top byte is discarded — the defining wraparound).
type acc256 [32]byte

// addInto adds the 32-byte big-endian value src into dst in place, modulo 2^256.
// It iterates from the least-significant (last) byte upward, propagating the
// carry; any carry out of the most-significant byte is discarded (wraparound).
func (dst *acc256) addInto(src *[32]byte) {
	var carry uint16
	for i := len(dst) - 1; i >= 0; i-- {
		sum := uint16(dst[i]) + uint16(src[i]) + carry
		dst[i] = byte(sum)
		carry = sum >> 8
	}
}

// addAcc adds another accumulator's value into dst, modulo 2^256.
func (dst *acc256) addAcc(src *acc256) {
	dst.addInto((*[32]byte)(src))
}
