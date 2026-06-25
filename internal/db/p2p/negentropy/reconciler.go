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

// This file implements the range-based set reconciliation state machine.
//
// Roles. A session is driven by one Initiator against a stateless responder
// ([Respond]). The initiator opens with a single full-range fingerprint and then,
// each round, maps every incoming range to exactly one outgoing range — Skip when
// it agrees, an echoed Fingerprint when it disagrees, Skip after it has consumed
// an IdList — so the initiator never changes the range tiling. Only the responder
// splits (a mismatching fingerprint range into BranchingFactor sub-ranges) or
// lists (a small mismatching range as its own IDs). The session converges when
// the initiator's outgoing message is all-Skip.
//
// Topology. A single initiator-driven session makes the *initiator* learn both
// its need-set (IDs the responder holds that it lacks) and its have-set (IDs it
// holds that the responder lacks); the responder learns nothing and stays
// stateless. The actual block transfer that follows is out of scope for this
// package.

// Initiator drives a reconciliation session against a local [Storage]. It tracks
// the round count and accumulates the have/need sets as leaf IdLists resolve. It
// is single-use: create one per session with [NewInitiator].
type Initiator struct {
	local Storage
	round int
	need  [][]byte
	have  [][]byte
	err   error
}

// NewInitiator returns an initiator that reconciles the local set against a peer.
func NewInitiator(local Storage) *Initiator {
	return &Initiator{local: local}
}

// Initiate returns the opening message: a single full-range Fingerprint over the
// whole local set, covering [MinBound, MaxBound).
func (it *Initiator) Initiate() Message {
	fp := it.local.Fingerprint(0, it.local.Size())
	return Message{Ranges: []Range{{
		UpperBound:  MaxBound(),
		Mode:        ModeFingerprint,
		Fingerprint: fp,
	}}}
}

// Reconcile consumes the responder's message and returns the next message to send
// plus a done flag. It updates the have/need sets from any IdList ranges. done is
// true once the outgoing message is all-Skip (converged) or the round cap is hit;
// in the latter case [Initiator.Err] is non-nil and the partial have/need are
// still available.
func (it *Initiator) Reconcile(incoming Message) (Message, bool) {
	it.round++
	if it.round > MaxRounds {
		it.err = NewErrRoundCapExceeded(it.round)
		return Message{}, true
	}

	out := Message{Ranges: make([]Range, 0, len(incoming.Ranges))}
	lo := MinBound()
	for k := range incoming.Ranges {
		r := incoming.Ranges[k]
		hi := r.UpperBound

		i, j, err := window(it.local, lo, hi)
		if err != nil {
			it.err = err
			return Message{}, true
		}

		switch r.Mode {
		case ModeSkip:
			out.Ranges = append(out.Ranges, Range{UpperBound: hi, Mode: ModeSkip})
		case ModeFingerprint:
			if mine := it.local.Fingerprint(i, j); mine == r.Fingerprint {
				out.Ranges = append(out.Ranges, Range{UpperBound: hi, Mode: ModeSkip})
			} else {
				out.Ranges = append(out.Ranges, Range{UpperBound: hi, Mode: ModeFingerprint, Fingerprint: mine})
			}
		case ModeIDList:
			it.resolveLeaf(r.IDs, i, j)
			out.Ranges = append(out.Ranges, Range{UpperBound: hi, Mode: ModeSkip})
		}

		lo = hi
	}

	if out.IsAllSkip() {
		return out, true
	}
	return out, false
}

// resolveLeaf consumes the responder's IDs for a range and the initiator's own
// items in that range [i, j), recording the set differences: need gains IDs the
// responder has but the initiator lacks; have gains IDs the initiator has but the
// responder lacks.
func (it *Initiator) resolveLeaf(remoteIDs [][]byte, i, j int) {
	remote := make(map[string]struct{}, len(remoteIDs))
	for _, id := range remoteIDs {
		remote[string(id)] = struct{}{}
	}
	localIDs := make(map[string]struct{}, j-i)
	for x := i; x < j; x++ {
		localIDs[string(it.local.ItemID(x))] = struct{}{}
	}

	for _, id := range remoteIDs {
		if _, ok := localIDs[string(id)]; !ok {
			it.need = append(it.need, id)
		}
	}
	for x := i; x < j; x++ {
		id := it.local.ItemID(x)
		if _, ok := remote[string(id)]; !ok {
			it.have = append(it.have, id)
		}
	}
}

// Need returns the IDs present on the peer but absent locally (to be pulled).
// Valid once a session reports done.
func (it *Initiator) Need() [][]byte { return it.need }

// Have returns the IDs present locally but absent on the peer (to be pushed).
// Valid once a session reports done.
func (it *Initiator) Have() [][]byte { return it.have }

// Round returns the number of Reconcile rounds executed.
func (it *Initiator) Round() int { return it.round }

// Err returns a non-nil error if the session terminated abnormally — currently
// only [NewErrRoundCapExceeded] or a malformed incoming bound.
func (it *Initiator) Err() error { return it.err }

// Respond answers an incoming message from a local [Storage] with no per-session
// state. For each incoming range it recomputes its own fingerprint over the
// bound-derived window and replies:
//
//   - Skip / IdList ranges from the initiator are settled — mirror Skip.
//   - Fingerprint match — Skip.
//   - Fingerprint mismatch, window <= IDListThreshold — emit the window's IDs.
//   - Fingerprint mismatch, larger window — split into BranchingFactor buckets.
//
// The output tiles the same keyspace as the input. Once MaxRangesPerMessage
// ranges have been produced, refinement is deferred (the range is echoed as a
// single fingerprint) so the message stays bounded.
func Respond(local Storage, incoming Message) (Message, error) {
	out := Message{Ranges: make([]Range, 0, len(incoming.Ranges))}
	lo := MinBound()
	for k := range incoming.Ranges {
		r := incoming.Ranges[k]
		hi := r.UpperBound

		i, j, err := window(local, lo, hi)
		if err != nil {
			return Message{}, err
		}

		switch r.Mode {
		case ModeSkip, ModeIDList:
			// Settled by the initiator; nothing for a stateless responder to do.
			out.Ranges = append(out.Ranges, Range{UpperBound: hi, Mode: ModeSkip})
		default: // ModeFingerprint
			mine := local.Fingerprint(i, j)
			switch {
			case mine == r.Fingerprint:
				out.Ranges = append(out.Ranges, Range{UpperBound: hi, Mode: ModeSkip})
			case len(out.Ranges) >= MaxRangesPerMessage:
				// Frame guard: defer refinement, echo our fingerprint unsplit.
				out.Ranges = append(out.Ranges, Range{UpperBound: hi, Mode: ModeFingerprint, Fingerprint: mine})
			case j-i <= IDListThreshold:
				out.Ranges = append(out.Ranges, idListRange(local, hi, i, j))
			default:
				out.Ranges = append(out.Ranges, splitRanges(local, hi, i, j)...)
			}
		}

		lo = hi
	}
	return out, nil
}

// window converts a [lo, hi) bound pair into a half-open index window over the
// storage, honoring the MaxBound sentinel. It errors if the bounds are inverted.
func window(s Storage, lo, hi []byte) (int, int, error) {
	i := s.Seek(lo)
	j := s.Seek(hi) // Seek maps MaxBound to Size()
	if i > j {
		return 0, 0, NewErrMalformedBound(lo, hi)
	}
	return i, j, nil
}

// idListRange builds an IdList range carrying the IDs in [i, j). The caller only
// invokes this when j-i <= IDListThreshold (<= MaxIDsPerRange), so the list never
// exceeds the cap.
func idListRange(s Storage, hi []byte, i, j int) Range {
	ids := make([][]byte, 0, j-i)
	for x := i; x < j; x++ {
		ids = append(ids, s.ItemID(x))
	}
	return Range{UpperBound: hi, Mode: ModeIDList, IDs: ids}
}

// splitRanges divides the window [i, j) into up to BranchingFactor contiguous
// buckets, each a Fingerprint range. Bucket upper bounds land on real item sort
// keys (exclusive) so the peer derives identical windows via Seek; the final
// bucket inherits the original upper bound hi.
func splitRanges(s Storage, hi []byte, i, j int) []Range {
	n := j - i
	b := BranchingFactor
	if n < b {
		b = n
	}

	ranges := make([]Range, 0, b)
	start := i
	for t := 1; t <= b; t++ {
		end := i + (n*t)/b
		upper := hi
		if t < b {
			upper = s.ItemSortKey(end)
		}
		ranges = append(ranges, Range{
			UpperBound:  upper,
			Mode:        ModeFingerprint,
			Fingerprint: s.Fingerprint(start, end),
		})
		start = end
	}
	return ranges
}
