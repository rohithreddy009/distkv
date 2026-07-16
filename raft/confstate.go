package raft

import (
	"encoding/binary"
	"sort"

	"github.com/rohithreddy/distkv/proto/raftpb"
)

// ConfState is the committed cluster membership. During a config change Joint
// holds C_old while Voters holds C_new (Raft §6 joint consensus).
type ConfState struct {
	Voters map[uint64]struct{}
	Joint  map[uint64]struct{} // nil when not in joint transition
}

func NewConfState(ids []uint64) ConfState {
	v := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		v[id] = struct{}{}
	}
	return ConfState{Voters: v}
}

func (c ConfState) InJoint() bool { return len(c.Joint) > 0 }

func (c ConfState) VoterIDs() []uint64 { return sortedKeys(c.Voters) }

func (c ConfState) JointIDs() []uint64 { return sortedKeys(c.Joint) }

// AllPeerIDs returns every server that should receive replication RPCs.
func (c ConfState) AllPeerIDs() []uint64 {
	if !c.InJoint() {
		return c.VoterIDs()
	}
	all := map[uint64]struct{}{}
	for id := range c.Voters {
		all[id] = struct{}{}
	}
	for id := range c.Joint {
		all[id] = struct{}{}
	}
	return sortedKeys(all)
}

func (c ConfState) Contains(id uint64) bool {
	_, ok := c.Voters[id]
	return ok
}

func majority(n int) int { return n/2 + 1 }

// HasVoteQuorum reports whether voteCount satisfies the election quorum.
func (c ConfState) HasVoteQuorum(voteCount int) bool {
	if !c.InJoint() {
		return voteCount >= majority(len(c.Voters))
	}
	return voteCount >= majority(len(c.Joint)) && voteCount >= majority(len(c.Voters))
}

// MaxCommitIndex returns the highest index replicated on required majorities.
func (c ConfState) MaxCommitIndex(matchIndex map[uint64]uint64) uint64 {
	ids := c.AllPeerIDs()
	if len(ids) == 0 {
		return 0
	}
	vals := make([]uint64, 0, len(ids))
	for _, id := range ids {
		vals = append(vals, matchIndex[id])
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] > vals[j] })

	for _, idx := range vals {
		if idx == 0 {
			continue
		}
		if c.canCommit(idx, matchIndex) {
			return idx
		}
	}
	return 0
}

func (c ConfState) canCommit(idx uint64, matchIndex map[uint64]uint64) bool {
	if !c.InJoint() {
		return countGE(c.VoterIDs(), matchIndex, idx) >= majority(len(c.Voters))
	}
	return countGE(c.JointIDs(), matchIndex, idx) >= majority(len(c.Joint)) &&
		countGE(c.VoterIDs(), matchIndex, idx) >= majority(len(c.Voters))
}

func countGE(ids []uint64, matchIndex map[uint64]uint64, idx uint64) int {
	n := 0
	for _, id := range ids {
		if matchIndex[id] >= idx {
			n++
		}
	}
	return n
}

// ApplyConfChange applies a committed config change and returns the new state.
func (c ConfState) ApplyConfChange(cc *raftpb.ConfChange) (ConfState, error) {
	next := c.clone()
	switch cc.Type {
	case raftpb.ConfChangeType_CONF_CHANGE_ADD_NODE:
		if next.InJoint() {
			return ConfState{}, ErrConfChangeInProgress
		}
		next.Joint = cloneSet(next.Voters)
		next.Voters[cc.NodeId] = struct{}{}
	case raftpb.ConfChangeType_CONF_CHANGE_REMOVE_NODE:
		if next.InJoint() {
			return ConfState{}, ErrConfChangeInProgress
		}
		if !next.Contains(cc.NodeId) {
			return ConfState{}, ErrUnknownMember
		}
		if len(next.Voters) <= 1 {
			return ConfState{}, ErrQuorumLoss
		}
		next.Joint = cloneSet(next.Voters)
		delete(next.Voters, cc.NodeId)
	case raftpb.ConfChangeType_CONF_CHANGE_LEAVE_JOINT:
		if !next.InJoint() {
			return ConfState{}, ErrNotInJoint
		}
		next.Joint = nil
	default:
		return ConfState{}, ErrInvalidConfChange
	}
	return next, nil
}

func (c ConfState) clone() ConfState {
	return ConfState{Voters: cloneSet(c.Voters), Joint: cloneSet(c.Joint)}
}

func cloneSet(m map[uint64]struct{}) map[uint64]struct{} {
	if len(m) == 0 {
		return nil
	}
	out := make(map[uint64]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

func sortedKeys(m map[uint64]struct{}) []uint64 {
	out := make([]uint64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func encodeConfState(c ConfState) []byte {
	voters := c.VoterIDs()
	joint := c.JointIDs()
	buf := make([]byte, 4+len(voters)*8+4+len(joint)*8)
	off := 0
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(voters)))
	off += 4
	for _, id := range voters {
		binary.LittleEndian.PutUint64(buf[off:], id)
		off += 8
	}
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(joint)))
	off += 4
	for _, id := range joint {
		binary.LittleEndian.PutUint64(buf[off:], id)
		off += 8
	}
	return buf[:off]
}

func decodeConfState(buf []byte) (ConfState, error) {
	if len(buf) < 4 {
		return ConfState{}, nil
	}
	off := 0
	nv := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4
	if off+nv*8 > len(buf) {
		return ConfState{}, ErrInvalidConfState
	}
	voters := make(map[uint64]struct{}, nv)
	for i := 0; i < nv; i++ {
		id := binary.LittleEndian.Uint64(buf[off:])
		off += 8
		voters[id] = struct{}{}
	}
	if off+4 > len(buf) {
		return ConfState{Voters: voters}, nil
	}
	nj := int(binary.LittleEndian.Uint32(buf[off:]))
	off += 4
	if off+nj*8 > len(buf) {
		return ConfState{}, ErrInvalidConfState
	}
	var joint map[uint64]struct{}
	if nj > 0 {
		joint = make(map[uint64]struct{}, nj)
		for i := 0; i < nj; i++ {
			id := binary.LittleEndian.Uint64(buf[off:])
			off += 8
			joint[id] = struct{}{}
		}
	}
	return ConfState{Voters: voters, Joint: joint}, nil
}
