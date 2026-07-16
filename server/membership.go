package server

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rohithreddy/distkv/proto/kvpb"
	"github.com/rohithreddy/distkv/proto/raftpb"
)

func (s *Server) AddMember(ctx context.Context, req *kvpb.AddMemberRequest) (*kvpb.AddMemberResponse, error) {
	if req.Id == 0 || req.RaftAddr == "" {
		return nil, status.Error(codes.InvalidArgument, "id and raft_addr required")
	}
	if err := s.changeMembership(ctx, &raftpb.ConfChange{
		Type:    raftpb.ConfChangeType_CONF_CHANGE_ADD_NODE,
		NodeId:  req.Id,
		Context: []byte(req.RaftAddr),
	}); err != nil {
		return nil, err
	}
	return &kvpb.AddMemberResponse{}, nil
}

func (s *Server) RemoveMember(ctx context.Context, req *kvpb.RemoveMemberRequest) (*kvpb.RemoveMemberResponse, error) {
	if req.Id == 0 {
		return nil, status.Error(codes.InvalidArgument, "id required")
	}
	if req.Id == s.opts.ID {
		return nil, status.Error(codes.InvalidArgument, "cannot remove self via RPC")
	}
	conf := s.node.ConfState()
	if len(conf.Voters) <= 1 {
		return nil, status.Error(codes.FailedPrecondition, "refusing to remove last voter")
	}
	if err := s.changeMembership(ctx, &raftpb.ConfChange{
		Type:   raftpb.ConfChangeType_CONF_CHANGE_REMOVE_NODE,
		NodeId: req.Id,
	}); err != nil {
		return nil, err
	}
	return &kvpb.RemoveMemberResponse{}, nil
}

func (s *Server) ListMembers(ctx context.Context, req *kvpb.ListMembersRequest) (*kvpb.ListMembersResponse, error) {
	conf := s.node.ConfState()
	addrs := s.node.MemberAddrs()
	resp := &kvpb.ListMembersResponse{InJoint: conf.InJoint()}
	for _, id := range conf.VoterIDs() {
		resp.Members = append(resp.Members, &kvpb.MemberInfo{
			Id:       id,
			RaftAddr: addrs[id],
			Voting:   true,
		})
	}
	return resp, nil
}

func (s *Server) changeMembership(ctx context.Context, cc *raftpb.ConfChange) error {
	if err := s.proposeConfChange(ctx, cc); err != nil {
		return err
	}
	if cc.Type == raftpb.ConfChangeType_CONF_CHANGE_LEAVE_JOINT {
		return nil
	}
	return s.proposeConfChange(ctx, &raftpb.ConfChange{
		Type: raftpb.ConfChangeType_CONF_CHANGE_LEAVE_JOINT,
	})
}

func (s *Server) proposeConfChange(ctx context.Context, cc *raftpb.ConfChange) error {
	index, term, err := s.node.ProposeConfChange(cc)
	if err != nil {
		return s.notLeaderErr()
	}

	ch := make(chan waitResult, 1)
	s.mu.Lock()
	s.waiters[index] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.waiters, index)
		s.mu.Unlock()
	}()

	select {
	case res := <-ch:
		if res.term != term {
			return s.notLeaderErr()
		}
		if res.err != nil {
			return status.Error(codes.Internal, res.err.Error())
		}
		return nil
	case <-ctx.Done():
		return status.Error(codes.DeadlineExceeded, "config change timed out")
	case <-time.After(10 * time.Second):
		return status.Error(codes.Unavailable, "config change timed out")
	}
}

func (s *Server) onConfChangeApplied(cc *raftpb.ConfChange) {
	if s.transport == nil {
		return
	}
	switch cc.Type {
	case raftpb.ConfChangeType_CONF_CHANGE_ADD_NODE:
		if len(cc.Context) > 0 {
			s.transport.SetPeer(cc.NodeId, string(cc.Context))
		}
	case raftpb.ConfChangeType_CONF_CHANGE_REMOVE_NODE:
		s.transport.RemovePeer(cc.NodeId)
	}
}
