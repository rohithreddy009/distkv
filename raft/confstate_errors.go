package raft

import "errors"

var (
	ErrConfChangeInProgress = errors.New("config change already in progress")
	ErrNotInJoint           = errors.New("not in joint config")
	ErrUnknownMember        = errors.New("unknown member")
	ErrQuorumLoss           = errors.New("config change would break quorum")
	ErrInvalidConfChange    = errors.New("invalid config change")
	ErrInvalidConfState     = errors.New("invalid persisted config state")
)
