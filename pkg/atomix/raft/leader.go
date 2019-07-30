package raft

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	log "github.com/sirupsen/logrus"
	"time"
)

// newLeaderRole returns a new leader role
func newLeaderRole(raft *RaftServer) Role {
	return &LeaderRole{
		ActiveRole: &ActiveRole{
			PassiveRole: &PassiveRole{
				raftRole: &raftRole{
					raft: raft,
				},
			},
		},
		appender: newAppender(raft),
	}
}

// LeaderRole implements a Raft leader
type LeaderRole struct {
	*ActiveRole
	appender      *raftAppender
	initIndex     int64
	configIndex   int64
	transferIndex int64
}

// Name is the name of the role
func (r *LeaderRole) Name() string {
	return "Leader"
}

func (r *LeaderRole) start() error {
	r.setLeadership()
	r.startAppender()
	r.commitInitializeEntry()
	return r.ActiveRole.start()
}

func (r *LeaderRole) setLeadership() {
	r.raft.setLeader(r.raft.cluster.member)
}

func (r *LeaderRole) startAppender() {
	go r.appender.start()
}

func (r *LeaderRole) commitInitializeEntry() {
	// Create and append an InitializeEntry.
	entry := &RaftLogEntry{
		Term:      r.raft.term,
		Timestamp: time.Now().UnixNano(),
		Entry: &RaftLogEntry_Initialize{
			Initialize: &InitializeEntry{},
		},
	}
	indexed := r.raft.writer.Append(entry)
	r.initIndex = indexed.Index

	// The Raft protocol dictates that leaders cannot commit entries from previous terms until
	// at least one entry from their current term has been stored on a majority of servers. Thus,
	// we force entries to be appended up to the leader's no-op entry. The LeaderAppender will ensure
	// that the commitIndex is not increased until the no-op entry is committed.
	err := r.appender.append(indexed)
	if err != nil {
		log.WithField("memberID", r.raft.cluster.member).
			Debugf("Failed to commit entry from leader's term; transitioning to follower")
		r.raft.setLeader("")
		r.raft.becomeFollower()
	} else {
		r.raft.state.applyEntry(indexed, nil)
	}
}

func (r *LeaderRole) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	r.raft.logRequest("PollRequest", request)
	response := &PollResponse{
		Status:   ResponseStatus_OK,
		Term:     r.raft.term,
		Accepted: false,
	}
	r.raft.logResponse("PollResponse", response, nil)
	return response, nil
}

func (r *LeaderRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.raft.logRequest("VoteRequest", request)
	if r.updateTermAndLeader(request.Term, "") {
		log.WithField("memberID", r.raft.cluster.member).
			Debug("Received greater term")
		defer r.raft.becomeFollower()
		response, err := r.ActiveRole.Vote(ctx, request)
		r.raft.logResponse("VoteResponse", response, err)
		return response, err
	} else {
		response := &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.raft.term,
			Voted:  false,
		}
		r.raft.logResponse("VoteResponse", response, nil)
		return response, nil
	}
}

func (r *LeaderRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.raft.logRequest("AppendRequest", request)
	if r.updateTermAndLeader(request.Term, request.Leader) {
		log.WithField("memberID", r.raft.cluster.member).
			Debug("Received greater term")
		defer r.raft.becomeFollower()
		response, err := r.ActiveRole.Append(ctx, request)
		r.raft.logResponse("AppendResponse", response, err)
		return response, err
	} else if request.Term < r.raft.term {
		response := &AppendResponse{
			Status:       ResponseStatus_OK,
			Term:         r.raft.term,
			Succeeded:    false,
			LastLogIndex: r.raft.writer.LastIndex(),
		}
		r.raft.logResponse("AppendResponse", response, nil)
		return response, nil
	} else {
		r.raft.setLeader(request.Leader)
		defer r.raft.becomeFollower()
		response, err := r.ActiveRole.Append(ctx, request);
		r.raft.logResponse("AppendResponse", response, err)
		return response, err
	}
}

func (r *LeaderRole) Command(request *CommandRequest, server RaftService_CommandServer) error {
	r.raft.logRequest("CommandRequest", request)

	entry := &RaftLogEntry{
		Term:      r.raft.term,
		Timestamp: time.Now().UnixNano(),
		Entry: &RaftLogEntry_Command{
			Command: &CommandEntry{
				Value: request.Value,
			},
		},
	}
	indexed := r.raft.writer.Append(entry)

	if err := r.appender.append(indexed); err != nil {
		response := &CommandResponse{
			Status: ResponseStatus_ERROR,
			Error:  RaftError_PROTOCOL_ERROR,
		}
		return r.raft.logResponse("CommandResponse", response, server.Send(response))
	} else {
		ch := make(chan service.Output)
		r.raft.state.applyEntry(indexed, ch)
		for output := range ch {
			if output.Succeeded() {
				response := &CommandResponse{
					Status:  ResponseStatus_OK,
					Leader:  r.raft.leader,
					Term:    r.raft.term,
					Members: r.raft.cluster.memberIDs,
					Output:  output.Value,
				}
				err := r.raft.logResponse("CommandResponse", response, server.Send(response))
				if err != nil {
					return err
				}
			} else {
				response := &CommandResponse{
					Status:  ResponseStatus_ERROR,
					Error:   RaftError_APPLICATION_ERROR,
					Message: output.Error.Error(),
					Leader:  r.raft.leader,
					Term:    r.raft.term,
					Members: r.raft.cluster.memberIDs,
				}
				err := r.raft.logResponse("CommandResponse", response, server.Send(response))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *LeaderRole) Query(request *QueryRequest, server RaftService_QueryServer) error {
	r.raft.logRequest("QueryRequest", request)
	switch request.ReadConsistency {
	case ReadConsistency_LINEARIZABLE:
		return r.queryLinearizable(request, server)
	case ReadConsistency_LINEARIZABLE_LEASE:
		return r.queryLinearizableLease(request, server)
	case ReadConsistency_SEQUENTIAL:
		return r.querySequential(request, server)
	default:
		return r.queryLinearizable(request, server)
	}
}

func (r *LeaderRole) queryLinearizable(request *QueryRequest, server RaftService_QueryServer) error {
	entry := &IndexedEntry{
		Index: r.raft.writer.LastIndex(),
		Entry: &RaftLogEntry{
			Term:      r.raft.term,
			Timestamp: time.Now().UnixNano(),
			Entry: &RaftLogEntry_Query{
				Query: &QueryEntry{
					Value: request.Value,
				},
			},
		},
	}

	// Create a result channel
	ch := make(chan service.Output)

	// Apply the entry to the state machine
	r.raft.state.applyEntry(entry, ch)

	// Iterate through results and translate them into QueryResponses.
	for result := range ch {
		// Send a heartbeat to a majority of the cluster to verify leadership.
		if err := r.appender.heartbeat(); err != nil {
			return r.raft.logResponse("QueryResponse", nil, err)
		}
		if result.Succeeded() {
			response := &QueryResponse{
				Status: ResponseStatus_OK,
				Output: result.Value,
			}
			err := r.raft.logResponse("QueryResponse", response, server.Send(response))
			if err != nil {
				return err
			}
		} else {
			response := &QueryResponse{
				Status:  ResponseStatus_ERROR,
				Message: result.Error.Error(),
			}
			err := r.raft.logResponse("QueryResponse", response, server.Send(response))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *LeaderRole) queryLinearizableLease(request *QueryRequest, server RaftService_QueryServer) error {
	return r.applyQuery(request, server)
}

func (r *LeaderRole) querySequential(request *QueryRequest, server RaftService_QueryServer) error {
	return r.applyQuery(request, server)
}

func (r *LeaderRole) stepDown() {
	if r.raft.leader != "" && r.raft.leader == r.raft.cluster.member {
		r.raft.setLeader("")
	}
}

func (r *LeaderRole) stop() error {
	r.appender.stop()
	r.stepDown()
	return nil
}
