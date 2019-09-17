// Copyright 2019-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	log "github.com/sirupsen/logrus"
	"time"
)

// newLeaderRole returns a new leader role
func newLeaderRole(server *Server) Role {
	return &LeaderRole{
		ActiveRole: newActiveRole(server),
		appender:   newAppender(server),
	}
}

// LeaderRole implements a Raft leader
type LeaderRole struct {
	*ActiveRole
	appender  *raftAppender
	initIndex Index
}

// Name is the name of the role
func (r *LeaderRole) Name() string {
	return "Leader"
}

// start starts the leader
func (r *LeaderRole) start() error {
	r.setLeadership()
	go r.startAppender()
	go r.commitInitializeEntry()
	return r.ActiveRole.start()
}

// setLeadership sets the leader as the current leader
func (r *LeaderRole) setLeadership() {
	r.server.setLeader(r.server.cluster.member)
}

// startAppender starts the appender goroutines
func (r *LeaderRole) startAppender() {
	r.appender.start()
}

// commitInitializeEntry commits a no-op entry for the leader
func (r *LeaderRole) commitInitializeEntry() {
	r.server.writeLock()

	// Create and append an InitializeEntry.
	entry := &RaftLogEntry{
		Term:      r.server.term,
		Timestamp: time.Now(),
		Entry: &RaftLogEntry_Initialize{
			Initialize: &InitializeEntry{},
		},
	}
	indexed := r.server.writer.Append(entry)
	r.server.writeUnlock()

	r.initIndex = indexed.Index

	// The Raft protocol dictates that leaders cannot commit entries from previous terms until
	// at least one entry from their current term has been stored on a majority of servers. Thus,
	// we force entries to be appended up to the leader's no-op entry. The LeaderAppender will ensure
	// that the commitIndex is not increased until the no-op entry is committed.
	err := r.appender.commit(indexed, nil)
	if err != nil {
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Failed to commit entry from leader's term; transitioning to follower")
		r.server.writeLock()
		r.server.setLeader("")
		r.server.writeUnlock()
		go r.server.becomeFollower()
	} else {
		r.server.state.applyEntry(indexed, nil)
	}
}

// Poll handles a poll request
func (r *LeaderRole) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	r.server.logRequest("PollRequest", request)
	r.server.readLock()
	defer r.server.readUnlock()
	response := &PollResponse{
		Status:   ResponseStatus_OK,
		Term:     r.server.term,
		Accepted: false,
	}
	_ = r.server.logResponse("PollResponse", response, nil)
	return response, nil
}

// Vote handles a vote request
func (r *LeaderRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.server.logRequest("VoteRequest", request)
	r.server.writeLock()
	defer r.server.writeUnlock()
	if r.updateTermAndLeader(request.Term, "") {
		log.WithField("memberID", r.server.cluster.member).
			Debug("Received greater term")
		defer r.server.becomeFollower()
		response, err := r.ActiveRole.Vote(ctx, request)
		_ = r.server.logResponse("VoteResponse", response, err)
		return response, err
	}

	response := &VoteResponse{
		Status: ResponseStatus_OK,
		Term:   r.server.term,
		Voted:  false,
	}
	_ = r.server.logResponse("VoteResponse", response, nil)
	return response, nil
}

// Append handles an append request
func (r *LeaderRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.server.logRequest("AppendRequest", request)
	r.server.writeLock()
	defer r.server.writeUnlock()
	if r.updateTermAndLeader(request.Term, request.Leader) {
		log.WithField("memberID", r.server.cluster.member).
			Debug("Received greater term")
		defer r.server.becomeFollower()
		response, err := r.ActiveRole.Append(ctx, request)
		_ = r.server.logResponse("AppendResponse", response, err)
		return response, err
	} else if request.Term < r.server.term {
		response := &AppendResponse{
			Status:       ResponseStatus_OK,
			Term:         r.server.term,
			Succeeded:    false,
			LastLogIndex: r.server.writer.LastIndex(),
		}
		_ = r.server.logResponse("AppendResponse", response, nil)
		return response, nil
	}

	r.server.setLeader(request.Leader)
	defer r.server.becomeFollower()
	response, err := r.ActiveRole.Append(ctx, request)
	_ = r.server.logResponse("AppendResponse", response, err)
	return response, err
}

// Command handles a command request
func (r *LeaderRole) Command(request *CommandRequest, server RaftService_CommandServer) error {
	r.server.logRequest("CommandRequest", request)

	// Acquire the write lock to write the entry to the log.
	r.server.writeLock()

	entry := &RaftLogEntry{
		Term:      r.server.term,
		Timestamp: time.Now(),
		Entry: &RaftLogEntry_Command{
			Command: &CommandEntry{
				Value: request.Value,
			},
		},
	}
	indexed := r.server.writer.Append(entry)

	// Release the write lock immediately after appending the entry to ensure the appenders
	// can acquire a read lock for the log.
	r.server.writeUnlock()

	// Create a function to apply the entry to the state machine once committed.
	// This is done in a function to ensure entries are applied in the order in which they
	// are committed by the appender.
	ch := make(chan node.Output)
	f := func() {
		r.server.state.applyEntry(indexed, ch)
	}

	// Pass the apply function to the appender to be called when the change is committed.
	if err := r.appender.commit(indexed, f); err != nil {
		response := &CommandResponse{
			Status: ResponseStatus_ERROR,
			Error:  RaftError_PROTOCOL_ERROR,
		}
		return r.server.logResponse("CommandResponse", response, server.Send(response))
	}

	for output := range ch {
		if output.Succeeded() {
			r.server.readLock()
			response := &CommandResponse{
				Status:  ResponseStatus_OK,
				Leader:  r.server.leader,
				Term:    r.server.term,
				Members: r.server.cluster.memberIDs,
				Output:  output.Value,
			}
			r.server.readUnlock()
			err := r.server.logResponse("CommandResponse", response, server.Send(response))
			if err != nil {
				return err
			}
		} else {
			r.server.readLock()
			response := &CommandResponse{
				Status:  ResponseStatus_ERROR,
				Error:   RaftError_APPLICATION_ERROR,
				Message: output.Error.Error(),
				Leader:  r.server.leader,
				Term:    r.server.term,
				Members: r.server.cluster.memberIDs,
			}
			r.server.readUnlock()
			err := r.server.logResponse("CommandResponse", response, server.Send(response))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Query handles a query request
func (r *LeaderRole) Query(request *QueryRequest, server RaftService_QueryServer) error {
	r.server.logRequest("QueryRequest", request)

	// Acquire a read lock before creating the entry.
	r.server.readLock()

	// Create the entry to apply to the state machine.
	entry := &LogEntry{
		Index: r.server.writer.LastIndex(),
		Entry: &RaftLogEntry{
			Term:      r.server.term,
			Timestamp: time.Now(),
			Entry: &RaftLogEntry_Query{
				Query: &QueryEntry{
					Value: request.Value,
				},
			},
		},
	}

	// Release the read lock before applying the entry.
	r.server.readUnlock()

	switch request.ReadConsistency {
	case ReadConsistency_LINEARIZABLE:
		return r.queryLinearizable(entry, server)
	case ReadConsistency_LINEARIZABLE_LEASE:
		return r.queryLinearizableLease(entry, server)
	case ReadConsistency_SEQUENTIAL:
		return r.querySequential(entry, server)
	default:
		return r.queryLinearizable(entry, server)
	}
}

// queryLinearizable performs a linearizable query
func (r *LeaderRole) queryLinearizable(entry *LogEntry, server RaftService_QueryServer) error {
	// Create a result channel
	ch := make(chan node.Output)

	// Apply the entry to the state machine
	r.server.state.applyEntry(entry, ch)

	// Iterate through results and translate them into QueryResponses.
	for result := range ch {
		// Send a heartbeat to a majority of the cluster to verify leadership.
		if err := r.appender.heartbeat(); err != nil {
			return r.server.logResponse("QueryResponse", nil, err)
		}
		if result.Succeeded() {
			response := &QueryResponse{
				Status: ResponseStatus_OK,
				Output: result.Value,
			}
			err := r.server.logResponse("QueryResponse", response, server.Send(response))
			if err != nil {
				return err
			}
		} else {
			response := &QueryResponse{
				Status:  ResponseStatus_ERROR,
				Message: result.Error.Error(),
			}
			err := r.server.logResponse("QueryResponse", response, server.Send(response))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// queryLinearizableLease performs a lease query
func (r *LeaderRole) queryLinearizableLease(entry *LogEntry, server RaftService_QueryServer) error {
	return r.applyQuery(entry, server)
}

// querySequential performs a sequential query
func (r *LeaderRole) querySequential(entry *LogEntry, server RaftService_QueryServer) error {
	return r.applyQuery(entry, server)
}

// stepDown unsets the leader
func (r *LeaderRole) stepDown() {
	if r.server.leader != "" && r.server.leader == r.server.cluster.member {
		r.server.setLeader("")
	}
}

// stop stops the leader
func (r *LeaderRole) stop() error {
	r.appender.stop()
	r.stepDown()
	return nil
}
