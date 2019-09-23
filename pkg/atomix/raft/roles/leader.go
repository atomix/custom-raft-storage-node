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

package roles

import (
	"context"
	"github.com/atomix/atomix-go-node/pkg/atomix/node"
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/state"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store/log"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"time"
)

// newLeaderRole returns a new leader role
func newLeaderRole(protocol raft.Raft, state state.Manager, store store.Store) raft.Role {
	log := util.NewRoleLogger(string(protocol.Member()), string(raft.RoleLeader))
	return &LeaderRole{
		ActiveRole: newActiveRole(protocol, state, store, log),
		appender:   newAppender(protocol, state, store, log),
	}
}

// LeaderRole implements a Raft leader
type LeaderRole struct {
	*ActiveRole
	appender  *raftAppender
	initIndex raft.Index
}

// Type is the role type
func (r *LeaderRole) Type() raft.RoleType {
	return raft.RoleLeader
}

// Start starts the leader
func (r *LeaderRole) Start() error {
	r.setLeadership()
	go r.startAppender()
	go r.commitInitializeEntry()
	return r.ActiveRole.Start()
}

// setLeadership sets the leader as the current leader
func (r *LeaderRole) setLeadership() {
	member := r.raft.Member()
	if err := r.raft.SetLeader(&member); err != nil {
		r.log.Error("Failed to set leadership", err)
	}
}

// startAppender starts the appender goroutines
func (r *LeaderRole) startAppender() {
	r.appender.start()
}

// commitInitializeEntry commits a no-op entry for the leader
func (r *LeaderRole) commitInitializeEntry() {
	r.raft.WriteLock()

	// Create and append an InitializeEntry.
	entry := &raft.LogEntry{
		Term:      r.raft.Term(),
		Timestamp: time.Now(),
		Entry: &raft.LogEntry_Initialize{
			Initialize: &raft.InitializeEntry{},
		},
	}
	indexed := r.store.Writer().Append(entry)
	r.raft.WriteUnlock()

	r.initIndex = indexed.Index

	// The Raft protocol dictates that leaders cannot commit entries from previous terms until
	// at least one entry from their current term has been stored on a majority of servers. Thus,
	// we force entries to be appended up to the leader's no-op entry. The LeaderAppender will ensure
	// that the commitIndex is not increased until the no-op entry is committed.
	err := r.appender.commit(indexed, nil)
	if err != nil {
		r.log.Debug("Failed to commit entry from leader's term; transitioning to follower")
		r.raft.WriteLock()
		defer r.raft.WriteUnlock()
		if err := r.raft.SetLeader(nil); err != nil {
			r.log.Error("Failed to unset leader", err)
		}
		r.raft.SetRole(raft.RoleFollower)
	} else {
		r.state.ApplyEntry(indexed, nil)
	}
}

// Poll handles a poll request
func (r *LeaderRole) Poll(ctx context.Context, request *raft.PollRequest) (*raft.PollResponse, error) {
	r.log.Request("PollRequest", request)
	r.raft.ReadLock()
	defer r.raft.ReadUnlock()
	response := &raft.PollResponse{
		Status:   raft.ResponseStatus_OK,
		Term:     r.raft.Term(),
		Accepted: false,
	}
	_ = r.log.Response("PollResponse", response, nil)
	return response, nil
}

// Vote handles a vote request
func (r *LeaderRole) Vote(ctx context.Context, request *raft.VoteRequest) (*raft.VoteResponse, error) {
	r.log.Request("VoteRequest", request)
	r.raft.WriteLock()
	defer r.raft.WriteUnlock()
	if r.updateTermAndLeader(request.Term, nil) {
		r.log.Debug("Received greater term")
		defer r.raft.SetRole(raft.RoleFollower)
		response, err := r.ActiveRole.Vote(ctx, request)
		_ = r.log.Response("VoteResponse", response, err)
		return response, err
	}

	response := &raft.VoteResponse{
		Status: raft.ResponseStatus_OK,
		Term:   r.raft.Term(),
		Voted:  false,
	}
	_ = r.log.Response("VoteResponse", response, nil)
	return response, nil
}

// Append handles an append request
func (r *LeaderRole) Append(ctx context.Context, request *raft.AppendRequest) (*raft.AppendResponse, error) {
	r.log.Request("AppendRequest", request)
	r.raft.WriteLock()
	defer r.raft.WriteUnlock()
	if r.updateTermAndLeader(request.Term, &request.Leader) {
		r.log.Debug("Received greater term")
		defer r.raft.SetRole(raft.RoleFollower)
		response, err := r.ActiveRole.Append(ctx, request)
		_ = r.log.Response("AppendResponse", response, err)
		return response, err
	} else if request.Term < r.raft.Term() {
		response := &raft.AppendResponse{
			Status:       raft.ResponseStatus_OK,
			Term:         r.raft.Term(),
			Succeeded:    false,
			LastLogIndex: r.store.Writer().LastIndex(),
		}
		_ = r.log.Response("AppendResponse", response, nil)
		return response, nil
	}

	response, err := r.ActiveRole.Append(ctx, request)
	_ = r.log.Response("AppendResponse", response, err)
	return response, err
}

// Command handles a command request
func (r *LeaderRole) Command(request *raft.CommandRequest, responseCh chan<- *raft.CommandStreamResponse) error {
	r.log.Request("CommandRequest", request)

	// Acquire the write lock to write the entry to the log.
	r.raft.WriteLock()

	entry := &raft.LogEntry{
		Term:      r.raft.Term(),
		Timestamp: time.Now(),
		Entry: &raft.LogEntry_Command{
			Command: &raft.CommandEntry{
				Value: request.Value,
			},
		},
	}
	indexed := r.store.Writer().Append(entry)

	// Release the write lock immediately after appending the entry to ensure the appenders
	// can acquire a read lock for the log.
	r.raft.WriteUnlock()

	// Create a function to apply the entry to the state machine once committed.
	// This is done in a function to ensure entries are applied in the order in which they
	// are committed by the appender.
	outputCh := make(chan node.Output)
	f := func() {
		r.state.ApplyEntry(indexed, outputCh)
	}

	// Pass the apply function to the appender to be called when the change is committed.
	if err := r.appender.commit(indexed, f); err != nil {
		response := &raft.CommandResponse{
			Status: raft.ResponseStatus_ERROR,
			Error:  raft.ResponseError_PROTOCOL_ERROR,
		}
		_ = r.log.Response("CommandResponse", response, nil)
		responseCh <- raft.NewCommandStreamResponse(response, nil)
		return nil
	}

	for output := range outputCh {
		var status raft.ResponseStatus
		var err raft.ResponseError
		if output.Succeeded() {
			status = raft.ResponseStatus_OK
		} else {
			status = raft.ResponseStatus_ERROR
			err = raft.ResponseError_APPLICATION_ERROR
		}

		r.raft.ReadLock()
		response := &raft.CommandResponse{
			Status:  status,
			Error:   err,
			Leader:  r.raft.Member(),
			Term:    r.raft.Term(),
			Members: r.raft.Members(),
			Output:  output.Value,
		}
		r.raft.ReadUnlock()
		_ = r.log.Response("CommandResponse", response, nil)
		responseCh <- raft.NewCommandStreamResponse(response, nil)
	}
	return nil
}

// Query handles a query request
func (r *LeaderRole) Query(request *raft.QueryRequest, responseCh chan<- *raft.QueryStreamResponse) error {
	r.log.Request("QueryRequest", request)

	// Acquire a read lock before creating the entry.
	r.raft.ReadLock()

	// Create the entry to apply to the state machine.
	entry := &log.Entry{
		Index: r.store.Writer().LastIndex(),
		Entry: &raft.LogEntry{
			Term:      r.raft.Term(),
			Timestamp: time.Now(),
			Entry: &raft.LogEntry_Query{
				Query: &raft.QueryEntry{
					Value: request.Value,
				},
			},
		},
	}

	// Release the read lock before applying the entry.
	r.raft.ReadUnlock()

	switch request.ReadConsistency {
	case raft.ReadConsistency_LINEARIZABLE:
		return r.queryLinearizable(entry, responseCh)
	case raft.ReadConsistency_LINEARIZABLE_LEASE:
		return r.queryLinearizableLease(entry, responseCh)
	case raft.ReadConsistency_SEQUENTIAL:
		return r.querySequential(entry, responseCh)
	default:
		return r.queryLinearizable(entry, responseCh)
	}
}

// queryLinearizable performs a linearizable query
func (r *LeaderRole) queryLinearizable(entry *log.Entry, responseCh chan<- *raft.QueryStreamResponse) error {
	// Create a result channel
	ch := make(chan node.Output)

	// Apply the entry to the state machine
	r.state.ApplyEntry(entry, ch)

	// Iterate through results and translate them into QueryResponses.
	for result := range ch {
		// Send a heartbeat to a majority of the cluster to verify leadership.
		if err := r.appender.heartbeat(); err != nil {
			return r.log.Response("QueryResponse", nil, err)
		}
		if result.Succeeded() {
			response := &raft.QueryResponse{
				Status: raft.ResponseStatus_OK,
				Output: result.Value,
			}
			_ = r.log.Response("QueryResponse", response, nil)
			responseCh <- raft.NewQueryStreamResponse(response, nil)
		} else {
			response := &raft.QueryResponse{
				Status:  raft.ResponseStatus_ERROR,
				Message: result.Error.Error(),
			}
			_ = r.log.Response("QueryResponse", response, nil)
			responseCh <- raft.NewQueryStreamResponse(response, nil)
		}
	}
	return nil
}

// queryLinearizableLease performs a lease query
func (r *LeaderRole) queryLinearizableLease(entry *log.Entry, responseCh chan<- *raft.QueryStreamResponse) error {
	return r.applyQuery(entry, responseCh)
}

// querySequential performs a sequential query
func (r *LeaderRole) querySequential(entry *log.Entry, responseCh chan<- *raft.QueryStreamResponse) error {
	return r.applyQuery(entry, responseCh)
}

// stepDown unsets the leader
func (r *LeaderRole) stepDown() {
	if r.raft.Leader() != nil && *r.raft.Leader() == r.raft.Member() {
		if err := r.raft.SetLeader(nil); err != nil {
			r.log.Error("Failed to step down", err)
		}
	}
}

// Stop stops the leader
func (r *LeaderRole) Stop() error {
	r.appender.stop()
	r.stepDown()
	return nil
}
