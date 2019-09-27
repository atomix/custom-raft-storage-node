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
	raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/state"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/store"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
)

func newActiveRole(raft raft.Raft, state state.Manager, store store.Store, log util.Logger) *ActiveRole {
	return &ActiveRole{
		PassiveRole: newPassiveRole(raft, state, store, log),
	}
}

// ActiveRole implements a Raft follower
type ActiveRole struct {
	*PassiveRole
}

// Append handles an append request
func (r *ActiveRole) Append(ctx context.Context, request *raft.AppendRequest) (*raft.AppendResponse, error) {
	r.log.Request("AppendRequest", request)

	// Acquire a write lock to append entries to the log.
	r.raft.WriteLock()
	defer r.raft.WriteUnlock()

	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context and transition to follower.
	if r.updateTermAndLeader(request.Term, &request.Leader) {
		defer r.raft.SetRole(raft.RoleFollower)
	}

	response, err := r.handleAppend(ctx, request)
	_ = r.log.Response("AppendResponse", response, err)
	return response, err
}

// Poll handles a poll request
func (r *ActiveRole) Poll(ctx context.Context, request *raft.PollRequest) (*raft.PollResponse, error) {
	r.log.Request("PollRequest", request)

	// Acquire a write lock to update the leader and term.
	r.raft.WriteLock()
	r.updateTermAndLeader(request.Term, nil)
	r.raft.WriteUnlock()

	// Acquire a read lock to vote for the follower.
	r.raft.ReadLock()
	response, err := r.handlePoll(ctx, request)
	r.raft.ReadUnlock()
	_ = r.log.Response("PollResponse", response, err)
	return response, err
}

// handlePoll handles a poll request
func (r *ActiveRole) handlePoll(ctx context.Context, request *raft.PollRequest) (*raft.PollResponse, error) {
	// If the request term is not as great as the current context term then don't
	// vote for the candidate. We want to vote for candidates that are at least
	// as up to date as us.
	if request.Term < r.raft.Term() {
		r.log.Debug("Rejected %v: candidate's term %d is less than the current term %d", request, request.Term, r.raft.Term())
		return &raft.PollResponse{
			Status:   raft.ResponseStatus_OK,
			Term:     r.raft.Term(),
			Accepted: false,
		}, nil
	} else if r.isLogUpToDate(request.LastLogIndex, request.LastLogTerm, request) {
		return &raft.PollResponse{
			Status:   raft.ResponseStatus_OK,
			Term:     r.raft.Term(),
			Accepted: true,
		}, nil
	} else {
		return &raft.PollResponse{
			Status:   raft.ResponseStatus_OK,
			Term:     r.raft.Term(),
			Accepted: false,
		}, nil
	}
}

// isLogUpToDate returns a boolean indicating whether the log is up to date with the given index and term
func (r *ActiveRole) isLogUpToDate(lastIndex raft.Index, lastTerm raft.Term, request interface{}) bool {
	// Read the last entry from the log.
	lastEntry := r.store.Writer().LastEntry()

	// If the log is empty then vote for the candidate.
	if lastEntry == nil {
		r.log.Debug("Accepted %v: candidate's log is up-to-date", request)
		return true
	}

	// If the candidate's last log term is lower than the local log's last entry term, reject the request.
	if lastTerm < lastEntry.Entry.Term {
		r.log.Debug("Rejected %v: candidate's last log entry (%d) is at a lower term than the local log (%d)", request, lastTerm, lastEntry.Entry.Term)
		return false
	}

	// If the candidate's last term is equal to the local log's last entry term, reject the request if the
	// candidate's last index is less than the local log's last index. If the candidate's last log term is
	// greater than the local log's last term then it's considered up to date, and if both have the same term
	// then the candidate's last index must be greater than the local log's last index.
	if lastTerm == lastEntry.Entry.Term && lastIndex < lastEntry.Index {
		r.log.Debug("Rejected %v: candidate's last log entry (%d) is at a lower index than the local log (%d)", request, lastIndex, lastEntry.Index)
		return false
	}

	// If we made it this far, the candidate's last term is greater than or equal to the local log's last
	// term, and if equal to the local log's last term, the candidate's last index is equal to or greater
	// than the local log's last index.
	r.log.Debug("Accepted %v: candidate's log is up-to-date", request)
	return true
}

// Vote handles a vote request
func (r *ActiveRole) Vote(ctx context.Context, request *raft.VoteRequest) (*raft.VoteResponse, error) {
	r.log.Request("VoteRequest", request)

	// Vote requests can modify the server's vote record, so we need to hold a write lock while handling the request.
	r.raft.WriteLock()
	defer r.raft.WriteUnlock()

	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context.
	if r.updateTermAndLeader(request.Term, nil) {
		defer r.raft.SetRole(raft.RoleFollower)
	}

	response, err := r.handleVote(ctx, request)
	_ = r.log.Response("VoteResponse", response, err)
	return response, err
}

// handleVote handles a vote request
func (r *ActiveRole) handleVote(ctx context.Context, request *raft.VoteRequest) (*raft.VoteResponse, error) {
	if request.Term < r.raft.Term() {
		// If the request term is not as great as the current context term then don't
		// vote for the candidate. We want to vote for candidates that are at least
		// as up to date as us.
		r.log.Debug("Rejected %+v: candidate's term is less than the current term", request)
		return &raft.VoteResponse{
			Status: raft.ResponseStatus_OK,
			Term:   r.raft.Term(),
			Voted:  false,
		}, nil
	} else if r.raft.Leader() != nil {
		// If a leader was already determined for this term then reject the request.
		r.log.Debug("Rejected %+v: leader already exists", request)
		return &raft.VoteResponse{
			Status: raft.ResponseStatus_OK,
			Term:   r.raft.Term(),
			Voted:  false,
		}, nil
	} else if r.raft.GetMember(request.Candidate) == nil {
		// If the requesting candidate is not a known member of the cluster (to this
		// node) then don't vote for it. Only vote for candidates that we know about.
		r.log.Debug("Rejected %+v: candidate is not known to the local member", request)
		return &raft.VoteResponse{
			Status: raft.ResponseStatus_OK,
			Term:   r.raft.Term(),
			Voted:  false,
		}, nil
	} else if r.raft.LastVotedFor() == nil {
		// If no vote has been cast, check the log and cast a vote if necessary.
		if r.isLogUpToDate(request.LastLogIndex, request.LastLogTerm, request) {
			if err := r.raft.SetLastVotedFor(request.Candidate); err != nil {
				r.log.Error("Failed to handle vote request", err)
				return &raft.VoteResponse{
					Status: raft.ResponseStatus_OK,
					Term:   r.raft.Term(),
					Voted:  false,
				}, nil
			}
			return &raft.VoteResponse{
				Status: raft.ResponseStatus_OK,
				Term:   r.raft.Term(),
				Voted:  true,
			}, nil
		}
		return &raft.VoteResponse{
			Status: raft.ResponseStatus_OK,
			Term:   r.raft.Term(),
			Voted:  false,
		}, nil
	} else if *r.raft.LastVotedFor() == request.Candidate {
		// If we already voted for the requesting server, respond successfully.
		r.log.Debug("Accepted %+v: already voted for %+v", request, request.Candidate)
		return &raft.VoteResponse{
			Status: raft.ResponseStatus_OK,
			Term:   r.raft.Term(),
			Voted:  true,
		}, nil
	} else {
		// In this case, we've already voted for someone else.
		r.log.Debug("Rejected %+v: already voted for %+v", request, r.raft.LastVotedFor())
		return &raft.VoteResponse{
			Status: raft.ResponseStatus_OK,
			Term:   r.raft.Term(),
			Voted:  false,
		}, nil
	}
}
