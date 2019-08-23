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
	log "github.com/sirupsen/logrus"
)

func newActiveRole(server *RaftServer) *ActiveRole {
	return &ActiveRole{
		PassiveRole: newPassiveRole(server),
	}
}

// ActiveRole implements a Raft follower
type ActiveRole struct {
	*PassiveRole
}

func (r *ActiveRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.server.logRequest("AppendRequest", request)

	// Acquire a write lock to append entries to the log.
	r.server.writeLock()
	defer r.server.writeUnlock()

	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context and transition to follower.
	if r.updateTermAndLeader(request.Term, request.Leader) {
		go r.server.becomeFollower()
	}
	response, err := r.handleAppend(ctx, request)
	_ = r.server.logResponse("AppendResponse", response, err)
	return response, err
}

func (r *ActiveRole) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	r.server.logRequest("PollRequest", request)

	// Acquire a write lock to update the leader and term.
	r.server.writeLock()
	r.updateTermAndLeader(request.Term, "")
	r.server.writeUnlock()

	// Acquire a read lock to vote for the follower.
	r.server.readLock()
	response, err := r.handlePoll(ctx, request)
	r.server.readUnlock()
	_ = r.server.logResponse("PollResponse", response, err)
	return response, err
}

func (r *ActiveRole) handlePoll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	// If the request term is not as great as the current context term then don't
	// vote for the candidate. We want to vote for candidates that are at least
	// as up to date as us.
	if request.Term < r.server.term {
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Rejected %v: candidate's term %d is less than the current term %d", request, request.Term, r.server.term)
		return &PollResponse{
			Status:   ResponseStatus_OK,
			Term:     r.server.term,
			Accepted: false,
		}, nil
	} else if r.isLogUpToDate(request.LastLogIndex, request.LastLogTerm, request) {
		return &PollResponse{
			Status:   ResponseStatus_OK,
			Term:     r.server.term,
			Accepted: true,
		}, nil
	} else {
		return &PollResponse{
			Status:   ResponseStatus_OK,
			Term:     r.server.term,
			Accepted: false,
		}, nil
	}
}

func (r *ActiveRole) isLogUpToDate(lastIndex Index, lastTerm Term, request interface{}) bool {
	// Read the last entry from the log.
	lastEntry := r.server.writer.LastEntry()

	// If the log is empty then vote for the candidate.
	if lastEntry == nil {
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Accepted %v: candidate's log is up-to-date", request)
		return true
	}

	// If the candidate's last log term is lower than the local log's last entry term, reject the request.
	if lastTerm < lastEntry.Entry.Term {
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Rejected %v: candidate's last log entry (%d) is at a lower term than the local log (%d)", request, lastTerm, lastEntry.Entry.Term)
		return false
	}

	// If the candidate's last term is equal to the local log's last entry term, reject the request if the
	// candidate's last index is less than the local log's last index. If the candidate's last log term is
	// greater than the local log's last term then it's considered up to date, and if both have the same term
	// then the candidate's last index must be greater than the local log's last index.
	if lastTerm == lastEntry.Entry.Term && lastIndex < lastEntry.Index {
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Rejected %v: candidate's last log entry (%d) is at a lower index than the local log (%d)", request, lastIndex, lastEntry.Index)
		return false
	}

	// If we made it this far, the candidate's last term is greater than or equal to the local log's last
	// term, and if equal to the local log's last term, the candidate's last index is equal to or greater
	// than the local log's last index.
	log.WithField("memberID", r.server.cluster.member).
		Debugf("Accepted %v: candidate's log is up-to-date", request)
	return true
}

func (r *ActiveRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.server.logRequest("VoteRequest", request)

	// Vote requests can modify the server's vote record, so we need to hold a write lock while handling the request.
	r.server.writeLock()
	defer r.server.writeUnlock()

	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context.
	if r.updateTermAndLeader(request.Term, "") {
		go r.server.becomeFollower()
	}

	response, err := r.handleVote(ctx, request)
	_ = r.server.logResponse("VoteResponse", response, err)
	return response, err
}

func (r *ActiveRole) handleVote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	if request.Term < r.server.term {
		// If the request term is not as great as the current context term then don't
		// vote for the candidate. We want to vote for candidates that are at least
		// as up to date as us.
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Rejected %+v: candidate's term is less than the current term", request)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.server.term,
			Voted:  false,
		}, nil
	} else if r.server.leader != "" {
		// If a leader was already determined for this term then reject the request.
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Rejected %+v: leader already exists", request)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.server.term,
			Voted:  false,
		}, nil
	} else if _, ok := r.server.cluster.members[request.Candidate]; !ok {
		// If the requesting candidate is not a known member of the cluster (to this
		// node) then don't vote for it. Only vote for candidates that we know about.
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Rejected %+v: candidate is not known to the local member", request)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.server.term,
			Voted:  false,
		}, nil
	} else if r.server.lastVotedFor == nil {
		// If no vote has been cast, check the log and cast a vote if necessary.
		if r.isLogUpToDate(request.LastLogIndex, request.LastLogTerm, request) {
			r.server.setLastVotedFor(&request.Candidate)
			return &VoteResponse{
				Status: ResponseStatus_OK,
				Term:   r.server.term,
				Voted:  true,
			}, nil
		} else {
			return &VoteResponse{
				Status: ResponseStatus_OK,
				Term:   r.server.term,
				Voted:  false,
			}, nil
		}
	} else if *r.server.lastVotedFor == request.Candidate {
		// If we already voted for the requesting server, respond successfully.
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Accepted %+v: already voted for %+v", request, request.Candidate)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.server.term,
			Voted:  true,
		}, nil
	} else {
		// In this case, we've already voted for someone else.
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Rejected %+v: already voted for %+v", request, r.server.lastVotedFor)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.server.term,
			Voted:  false,
		}, nil
	}
}
