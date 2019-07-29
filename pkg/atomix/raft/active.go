package raft

import (
	"context"
	log "github.com/sirupsen/logrus"
)

// ActiveRole implements a Raft follower
type ActiveRole struct {
	*PassiveRole
}

func (r *ActiveRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context and transition to follower.
	if r.updateTermAndLeader(request.Term, request.Leader) {
		defer r.raft.becomeFollower()
	}
	return r.handleAppend(ctx, request)
}

func (r *ActiveRole) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	r.updateTermAndLeader(request.Term, "")
	return r.handlePoll(ctx, request)
}

func (r *ActiveRole) handlePoll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	// If the request term is not as great as the current context term then don't
	// vote for the candidate. We want to vote for candidates that are at least
	// as up to date as us.
	if request.Term < r.raft.term {
		log.Debugf("Rejected %v: candidate's term is less than the current term", request)
		return &PollResponse{
			Status:   ResponseStatus_OK,
			Term:     r.raft.term,
			Accepted: false,
		}, nil
	} else if r.isLogUpToDate(request.LastLogIndex, request.LastLogTerm, request) {
		return &PollResponse{
			Status:   ResponseStatus_OK,
			Term:     r.raft.term,
			Accepted: true,
		}, nil
	} else {
		return &PollResponse{
			Status:   ResponseStatus_OK,
			Term:     r.raft.term,
			Accepted: false,
		}, nil
	}
}

func (r *ActiveRole) isLogUpToDate(lastIndex int64, lastTerm int64, request interface{}) bool {
	// Read the last entry from the log.
	lastEntry := r.raft.writer.LastEntry()

	// If the log is empty then vote for the candidate.
	if lastEntry == nil {
		log.Debugf("Accepted %v: candidate's log is up-to-date", request)
		return true
	}

	// If the candidate's last log term is lower than the local log's last entry term, reject the request.
	if lastTerm < lastEntry.Entry.Term {
		log.Debugf("Rejected %v: candidate's last log entry (%d) is at a lower term than the local log (%d)", request, lastTerm, lastEntry.Entry.Term)
		return false
	}

	// If the candidate's last term is equal to the local log's last entry term, reject the request if the
	// candidate's last index is less than the local log's last index. If the candidate's last log term is
	// greater than the local log's last term then it's considered up to date, and if both have the same term
	// then the candidate's last index must be greater than the local log's last index.
	if lastTerm == lastEntry.Entry.Term && lastIndex < lastEntry.Index {
		log.Debugf("Rejected %v: candidate's last log entry (%d) is at a lower index than the local log (%d)", request, lastIndex, lastEntry.Index)
		return false
	}

	// If we made it this far, the candidate's last term is greater than or equal to the local log's last
	// term, and if equal to the local log's last term, the candidate's last index is equal to or greater
	// than the local log's last index.
	log.Debugf("Accepted %v: candidate's log is up-to-date", request)
	return true
}

func (r *ActiveRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context.
	transition := r.updateTermAndLeader(request.Term, "")

	response, err := r.handleVote(ctx, request)
	if transition {
		r.raft.becomeFollower()
	}
	return response, err
}

func (r *ActiveRole) handleVote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	if request.Term < r.raft.term {
		// If the request term is not as great as the current context term then don't
		// vote for the candidate. We want to vote for candidates that are at least
		// as up to date as us.
		log.Debugf("Rejected %v: candidate's term is less than the current term", request)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.raft.term,
			Voted:  false,
		}, nil
	} else if r.raft.leader != "" {
		// If a leader was already determined for this term then reject the request.
		log.Debugf("Rejected %v: leader already exists", request)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.raft.term,
			Voted:  false,
		}, nil
	} else if _, ok := r.raft.cluster.members[request.Candidate]; !ok {
		// If the requesting candidate is not a known member of the cluster (to this
		// node) then don't vote for it. Only vote for candidates that we know about.
		log.Debugf("Rejected %v: candidate is not known to the local member", request)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.raft.term,
			Voted:  false,
		}, nil
	} else if r.raft.lastVotedFor == "" {
		// If no vote has been cast, check the log and cast a vote if necessary.
		if r.isLogUpToDate(request.LastLogIndex, request.LastLogTerm, request) {
			r.raft.setLastVotedFor(request.Candidate)
			return &VoteResponse{
				Status: ResponseStatus_OK,
				Term:   r.raft.term,
				Voted:  true,
			}, nil
		} else {
			return &VoteResponse{
				Status: ResponseStatus_OK,
				Term:   r.raft.term,
				Voted:  false,
			}, nil
		}
	} else if r.raft.lastVotedFor == request.Candidate {
		// If we already voted for the requesting server, respond successfully.
		log.Debugf("Accepted %v: already voted for %s", request, request.Candidate)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.raft.term,
			Voted:  true,
		}, nil
	} else {
		// In this case, we've already voted for someone else.
		log.Debugf("Rejected %v: already voted for %s", request, r.raft.lastVotedFor)
		return &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.raft.term,
			Voted:  false,
		}, nil
	}
}
