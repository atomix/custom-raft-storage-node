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
	log "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"time"
)

// newCandidateRole returns a new candidate role
func newCandidateRole(protocol raft.Raft, state state.Manager, store store.Store) raft.Role {
	log := util.NewRoleLogger(string(protocol.Member()), string(raft.RoleCandidate))
	return &CandidateRole{
		ActiveRole: newActiveRole(protocol, state, store, log),
	}
}

// CandidateRole implements a Raft candidate
type CandidateRole struct {
	*ActiveRole
	electionTimer   *time.Timer
	electionExpired chan bool
}

// Type is the role type
func (r *CandidateRole) Type() raft.RoleType {
	return raft.RoleCandidate
}

// Start starts the candidate
func (r *CandidateRole) Start() error {
	// If there are no other members in the cluster, immediately transition to leader.
	if len(r.raft.Members()) == 1 {
		log.WithField("memberID", r.raft.Member()).
			Debug("Single node cluster; skipping election")
		defer r.raft.SetRole(raft.RoleLeader)
		return nil
	}
	_ = r.ActiveRole.Start()
	r.sendVoteRequests()
	return nil
}

// Stop stops the candidate
func (r *CandidateRole) Stop() error {
	if r.electionTimer != nil && r.electionTimer.Stop() {
		r.electionExpired <- true
	}
	return r.ActiveRole.Stop()
}

// Vote handles a vote request
func (r *CandidateRole) Vote(ctx context.Context, request *raft.VoteRequest) (*raft.VoteResponse, error) {
	r.log.Request("VoteRequest", request)
	r.raft.WriteLock()
	defer r.raft.WriteUnlock()

	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context and step down as a candidate.
	if r.updateTermAndLeader(request.Term, nil) {
		defer r.raft.SetRole(raft.RoleFollower)
		response, err := r.handleVote(ctx, request)
		_ = r.log.Response("VoteResponse", response, err)
		return response, err
	}

	// Candidates will always vote for themselves, so if the vote request is for this node then accept the request.
	if request.Candidate == r.raft.Member() {
		response := &raft.VoteResponse{
			Status: raft.ResponseStatus_OK,
			Term:   r.raft.Term(),
			Voted:  true,
		}
		_ = r.log.Response("VoteResponse", response, nil)
		return response, nil
	}

	// Otherwise, reject it.
	response := &raft.VoteResponse{
		Status: raft.ResponseStatus_OK,
		Term:   r.raft.Term(),
		Voted:  false,
	}
	_ = r.log.Response("VoteResponse", response, nil)
	return response, nil
}

// resetElectionTimeout resets the candidate's election timer
func (r *CandidateRole) resetElectionTimeout() {
	// If a timer is already set, cancel the timer.
	if r.electionTimer != nil {
		if !r.electionTimer.Stop() {
			r.electionExpired <- true
			return
		}
	}

	// Set the election timeout in a semi-random fashion with the random range
	// being election timeout and 2 * election timeout.
	timeout := r.raft.Config().GetElectionTimeoutOrDefault() + time.Duration(rand.Int63n(int64(r.raft.Config().GetElectionTimeoutOrDefault())))
	r.electionTimer = time.NewTimer(timeout)
	electionCh := r.electionTimer.C
	r.electionExpired = make(chan bool, 1)
	expiredCh := r.electionExpired
	go func() {
		select {
		case <-electionCh:
			if r.active {
				// When the election times out, clear the previous majority vote
				// check and restart the election.
				log.WithField("memberID", r.raft.Member()).
					Debugf("Election round for term %d expired: not enough votes received within the election timeout; restarting election", r.raft.Term())
				r.sendVoteRequests()
			}
		case <-expiredCh:
			return
		}
	}()
}

// sendVoteRequests sends vote requests to peers
func (r *CandidateRole) sendVoteRequests() {
	// Because of asynchronous execution, the candidate state could have already been closed. In that case,
	// simply skip the election.
	if !r.active {
		return
	}

	// Reset the election timeout.
	r.resetElectionTimeout()

	// When the election timer is reset, increment the current term and
	// restart the election.
	r.raft.WriteLock()
	member := r.raft.Member()
	if err := r.raft.SetTerm(r.raft.Term() + 1); err != nil {
		r.log.Error("Failed to increment term", err)
		defer r.raft.SetRole(raft.RoleFollower)
		return
	}
	if err := r.raft.SetLastVotedFor(member); err != nil {
		r.log.Error("Failed to vote for self", err)
		defer r.raft.SetRole(raft.RoleFollower)
		return
	}
	term := r.raft.Term()
	r.raft.WriteUnlock()

	// Create a quorum that will track the number of nodes that have responded to the poll request.
	votingMembers := r.raft.Members()

	// Compute the quorum and create a goroutine to count votes
	votes := make(chan bool, len(votingMembers))
	quorum := int(math.Floor(float64(len(votingMembers))/2.0) + 1)
	go func() {
		voteCount := 0
		rejectCount := 0
		for vote := range votes {
			r.raft.WriteLock()
			if !r.active {
				r.raft.WriteUnlock()
				return
			}
			if vote {
				// If no other leader has been discovered and a quorum of votes was received, transition to leader.
				voteCount++
				if r.raft.Leader() == nil && voteCount == quorum {
					log.WithField("memberID", r.raft.Member()).
						Debugf("Won election with %d/%d votes; transitioning to leader", voteCount, len(votingMembers))
					r.raft.SetRole(raft.RoleLeader)
					r.raft.WriteUnlock()
					return
				}
				r.raft.WriteUnlock()
			} else {
				// If a quorum of vote requests were rejected, transition back to follower.
				rejectCount++
				if rejectCount == quorum {
					log.WithField("memberID", r.raft.Member()).
						Debugf("Lost election with %d/%d votes rejected; transitioning back to follower", rejectCount, len(votingMembers))
					r.raft.SetRole(raft.RoleFollower)
					r.raft.WriteUnlock()
					return
				}
				r.raft.WriteUnlock()
			}
		}

		// If not enough votes were received, restart the election.
		r.sendVoteRequests()
	}()

	// First, load the last log entry to get its term. We load the entry
	// by its index since the index is required by the protocol.
	r.raft.ReadLock()
	lastEntry := r.store.Writer().LastEntry()
	r.raft.ReadUnlock()
	var lastIndex raft.Index
	if lastEntry != nil {
		lastIndex = lastEntry.Index
	}

	var lastTerm raft.Term
	if lastEntry != nil {
		lastTerm = lastEntry.Entry.Term
	}

	log.WithField("memberID", r.raft.Member()).
		Debugf("Requesting votes for term %d", term)

	// Once we got the last log term, iterate through each current member
	// of the cluster and request a vote from each.
	for _, member := range votingMembers {
		// Vote for yourself!
		if member == r.raft.Member() {
			votes <- true
			continue
		}

		go func(member raft.MemberID) {
			log.WithField("memberID", r.raft.Member()).
				Debugf("Requesting vote from %s for term %d", member, term)
			request := &raft.VoteRequest{
				Term:         term,
				Candidate:    r.raft.Member(),
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}

			r.log.Send("VoteRequest", request)
			response, err := r.raft.Protocol().Vote(context.Background(), request, member)
			if err != nil {
				votes <- false
				log.WithField("memberID", r.raft.Member()).Warn(err)
			} else {
				r.log.Receive("VoteResponse", response)
				r.raft.WriteLock()
				if response.Term > request.Term {
					log.WithField("memberID", r.raft.Member()).
						Debugf("Received greater term from %s; transitioning back to follower", member)
					_ = r.raft.SetTerm(response.Term)
					defer r.raft.SetRole(raft.RoleFollower)
					r.raft.WriteUnlock()
					close(votes)
					return
				} else if !response.Voted {
					log.WithField("memberID", r.raft.Member()).
						Debugf("Received rejected vote from %s", member)
					votes <- false
				} else if response.Term != r.raft.Term() {
					log.WithField("memberID", r.raft.Member()).
						Debugf("Received successful vote for a different term from %s", member)
					votes <- false
				} else {
					log.WithField("memberID", r.raft.Member()).
						Debugf("Received successful vote from %s", member)
					votes <- true
				}
				r.raft.WriteUnlock()
			}
		}(member)
	}
}
