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
	"math"
	"math/rand"
	"time"
)

// newCandidateRole returns a new candidate role
func newCandidateRole(server *RaftServer) Role {
	return &CandidateRole{
		ActiveRole: newActiveRole(server),
	}
}

// CandidateRole implements a Raft candidate
type CandidateRole struct {
	*ActiveRole
	electionTimer   *time.Timer
	electionExpired chan bool
}

// Name is the name of the role
func (r *CandidateRole) Name() string {
	return "Candidate"
}

func (r *CandidateRole) start() error {
	// If there are no other members in the cluster, immediately transition to leader.
	if len(r.server.cluster.members) == 1 {
		log.WithField("memberID", r.server.cluster.member).
			Debug("Single node cluster; skipping election")
		go r.server.becomeLeader()
		return nil
	}
	r.ActiveRole.start()
	r.sendVoteRequests()
	return nil
}

func (r *CandidateRole) stop() error {
	if r.electionTimer != nil && r.electionTimer.Stop() {
		r.electionExpired <- true
	}
	return r.ActiveRole.stop()
}

func (r *CandidateRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.server.logRequest("VoteRequest", request)
	r.server.writeLock()
	defer r.server.writeUnlock()

	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context and step down as a candidate.
	if r.updateTermAndLeader(request.Term, "") {
		go r.server.becomeFollower()
		response, err := r.handleVote(ctx, request)
		r.server.logResponse("VoteResponse", response, err)
		return response, err
	}

	// Candidates will always vote for themselves, so if the vote request is for this node then accept the request.
	// Otherwise, reject it.
	if request.Candidate == r.server.cluster.member {
		response := &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.server.term,
			Voted:  true,
		}
		r.server.logResponse("VoteResponse", response, nil)
		return response, nil
	} else {
		response := &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.server.term,
			Voted:  false,
		}
		r.server.logResponse("VoteResponse", response, nil)
		return response, nil
	}
}

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
	timeout := r.server.electionTimeout + time.Duration(rand.Int63n(int64(r.server.electionTimeout)))
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
				log.WithField("memberID", r.server.cluster.member).
					Debugf("Election round for term %d expired: not enough votes received within the election timeout; restarting election", r.server.term)
				r.sendVoteRequests()
			}
		case <-expiredCh:
			return
		}
	}()
}

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
	r.server.writeLock()
	r.server.setTerm(r.server.term + 1)
	r.server.setLastVotedFor(&r.server.cluster.member)
	term := r.server.term
	r.server.writeUnlock()

	// Create a quorum that will track the number of nodes that have responded to the poll request.
	votingMembers := r.server.cluster.memberIDs

	// Compute the quorum and create a goroutine to count votes
	votes := make(chan bool, len(votingMembers))
	quorum := int(math.Floor(float64(len(votingMembers))/2.0) + 1)
	go func() {
		voteCount := 0
		rejectCount := 0
		for vote := range votes {
			r.server.writeLock()
			if !r.active {
				r.server.writeUnlock()
				return
			}
			if vote {
				// If no other leader has been discovered and a quorum of votes was received, transition to leader.
				voteCount++
				if r.server.leader == "" && voteCount == quorum {
					log.WithField("memberID", r.server.cluster.member).
						Debugf("Won election with %d/%d votes; transitioning to leader", voteCount, len(votingMembers))
					go r.server.becomeLeader()
					r.server.writeUnlock()
					return
				} else {
					r.server.writeUnlock()
				}
			} else {
				// If a quorum of vote requests were rejected, transition back to follower.
				rejectCount++
				if rejectCount == quorum {
					log.WithField("memberID", r.server.cluster.member).
						Debugf("Lost election with %d/%d votes rejected; transitioning back to follower", rejectCount, len(votingMembers))
					go r.server.becomeFollower()
					r.server.writeUnlock()
					return
				} else {
					r.server.writeUnlock()
				}
			}
		}

		// If not enough votes were received, restart the election.
		r.sendVoteRequests()
	}()

	// First, load the last log entry to get its term. We load the entry
	// by its index since the index is required by the protocol.
	r.server.readLock()
	lastEntry := r.server.writer.LastEntry()
	r.server.readUnlock()
	var lastIndex Index
	if lastEntry != nil {
		lastIndex = lastEntry.Index
	}

	var lastTerm Term
	if lastEntry != nil {
		lastTerm = lastEntry.Entry.Term
	}

	log.WithField("memberID", r.server.cluster.member).
		Debugf("Requesting votes for term %d", term)

	// Once we got the last log term, iterate through each current member
	// of the cluster and request a vote from each.
	for _, member := range votingMembers {
		// Vote for yourself!
		if member == r.server.cluster.member {
			votes <- true
			continue
		}

		go func(member MemberID) {
			log.WithField("memberID", r.server.cluster.member).
				Debugf("Requesting vote from %s for term %d", member, term)
			request := &VoteRequest{
				Term:         term,
				Candidate:    r.server.cluster.member,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}

			client, err := r.server.cluster.getClient(member)
			if err == nil {
				r.server.logSend("VoteRequest", request)
				response, err := client.Vote(context.Background(), request)
				if err != nil {
					votes <- false
					log.WithField("memberID", r.server.cluster.member).Warn(err)
				} else {
					r.server.logReceive("VoteResponse", response)
					r.server.writeLock()
					if response.Term > request.Term {
						log.WithField("memberID", r.server.cluster.member).
							Debugf("Received greater term from %s; transitioning back to follower", member)
						r.server.setTerm(response.Term)
						go r.server.becomeFollower()
						r.server.writeUnlock()
						close(votes)
						return
					} else if !response.Voted {
						log.WithField("memberID", r.server.cluster.member).
							Debugf("Received rejected vote from %s", member)
						votes <- false
					} else if response.Term != r.server.term {
						log.WithField("memberID", r.server.cluster.member).
							Debugf("Received successful vote for a different term from %s", member)
						votes <- false
					} else {
						log.WithField("memberID", r.server.cluster.member).
							Debugf("Received successful vote from %s", member)
						votes <- true
					}
					r.server.writeUnlock()
				}
			}
		}(member)
	}
}
