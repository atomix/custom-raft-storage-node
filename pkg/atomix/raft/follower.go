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

// newFollowerRole returns a new follower role
func newFollowerRole(server *RaftServer) Role {
	return &FollowerRole{
		ActiveRole: newActiveRole(server),
	}
}

// FollowerRole implements a Raft follower
type FollowerRole struct {
	*ActiveRole
	heartbeatTimer *time.Timer
	heartbeatStop  chan bool
}

// Name is the name of the role
func (r *FollowerRole) Name() string {
	return "Follower"
}

func (r *FollowerRole) start() error {
	// If there are no other members in the cluster, immediately transition to candidate to increment the term.
	if len(r.server.cluster.members) == 1 {
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Single node cluster; starting election")
		go r.server.becomeCandidate()
		return nil
	}
	r.ActiveRole.start()
	r.resetHeartbeatTimeout()
	return nil
}

func (r *FollowerRole) stop() error {
	if r.heartbeatTimer != nil && r.heartbeatTimer.Stop() {
		r.heartbeatStop <- true
	}
	return r.ActiveRole.stop()
}

func (r *FollowerRole) resetHeartbeatTimeout() {
	r.server.writeLock()
	defer r.server.writeUnlock()

	// If a timer is already set, cancel the timer.
	if r.heartbeatTimer != nil {
		if !r.heartbeatTimer.Stop() {
			r.heartbeatStop <- true
			return
		}
	}

	// Set the election timeout in a semi-random fashion with the random range
	// being election timeout and 2 * election timeout.
	timeout := r.server.electionTimeout + time.Duration(rand.Int63n(int64(r.server.electionTimeout)))
	r.heartbeatTimer = time.NewTimer(timeout)
	heartbeatStop := make(chan bool, 1)
	r.heartbeatStop = heartbeatStop
	heartbeatCh := r.heartbeatTimer.C
	go func() {
		select {
		case <-heartbeatCh:
			r.server.writeLock()
			if r.active {
				r.server.setLeader("")
				r.server.writeUnlock()
				log.WithField("memberID", r.server.cluster.member).
					Debugf("Heartbeat timed out in %d", timeout)
				r.sendPollRequests()
			} else {
				r.server.writeUnlock()
			}
		case <-heartbeatStop:
			return
		}
	}()
}

// sendPollRequests sends PollRequests to all members of the cluster
func (r *FollowerRole) sendPollRequests() {
	// Set a new timer within which other nodes must respond in order for this node to transition to candidate.
	timeoutTimer := time.NewTimer(r.server.electionTimeout)
	timeoutExpired := make(chan bool, 1)
	go func() {
		select {
		case <-timeoutTimer.C:
			if r.active {
				log.WithField("memberID", r.server.cluster.member).
					Debugf("Failed to poll a majority of the cluster in %d", r.server.electionTimeout)
				r.resetHeartbeatTimeout()
			}
		case <-timeoutExpired:
			return
		}
	}()

	// Create a quorum that will track the number of nodes that have responded to the poll request.
	votingMembers := r.server.cluster.memberIDs
	votes := make(chan bool, len(votingMembers))
	quorum := int(math.Floor(float64(len(votingMembers))/2.0) + 1)
	go func() {
		acceptCount := 0
		rejectCount := 0
		for vote := range votes {
			r.server.readLock()
			if !r.active {
				r.server.readUnlock()
				return
			}
			if vote {
				// If no leader has been discovered and the quorum was reached, transition to candidate.
				acceptCount++
				if r.server.leader == "" && acceptCount == quorum {
					r.server.readUnlock()
					log.WithField("memberID", r.server.cluster.member).
						Debugf("Received %d/%d pre-votes; transitioning to candidate", acceptCount, len(votingMembers))
					go r.server.becomeCandidate()
					return
				} else {
					r.server.readUnlock()
				}
			} else {
				rejectCount++
				if rejectCount == quorum {
					log.WithField("memberID", r.server.cluster.member).
						Debugf("Received %d/%d rejected pre-votes; resetting heartbeat timeout", rejectCount, len(votingMembers))
					r.resetHeartbeatTimeout()
					return
				}
				r.server.readUnlock()
			}
		}

		// If not enough votes were received, reset the heartbeat timeout.
		r.resetHeartbeatTimeout()
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
		Debugf("Polling members %v", votingMembers)

	// Once we got the last log term, iterate through each current member
	// of the cluster and vote each member for a vote.
	for _, member := range votingMembers {
		// Vote for yourself!
		if member == r.server.cluster.member {
			votes <- true
			continue
		}

		go func(member MemberID) {
			r.server.readLock()
			term := r.server.term
			r.server.readUnlock()
			log.WithField("memberID", r.server.cluster.member).
				Debugf("Polling %s for next term %d", member, term+1)
			request := &PollRequest{
				Term:         term,
				Candidate:    r.server.cluster.member,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}

			client, err := r.server.cluster.getClient(member)
			if err != nil {
				votes <- false
				log.WithField("memberID", r.server.cluster.member).Warn(err)
			} else {
				r.server.logSend("PollRequest", request)
				response, err := client.Poll(context.Background(), request)
				if err != nil {
					votes <- false
					log.WithField("memberID", r.server.cluster.member).Warn(err)
				} else {
					r.server.logReceive("PollResponse", response)

					// If the response term is greater than the term we send, use a double checked lock
					// to increment the term.
					if response.Term > term {
						r.server.writeLock()
						if response.Term > r.server.term {
							r.server.setTerm(response.Term)
						}
						r.server.writeUnlock()
					}

					if !response.Accepted {
						log.WithField("memberID", r.server.cluster.member).
							Debugf("Received rejected poll from %s", member)
						votes <- false
					} else if response.Term != request.Term {
						log.WithField("memberID", r.server.cluster.member).
							Debugf("Received accepted poll for a different term from %s", member)
						votes <- false
					} else {
						log.WithField("memberID", r.server.cluster.member).
							Debugf("Received accepted poll from %s", member)
						votes <- true
					}
				}
			}
		}(member)
	}
}

func (r *FollowerRole) Configure(ctx context.Context, request *ConfigureRequest) (*ConfigureResponse, error) {
	response, err := r.PassiveRole.Configure(ctx, request)
	r.resetHeartbeatTimeout()
	return response, err
}

func (r *FollowerRole) Install(stream RaftService_InstallServer) error {
	err := r.PassiveRole.Install(stream)
	r.resetHeartbeatTimeout()
	return err
}

func (r *FollowerRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	response, err := r.PassiveRole.Append(ctx, request)
	r.resetHeartbeatTimeout()
	return response, err
}

func (r *FollowerRole) handleVote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.server.writeLock()
	response, err := r.ActiveRole.handleVote(ctx, request)
	r.server.writeUnlock()
	if response.Voted {
		r.resetHeartbeatTimeout()
	}
	return response, err
}
