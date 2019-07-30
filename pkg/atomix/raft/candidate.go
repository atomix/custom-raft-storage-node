package raft

import (
	"context"
	log "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"time"
)

// newCandidateRole returns a new candidate role
func newCandidateRole(raft *RaftServer) Role {
	return &CandidateRole{
		ActiveRole: &ActiveRole{
			PassiveRole: &PassiveRole{
				raftRole: &raftRole{
					raft: raft,
				},
			},
		},
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

func (r *CandidateRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.raft.logRequest("AppendRequest", request)

	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context and step down as a candidate.
	if request.Term >= r.raft.term {
		r.raft.setTerm(request.Term)
		defer r.raft.becomeFollower()
	}
	response, err := r.ActiveRole.Append(ctx, request)
	r.raft.logResponse("AppendResponse", response, err)
	return response, err
}

func (r *CandidateRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	r.raft.logRequest("VoteRequest", request)

	// If the request indicates a term that is greater than the current term then
	// assign that term and leader to the current context and step down as a candidate.
	if r.updateTermAndLeader(request.Term, "") {
		defer r.raft.becomeFollower()
		response, err := r.ActiveRole.Vote(ctx, request)
		r.raft.logResponse("VoteResponse", response, err)
		return response, err
	}

	// Candidates will always vote for themselves, so if the vote request is for this node then accept the request.
	// Otherwise, reject it.
	if request.Candidate == r.raft.cluster.member {
		response := &VoteResponse{
			Status: ResponseStatus_OK,
			Term:   r.raft.term,
			Voted:  true,
		}
		r.raft.logResponse("VoteResponse", response, nil)
		return response, nil
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
	timeout := r.raft.electionTimeout + time.Duration(rand.Int63n(int64(r.raft.electionTimeout)))
	r.electionTimer = time.NewTimer(timeout)
	r.electionExpired = make(chan bool, 1)
	go func() {
		select {
		case <-r.electionTimer.C:
			if r.active {
				// When the election times out, clear the previous majority vote
				// check and restart the election.
				log.WithField("memberID", r.raft.cluster.member).
					Debugf("Election round for term %d expired: not enough votes received within the election timeout; restarting election", r.raft.term)
				r.sendVoteRequests()
			}
		case <-r.electionExpired:
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
	r.raft.setTerm(r.raft.term + 1)
	r.raft.setLastVotedFor(r.raft.cluster.member)

	// Create a quorum that will track the number of nodes that have responded to the poll request.
	votingMembers := r.raft.cluster.memberIDs

	// If there are no other members in the cluster, immediately transition to leader.
	if len(votingMembers) == 1 {
		log.WithField("memberID", r.raft.cluster.member).
			Debug("Single node cluster; skipping election")
		r.raft.becomeLeader()
		return
	}

	// Compute the quorum and create a goroutine to count votes
	votes := make(chan bool, len(votingMembers))
	quorum := int(math.Floor(float64(len(votingMembers))/2.0) + 1)
	go func() {
		voteCount := 0
		rejectCount := 0
		for vote := range votes {
			if !r.active {
				return
			}
			if vote {
				// If no oother leader has been discovered and a quorum of votes was received, transition to leader.
				voteCount++
				if r.raft.leader == "" && voteCount == quorum {
					log.WithField("memberID", r.raft.cluster.member).
						Debugf("Won election with %d/%d votes; transitioning to leader", voteCount, len(votingMembers))
					r.raft.becomeLeader()
					return
				}
			} else {
				// If a quorum of vote requests were rejected, transition back to follower.
				rejectCount++
				if rejectCount == quorum {
					log.WithField("memberID", r.raft.cluster.member).
						Debugf("Lost election with %d/%d votes rejected; transitioning back to follower", rejectCount, len(votingMembers))
					r.raft.becomeFollower()
				}
			}
		}

		// If not enough votes were received, restart the election.
		r.sendVoteRequests()
	}()

	// First, load the last log entry to get its term. We load the entry
	// by its index since the index is required by the protocol.
	lastEntry := r.raft.writer.LastEntry()
	var lastIndex int64
	if lastEntry != nil {
		lastIndex = lastEntry.Index
	}

	var lastTerm int64
	if lastEntry != nil {
		lastTerm = lastEntry.Entry.Term
	}

	log.WithField("memberID", r.raft.cluster.member).
		Debugf("Requesting votes for term %d", r.raft.term)

	// Once we got the last log term, iterate through each current member
	// of the cluster and request a vote from each.
	for _, member := range votingMembers {
		// Vote for yourself!
		if member == r.raft.cluster.member {
			votes <- true
			continue
		}

		go func(member string) {
			log.WithField("memberID", r.raft.cluster.member).
				Debugf("Requesting vote from %s for term %d", member, r.raft.term+1)
			request := &VoteRequest{
				Term:         r.raft.term,
				Candidate:    r.raft.cluster.member,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}

			client, err := r.raft.getClient(member)
			if err == nil {
				r.raft.logSend("VoteRequest", request)
				response, err := client.Vote(context.Background(), request)
				if err != nil {
					votes <- false
					log.WithField("memberID", r.raft.cluster.member).Warn(err)
				} else {
					r.raft.logReceive("VoteResponse", response)
					if response.Term > r.raft.term {
						log.WithField("memberID", r.raft.cluster.member).
							Debugf("Received greater term from %s; transitioning back to follower", member)
						r.raft.setTerm(response.Term)
						r.raft.becomeFollower()
						close(votes)
					} else if !response.Voted {
						log.WithField("memberID", r.raft.cluster.member).
							Debugf("Received rejected vote from %s", member)
						votes <- false
					} else if response.Term != r.raft.term {
						log.WithField("memberID", r.raft.cluster.member).
							Debugf("Received successful vote for a different term from %s", member)
						votes <- false
					} else {
						log.WithField("memberID", r.raft.cluster.member).
							Debugf("Received successful vote from %s", member)
						votes <- true
					}
				}
			}
		}(member)
	}
}
