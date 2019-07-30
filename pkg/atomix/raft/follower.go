package raft

import (
	"context"
	log "github.com/sirupsen/logrus"
	"math"
	"math/rand"
	"time"
)

// newFollowerRole returns a new follower role
func newFollowerRole(raft *RaftServer) Role {
	return &FollowerRole{
		ActiveRole: &ActiveRole{
			PassiveRole: &PassiveRole{
				raft: raft,
			},
		},
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
	// If there are no other members in the cluster, immediately transition to leader.
	if len(r.raft.cluster.members) == 1 {
		log.WithField("memberID", r.raft.cluster.member).Debugf("Single node cluster; starting election")
		r.raft.becomeCandidate()
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
	// If a timer is already set, cancel the timer.
	if r.heartbeatTimer != nil {
		if !r.heartbeatTimer.Stop() {
			r.heartbeatStop <- true
			return
		}
	}

	// Set the election timeout in a semi-random fashion with the random range
	// being election timeout and 2 * election timeout.
	timeout := r.raft.electionTimeout + time.Duration(rand.Int63n(int64(r.raft.electionTimeout)))
	r.heartbeatTimer = time.NewTimer(timeout)
	r.heartbeatStop = make(chan bool, 1)
	go func() {
		select {
		case <-r.heartbeatTimer.C:
			if r.active {
				r.raft.setLeader("")
				log.WithField("memberID", r.raft.cluster.member).
					Debugf("Heartbeat timed out in %d", timeout)
				r.sendPollRequests()
			}
		case <-r.heartbeatStop:
			return
		}
	}()
}

// sendPollRequests sends PollRequests to all members of the cluster
func (r *FollowerRole) sendPollRequests() {
	// Set a new timer within which other nodes must respond in order for this node to transition to candidate.
	timeoutTimer := time.NewTimer(r.raft.electionTimeout)
	timeoutExpired := make(chan bool, 1)
	go func() {
		select {
		case <-timeoutTimer.C:
			log.WithField("memberID", r.raft.cluster.member).
				Debugf("Failed to poll a majority of the cluster in %d", r.raft.electionTimeout)
			r.resetHeartbeatTimeout()
		case <-timeoutExpired:
			return
		}
	}()

	// Create a quorum that will track the number of nodes that have responded to the poll request.
	votingMembers := r.raft.cluster.memberIDs

	// If there are no other members in the cluster, immediately transition to leader.
	if len(votingMembers) == 1 {
		log.WithField("memberID", r.raft.cluster.member).
			Debugf("Single node cluster; starting election")
		r.raft.becomeCandidate()
		return
	}

	// Compute the quorum and create a goroutine to count votes
	votes := make(chan bool, len(votingMembers))
	quorum := int(math.Floor(float64(len(votingMembers))/2.0) + 1)
	go func() {
		acceptCount := 0
		rejectCount := 0
		for vote := range votes {
			if !r.active {
				return
			}
			if vote {
				// If no leader has been discovered and the quorum was reached, transition to candidate.
				acceptCount++
				if r.raft.leader == "" && acceptCount == quorum {
					log.WithField("memberID", r.raft.cluster.member).
						Debugf("Received %d/%d pre-votes; transitioning to candidate", acceptCount, quorum)
					r.raft.becomeCandidate()
					return
				}
			} else {
				rejectCount++
				if rejectCount == quorum {
					log.WithField("memberID", r.raft.cluster.member).
						Debugf("Received %d/%d rejected pre-votes; resetting heartbeat timeout", rejectCount, quorum)
					r.resetHeartbeatTimeout()
					return
				}
			}
		}

		// If not enough votes were received, reset the heartbeat timeout.
		r.resetHeartbeatTimeout()
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
		Debugf("Polling members %v", votingMembers)

	// Once we got the last log term, iterate through each current member
	// of the cluster and vote each member for a vote.
	for _, member := range votingMembers {
		// Vote for yourself!
		if member == r.raft.cluster.member {
			votes <- true
			continue
		}

		go func(member string) {
			log.WithField("memberID", r.raft.cluster.member).
				Debugf("Polling %s for next term %d", member, r.raft.term+1)
			request := &PollRequest{
				Term:         r.raft.term,
				Candidate:    r.raft.cluster.member,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}

			client, err := r.raft.getClient(member)
			if err != nil {
				votes <- false
				log.WithField("memberID", r.raft.cluster.member).Warn(err)
			} else {
				response, err := client.Poll(context.Background(), request)
				if err != nil {
					votes <- false
					log.WithField("memberID", r.raft.cluster.member).Warn(err)
				} else {
					if response.Term > r.raft.term {
						r.raft.setTerm(response.Term)
					}

					if !response.Accepted {
						log.WithField("memberID", r.raft.cluster.member).
							Debugf("Received rejected poll from %s", member)
						votes <- false
					} else if response.Term != r.raft.term {
						log.WithField("memberID", r.raft.cluster.member).
							Debugf("Received accepted poll for a different term from %s", member)
						votes <- false
					} else {
						log.WithField("memberID", r.raft.cluster.member).
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
	response, err := r.ActiveRole.handleVote(ctx, request)
	if response.Voted {
		r.resetHeartbeatTimeout()
	}
	return response, err
}
