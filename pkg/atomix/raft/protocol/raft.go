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

package protocol

import (
	"context"
	"errors"
	atomix "github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/config"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/util"
	"sync"
)

// Status represents the status of a Raft server
type Status string

const (
	// StatusStopped indicates the server is not running
	StatusStopped Status = "stopped"

	// StatusRunning indicates the server is running but has not found a leader
	StatusRunning Status = "running"

	// StatusReady indicates the server is running, has found a leader, and has applied committed entries to its state machine
	StatusReady Status = "ready"
)

// NewProtocol returns a new Raft protocol state struct
func NewProtocol(cluster atomix.Cluster, config *config.ProtocolConfig) Raft {
	return &raft{
		log:       util.NewNodeLogger(string(cluster.MemberID)),
		config:    config,
		status:    StatusStopped,
		listeners: make([]func(Status), 0, 1),
		cluster:   NewCluster(cluster),
		metadata:  newMemoryMetadataStore(),
	}
}

// MemberID is the ID of a Raft cluster member
type MemberID string

// Index is a Raft log index
type Index uint64

// Term is a Raft term
type Term uint64

// Raft is an interface for managing the state of the Raft consensus protocol
type Raft interface {
	RaftServiceServer

	// Status returns the Raft protocol status
	Status() Status

	// WatchStatus watches the protocol status for changes
	WatchStatus(func(Status))

	// Config returns the Raft protocol configuration
	Config() *config.ProtocolConfig

	// Member returns the local member ID
	Member() MemberID

	// Members returns a list of all members in the Raft cluster
	Members() []MemberID

	// GetMember returns a RaftMember by ID
	GetMember(memberID MemberID) *RaftMember

	// Connect gets a client for the given member
	Connect(memberID MemberID) (RaftServiceClient, error)

	// Term returns the current term
	Term() Term

	// SetTerm sets the current term
	SetTerm(term Term)

	// Leader returns the current leader
	Leader() MemberID

	// SetLeader sets the current leader
	SetLeader(leader MemberID)

	// LastVotedFor returns the last member voted for by this node
	LastVotedFor() *MemberID

	// SetLastVotedFor sets the last member voted for by this node
	SetLastVotedFor(memberID *MemberID)

	// CommitIndex returns the current commit index
	CommitIndex() Index

	// SetCommitIndex sets the current and persisted commit indexes and returns the previous commit index
	SetCommitIndex(commitIndex, storedIndex Index) Index

	// WriteLock acquires a write lock on the state
	WriteLock()

	// WriteUnlock releases a write lock on the state
	WriteUnlock()

	// ReadLock acquires a read lock on the state
	ReadLock()

	// ReadUnlock releases a read lock on the state
	ReadUnlock()

	// SetRole sets the protocol's current role
	SetRole(role Role)

	// Close closes the Raft state
	Close() error
}

// Role is implemented by server roles to support protocol requests
type Role interface {
	RaftServiceServer

	// Name is the name of the role
	Name() string

	// Start initializes the role
	Start() error

	// Stop stops the role
	Stop() error
}

// raft is the default implementation of the Raft protocol state
type raft struct {
	log              util.Logger
	status           Status
	config           *config.ProtocolConfig
	metadata         MetadataStore
	listeners        []func(Status)
	role             Role
	term             Term
	leader           MemberID
	lastVotedFor     *MemberID
	firstCommitIndex *Index
	commitIndex      Index
	cluster          Cluster
	mu               sync.RWMutex
}

func (r *raft) Status() Status {
	return r.status
}

func (r *raft) WatchStatus(f func(Status)) {
	r.listeners = append(r.listeners, f)
}

// setStatus sets the node's status
func (r *raft) setStatus(status Status) {
	if r.status != status {
		r.log.Info("Server is %s", status)
		r.status = status
		for _, listener := range r.listeners {
			listener(status)
		}
	}
}

func (r *raft) Config() *config.ProtocolConfig {
	return r.config
}

func (r *raft) Member() MemberID {
	return r.cluster.Member()
}

func (r *raft) Members() []MemberID {
	return r.cluster.Members()
}

func (r *raft) GetMember(memberID MemberID) *RaftMember {
	return r.cluster.GetMember(memberID)
}

func (r *raft) Connect(memberID MemberID) (RaftServiceClient, error) {
	return r.cluster.GetClient(memberID)
}

func (r *raft) Term() Term {
	return r.term
}

func (r *raft) SetTerm(term Term) {
	r.term = term
	r.leader = ""
	r.lastVotedFor = nil
	r.metadata.StoreTerm(term)
	r.metadata.StoreVote(r.lastVotedFor)
}

func (r *raft) Leader() MemberID {
	return r.leader
}

func (r *raft) SetLeader(leader MemberID) {
	if r.leader != leader {
		if leader == "" {
			r.leader = ""
		} else {
			if r.GetMember(leader) != nil {
				r.leader = leader
			}
		}

		r.lastVotedFor = nil
		r.metadata.StoreVote(r.lastVotedFor)
	}
}

func (r *raft) LastVotedFor() *MemberID {
	return r.lastVotedFor
}

func (r *raft) SetLastVotedFor(memberID *MemberID) {
	// If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
	if r.lastVotedFor != nil && memberID != nil {
		r.log.Error("Already voted for another candidate")
	}

	// Verify the candidate is a member of the cluster.
	if memberID != nil {
		if r.GetMember(*memberID) == nil {
			r.log.Error("Unknown candidate: %+v", memberID)
		}
	}

	r.lastVotedFor = memberID
	r.metadata.StoreVote(memberID)

	if memberID != nil {
		r.log.Debug("Voted for %+v", memberID)
	} else {
		r.log.Trace("Reset last voted for")
	}
}

func (r *raft) CommitIndex() Index {
	return r.commitIndex
}

func (r *raft) SetCommitIndex(commitIndex, storedIndex Index) Index {
	previousCommitIndex := r.commitIndex
	if commitIndex > previousCommitIndex {
		r.commitIndex = storedIndex
		r.setFirstCommitIndex(commitIndex)
	}
	if r.firstCommitIndex != nil && storedIndex >= *r.firstCommitIndex {
		r.setStatus(StatusReady)
	}
	return previousCommitIndex
}

// setFirstCommitIndex sets the first commit index learned by the node
func (r *raft) setFirstCommitIndex(commitIndex Index) {
	if r.firstCommitIndex == nil {
		r.firstCommitIndex = &commitIndex
	}
}

func (r *raft) WriteLock() {
	r.mu.Lock()
}

func (r *raft) WriteUnlock() {
	r.mu.Unlock()
}

func (r *raft) ReadLock() {
	r.mu.RLock()
}

func (r *raft) ReadUnlock() {
	r.mu.RUnlock()
}

func (r *raft) SetRole(role Role) {
	r.WriteLock()
	if r.status == StatusStopped {
		r.setStatus(StatusRunning)
	}
	r.log.Info("Transitioning to %s", role.Name())
	if r.role != nil {
		if err := r.role.Stop(); err != nil {
			r.log.Error("Failed to stop %s role", r.role.Name(), err)
		}
	}
	r.role = role
	r.WriteUnlock()
	if err := role.Start(); err != nil {
		r.log.Error("Failed to start %s role", role.Name(), err)
	}
}

func (r *raft) getRole() Role {
	r.ReadLock()
	defer r.ReadUnlock()
	return r.role
}

// Poll handles a poll request
func (r *raft) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	return r.getRole().Poll(ctx, request)
}

// Vote handles a vote request
func (r *raft) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	return r.getRole().Vote(ctx, request)
}

// Append handles an append request
func (r *raft) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	return r.getRole().Append(ctx, request)
}

// Install handles an install request
func (r *raft) Install(server RaftService_InstallServer) error {
	return r.getRole().Install(server)
}

// Command handles a client command request
func (r *raft) Command(request *CommandRequest, server RaftService_CommandServer) error {
	return r.getRole().Command(request, server)
}

// Query handles a client query request
func (r *raft) Query(request *QueryRequest, server RaftService_QueryServer) error {
	return r.getRole().Query(request, server)
}

func (r *raft) Join(context.Context, *JoinRequest) (*JoinResponse, error) {
	return nil, errors.New("not supported")
}

func (r *raft) Leave(context.Context, *LeaveRequest) (*LeaveResponse, error) {
	return nil, errors.New("not supported")
}

func (r *raft) Configure(context.Context, *ConfigureRequest) (*ConfigureResponse, error) {
	return nil, errors.New("not supported")
}

func (r *raft) Reconfigure(context.Context, *ReconfigureRequest) (*ReconfigureResponse, error) {
	return nil, errors.New("not supported")
}

func (r *raft) Transfer(context.Context, *TransferRequest) (*TransferResponse, error) {
	return nil, errors.New("not supported")
}

func (r *raft) Close() error {
	return nil
}
