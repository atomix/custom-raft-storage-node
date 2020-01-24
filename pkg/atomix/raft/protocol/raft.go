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
	"fmt"
	"github.com/atomix/raft-replica/pkg/atomix/raft/config"
	"github.com/atomix/raft-replica/pkg/atomix/raft/util"
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

// NewRaft returns a new Raft protocol state struct
func NewRaft(cluster Cluster, config *config.ProtocolConfig, protocol Client, roles map[RoleType]func(Raft) Role) Raft {
	return newRaft(cluster, config, protocol, roles, newMemoryMetadataStore())
}

// newRaft returns a new Raft protocol state struct
func newRaft(cluster Cluster, config *config.ProtocolConfig, protocol Client, roles map[RoleType]func(Raft) Role, store MetadataStore) Raft {
	return &raft{
		log:      util.NewNodeLogger(string(cluster.Member())),
		config:   config,
		protocol: protocol,
		status:   StatusStopped,
		watchers: make([]func(Event), 0),
		roles:    roles,
		cluster:  cluster,
		metadata: store,
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
	Server

	// Init initializes the Raft state
	Init()

	// Watch watches the Raft protocol state for changes
	Watch(func(Event))

	// Role is the current role
	Role() RoleType

	// Status returns the Raft protocol status
	Status() Status

	// Config returns the Raft protocol configuration
	Config() *config.ProtocolConfig

	// Member returns the local member ID
	Member() MemberID

	// Members returns a list of all members in the Raft cluster
	Members() []MemberID

	// GetMember returns a RaftMember by ID
	GetMember(memberID MemberID) *Member

	// Client returns the Raft messaging protocol
	Protocol() Client

	// Term returns the current term
	Term() Term

	// SetTerm sets the current term
	SetTerm(term Term) error

	// Leader returns the current leader
	Leader() *MemberID

	// SetLeader sets the current leader
	SetLeader(leader *MemberID) error

	// LastVotedFor returns the last member voted for by this node
	LastVotedFor() *MemberID

	// SetLastVotedFor sets the last member voted for by this node
	SetLastVotedFor(memberID MemberID) error

	// CommitIndex returns the current commit index
	CommitIndex() Index

	// SetCommitIndex sets the highest known commit index
	SetCommitIndex(index Index)

	// Commit sets the persisted commit index
	Commit(index Index) Index

	// WriteLock acquires a write lock on the state
	WriteLock()

	// WriteUnlock releases a write lock on the state
	WriteUnlock()

	// ReadLock acquires a read lock on the state
	ReadLock()

	// ReadUnlock releases a read lock on the state
	ReadUnlock()

	// SetRole sets the protocol's current role
	SetRole(role RoleType)

	// Close closes the Raft state
	Close() error
}

// Event is a Raft protocol state change event
type Event struct {
	Type   EventType
	Status Status
	Role   RoleType
	Term   Term
	Leader *MemberID
}

// EventType is a Raft protocol state change event type
type EventType string

const (
	// EventTypeStatus is a status change event
	EventTypeStatus EventType = "Status"

	// EventTypeRole is a role change event
	EventTypeRole EventType = "Role"

	// EventTypeTerm is a term change event
	EventTypeTerm EventType = "Term"

	// EventTypeLeader is a leader change event
	EventTypeLeader EventType = "Leader"
)

// RoleType is the name of a role
type RoleType string

const (
	// RoleFollower is a Raft follower role
	RoleFollower RoleType = "Follower"

	// RoleCandidate is a Raft candidate role
	RoleCandidate RoleType = "Candidate"

	// RoleLeader is a Raft leader role
	RoleLeader RoleType = "Leader"
)

// Role is implemented by server roles to support protocol requests
type Role interface {
	Server

	// Type is the type of the role
	Type() RoleType

	// Start initializes the role
	// The Start method will always be called with a write lock on the Raft object
	Start() error

	// Stop stops the role
	// The Stop method will always be called with a write lock on the Raft object
	Stop() error
}

// raft is the default implementation of the Raft protocol state
type raft struct {
	log              util.Logger
	status           Status
	config           *config.ProtocolConfig
	protocol         Client
	metadata         MetadataStore
	watchers         []func(Event)
	roles            map[RoleType]func(Raft) Role
	role             Role
	term             Term
	leader           *MemberID
	lastVotedFor     *MemberID
	firstCommitIndex *Index
	commitIndex      Index
	cluster          Cluster
	mu               sync.RWMutex
}

func (r *raft) Init() {
	term := r.metadata.LoadTerm()
	if term != nil {
		r.term = *term
	}
	r.lastVotedFor = r.metadata.LoadVote()
	r.setStatus(StatusRunning)
	r.SetRole(RoleFollower)
}

func (r *raft) Watch(watcher func(Event)) {
	r.watchers = append(r.watchers, watcher)
}

func (r *raft) notify(eventType EventType) {
	event := Event{
		Type:   eventType,
		Status: r.status,
		Role:   r.Role(),
		Term:   r.term,
		Leader: r.leader,
	}
	for _, watcher := range r.watchers {
		watcher(event)
	}
}

func (r *raft) Status() Status {
	return r.status
}

// setStatus sets the node's status
func (r *raft) setStatus(status Status) {
	if r.status != status {
		r.log.Info("Server is %s", status)
		r.status = status
		r.notify(EventTypeStatus)
	}
}

func (r *raft) Config() *config.ProtocolConfig {
	return r.config
}

func (r *raft) Protocol() Client {
	return r.protocol
}

func (r *raft) Member() MemberID {
	return r.cluster.Member()
}

func (r *raft) Members() []MemberID {
	return r.cluster.Members()
}

func (r *raft) GetMember(memberID MemberID) *Member {
	return r.cluster.GetMember(memberID)
}

func (r *raft) Connect(memberID MemberID) (RaftServiceClient, error) {
	return r.cluster.GetClient(memberID)
}

func (r *raft) Term() Term {
	return r.term
}

func (r *raft) SetTerm(term Term) error {
	if term < r.term {
		return fmt.Errorf("cannot decrease term %d to %d", r.term, term)
	} else if term > r.term {
		r.term = term
		r.leader = nil
		r.lastVotedFor = nil
		r.metadata.StoreTerm(term)
		r.metadata.StoreVote(r.lastVotedFor)
		r.notify(EventTypeTerm)
	}
	return nil
}

func (r *raft) Leader() *MemberID {
	return r.leader
}

func (r *raft) SetLeader(leader *MemberID) error {
	if r.leader == nil && leader != nil {
		// If the leader is being set for the first time, verify it's a member of the cluster configuration
		if r.GetMember(*leader) != nil {
			r.leader = leader
			r.notify(EventTypeLeader)
		} else {
			return fmt.Errorf("unknown member %+v", leader)
		}
	} else if r.leader != nil && leader == nil {
		r.leader = nil
		r.notify(EventTypeLeader)
	} else if r.leader != nil && leader != nil && r.leader != leader {
		return fmt.Errorf("cannot change leader %+v to %+v", r.leader, leader)
	}
	return nil
}

func (r *raft) LastVotedFor() *MemberID {
	return r.lastVotedFor
}

func (r *raft) SetLastVotedFor(memberID MemberID) error {
	// If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
	if r.lastVotedFor != nil && *r.lastVotedFor != memberID {
		return fmt.Errorf("already voted for %+v", r.lastVotedFor)
	}

	// Verify the candidate is a member of the cluster.
	if r.GetMember(memberID) == nil {
		return fmt.Errorf("unknown candidate %s", memberID)
	}

	r.lastVotedFor = &memberID
	r.metadata.StoreVote(&memberID)
	r.log.Debug("Voted for %+v", memberID)
	return nil
}

func (r *raft) CommitIndex() Index {
	return r.commitIndex
}

func (r *raft) SetCommitIndex(index Index) {
	if r.firstCommitIndex == nil {
		r.firstCommitIndex = &index
	}
}

func (r *raft) Commit(index Index) Index {
	prevIndex := r.commitIndex
	if index > prevIndex {
		r.commitIndex = index
		if r.firstCommitIndex != nil && index >= *r.firstCommitIndex {
			r.setStatus(StatusReady)
		}
	}
	return prevIndex
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

func (r *raft) Role() RoleType {
	if r.role == nil {
		return ""
	}
	return r.role.Type()
}

func (r *raft) SetRole(roleType RoleType) {
	// Get the role factory function
	roleFunc, ok := r.roles[roleType]
	if !ok {
		r.log.Error("Unknown role type %s", roleType)
		return
	}

	// If the role has not changed, ignore the call
	if r.role != nil && r.role.Type() == roleType {
		return
	}

	// Stop the current role if set
	r.log.Info("Transitioning to %s", roleType)
	if r.role != nil {
		if err := r.role.Stop(); err != nil {
			r.log.Error("Failed to stop %s role", r.role.Type(), err)
		}
	}

	// Create and start the new role
	role := roleFunc(r)
	r.role = role
	if err := role.Start(); err != nil {
		r.log.Error("Failed to start %s role", role.Type(), err)
	}

	// Notify watchers
	r.notify(EventTypeRole)
}

func (r *raft) getRole() Role {
	r.ReadLock()
	defer r.ReadUnlock()
	return r.role
}

func (r *raft) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	return r.getRole().Poll(ctx, request)
}

func (r *raft) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	return r.getRole().Vote(ctx, request)
}

func (r *raft) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	return r.getRole().Append(ctx, request)
}

func (r *raft) Install(ch <-chan *InstallStreamRequest) (*InstallResponse, error) {
	return r.getRole().Install(ch)
}

func (r *raft) Command(request *CommandRequest, ch chan<- *CommandStreamResponse) error {
	return r.getRole().Command(request, ch)
}

func (r *raft) Query(request *QueryRequest, ch chan<- *QueryStreamResponse) error {
	return r.getRole().Query(request, ch)
}

func (r *raft) Join(ctx context.Context, request *JoinRequest) (*JoinResponse, error) {
	return r.getRole().Join(ctx, request)
}

func (r *raft) Leave(ctx context.Context, request *LeaveRequest) (*LeaveResponse, error) {
	return r.getRole().Leave(ctx, request)
}

func (r *raft) Configure(ctx context.Context, request *ConfigureRequest) (*ConfigureResponse, error) {
	return r.getRole().Configure(ctx, request)
}

func (r *raft) Reconfigure(ctx context.Context, request *ReconfigureRequest) (*ReconfigureResponse, error) {
	return r.getRole().Reconfigure(ctx, request)
}

func (r *raft) Transfer(ctx context.Context, request *TransferRequest) (*TransferResponse, error) {
	return r.getRole().Transfer(ctx, request)
}

func (r *raft) Close() error {
	r.setStatus(StatusStopped)
	return r.metadata.Close()
}
