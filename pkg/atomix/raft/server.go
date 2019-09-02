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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/cluster"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"strings"
	"sync"
	"time"
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

// NewServer returns a new Raft consensus protocol server
func NewServer(cluster cluster.Cluster, registry *service.Registry, electionTimeout time.Duration) *Server {
	log := newMemoryLog()
	reader := log.OpenReader(0)
	writer := log.Writer()
	server := &Server{
		cluster:         newCluster(cluster),
		snapshot:        newMemorySnapshotStore(),
		metadata:        newMemoryMetadataStore(),
		log:             log,
		reader:          reader,
		writer:          writer,
		electionTimeout: electionTimeout,
		status:          StatusStopped,
		readyCh:         make(chan struct{}, 1),
	}
	server.state = newStateManager(server, registry)
	return server
}

// Server implements the Raft consensus protocol server
type Server struct {
	RaftServiceServer
	server           *grpc.Server
	status           Status
	readyCh          chan struct{}
	role             Role
	state            *stateManager
	term             Term
	leader           MemberID
	lastVotedFor     *MemberID
	firstCommitIndex *Index
	commitIndex      Index
	cluster          *Cluster
	metadata         MetadataStore
	snapshot         SnapshotStore
	log              Log
	writer           LogWriter
	reader           LogReader
	electionTimeout  time.Duration
	mu               sync.RWMutex
}

// readLock acquires the server read lock
func (s *Server) readLock() {
	s.mu.RLock()
}

// readUnlock releases the server read lock
func (s *Server) readUnlock() {
	s.mu.RUnlock()
}

// writeLock acquires the server write lock
func (s *Server) writeLock() {
	s.mu.Lock()
}

// writeUnlock releases the server write lock
func (s *Server) writeUnlock() {
	s.mu.Unlock()
}

// setTerm sets the current term
func (s *Server) setTerm(term Term) {
	if term > s.term {
		s.term = term
		s.leader = ""
		s.lastVotedFor = nil
		s.metadata.StoreTerm(term)
		s.metadata.StoreVote(s.lastVotedFor)
	}
}

// setLeader sets the current leader
func (s *Server) setLeader(leader MemberID) {
	if s.leader != leader {
		if leader == "" {
			s.leader = ""
		} else {
			_, ok := s.cluster.members[leader]
			if ok {
				s.leader = leader
			}
		}

		s.lastVotedFor = nil
		s.metadata.StoreVote(s.lastVotedFor)
	}
}

// setLastVotedFor sets the last member the node voted for
func (s *Server) setLastVotedFor(candidate *MemberID) {
	// If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
	if s.lastVotedFor != nil && candidate != nil {
		log.WithField("memberID", s.cluster.member).Error("Already voted for another candidate")
	}

	// Verify the candidate is a member of the cluster.
	if candidate != nil {
		if _, ok := s.cluster.members[*candidate]; !ok {
			log.WithField("memberID", s.cluster.member).Errorf("Unknown candidate: %+v", candidate)
		}
	}

	s.lastVotedFor = candidate
	s.metadata.StoreVote(candidate)

	if candidate != nil {
		log.WithField("memberID", s.cluster.member).Debugf("Voted for %+v", candidate)
	} else {
		log.WithField("memberID", s.cluster.member).Trace("Reset last voted for")
	}
}

// setStatus sets the node's status
func (s *Server) setStatus(status Status) {
	if s.status != status {
		log.WithField("memberID", s.cluster.member).Infof("Server is %s", status)
		s.status = status
		if status == StatusReady && s.readyCh != nil {
			s.readyCh <- struct{}{}
		}
	}
}

// setFirstCommitIndex sets the first commit index learned by the node
func (s *Server) setFirstCommitIndex(commitIndex Index) {
	if s.firstCommitIndex == nil {
		s.firstCommitIndex = &commitIndex
	}
}

// setCommitIndex sets the current commit index
func (s *Server) setCommitIndex(commitIndex Index) Index {
	previousCommitIndex := s.commitIndex
	if commitIndex > previousCommitIndex {
		s.commitIndex = commitIndex
		s.setFirstCommitIndex(commitIndex)
	}
	if s.firstCommitIndex != nil && commitIndex >= *s.firstCommitIndex {
		s.setStatus(StatusReady)
	}
	return previousCommitIndex
}

// Start starts the Raft server
func (s *Server) Start() error {
	// Load the term and vote from stores.
	term := s.metadata.LoadTerm()
	if term != nil {
		s.term = *term
	}
	s.lastVotedFor = s.metadata.LoadVote()
	s.setStatus(StatusRunning)

	// Start applying changes to the state machine
	go s.state.start()

	// Transition the node to the follower role
	go s.becomeFollower()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.cluster.locations[s.cluster.member].Port))
	if err != nil {
		return err
	}

	s.server = grpc.NewServer()
	RegisterRaftServiceServer(s.server, s)
	return s.server.Serve(lis)
}

// WaitForReady blocks the current goroutine until the server is ready
func (s *Server) WaitForReady() error {
	_, ok := <-s.readyCh
	if ok {
		return nil
	}
	return errors.New("server stopped")
}

func (s *Server) logReceive(name string, message interface{}) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Received %s [%v]", name, message)
}

func (s *Server) logSend(name string, message interface{}) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Sending %s [%v]", name, message)
}

func (s *Server) logError(name string, err error) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Error %s [%v]", name, err)
}

func (s *Server) logSendTo(name string, message interface{}, member MemberID) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Sending %s [%v] to %s", name, message, member)
}

func (s *Server) logReceiveFrom(name string, message interface{}, member MemberID) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Received %s [%v] from %s", name, message, member)
}

func (s *Server) logErrorFrom(name string, err error, member MemberID) {
	log.WithField("memberID", s.cluster.member).
		Warnf("Error %s [%v] from %s", name, err, member)
}

func (s *Server) logRequest(name string, request interface{}) {
	s.logReceive(name, request)
}

func (s *Server) logResponse(name string, response interface{}, err error) error {
	if response != nil {
		s.logSend(name, response)
	}
	if err != nil {
		s.logError(name, err)
	}
	return err
}

// becomeFollower transitions the server's role to follower
func (s *Server) becomeFollower() {
	s.setRole(newFollowerRole(s))
}

// becomeCandidate transitions the server's role to candidate
func (s *Server) becomeCandidate() {
	s.setRole(newCandidateRole(s))
}

// becomeLeader transitions the server's role to candidate
func (s *Server) becomeLeader() {
	s.setRole(newLeaderRole(s))
}

func (s *Server) setRole(role Role) {
	s.writeLock()
	log.WithField("memberID", s.cluster.member).Infof("Transitioning to %s", role.Name())
	if s.role != nil {
		if err := s.role.stop(); err != nil {
			log.Error(err)
		}
	}
	s.role = role
	s.writeUnlock()
	if err := role.start(); err != nil {
		log.Error(err)
	}
}

func (s *Server) getRole() Role {
	s.readLock()
	defer s.readUnlock()
	return s.role
}

// Poll handles a poll request
func (s *Server) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	return s.getRole().Poll(ctx, request)
}

// Vote handles a vote request
func (s *Server) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	return s.getRole().Vote(ctx, request)
}

// Append handles an append request
func (s *Server) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	return s.getRole().Append(ctx, request)
}

// Install handles an install request
func (s *Server) Install(server RaftService_InstallServer) error {
	return s.getRole().Install(server)
}

// Command handles a client command request
func (s *Server) Command(request *CommandRequest, server RaftService_CommandServer) error {
	return s.getRole().Command(request, server)
}

// Query handles a client query request
func (s *Server) Query(request *QueryRequest, server RaftService_QueryServer) error {
	return s.getRole().Query(request, server)
}

// Stop shuts down the Raft server
func (s *Server) Stop() error {
	s.writeLock()
	defer s.writeUnlock()
	if s.server != nil {
		s.server.Stop()
	}
	if s.role != nil {
		s.role.stop()
	}
	close(s.readyCh)
	s.readyCh = nil
	return nil
}

type serverFormatter struct{}

func (f *serverFormatter) Format(entry *log.Entry) ([]byte, error) {
	buf := bytes.Buffer{}
	buf.Write([]byte(entry.Time.Format(time.StampMilli)))
	buf.Write([]byte(" "))
	buf.Write([]byte(fmt.Sprintf("%-6v", strings.ToUpper(entry.Level.String()))))
	buf.Write([]byte(" "))
	memberID := entry.Data["memberID"]
	if memberID == nil {
		memberID = ""
	}
	buf.Write([]byte(fmt.Sprintf("%-10v", memberID)))
	buf.Write([]byte(" "))
	buf.Write([]byte(entry.Message))
	buf.Write([]byte("\n"))
	return buf.Bytes(), nil
}

func init() {
	log.SetFormatter(&serverFormatter{})
}
