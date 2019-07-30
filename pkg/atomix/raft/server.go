package raft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"strings"
	"sync"
	"time"
)

type RaftStatus string

const (
	RaftStatusStopped RaftStatus = "stopped"
	RaftStatusRunning RaftStatus = "running"
	RaftStatusReady   RaftStatus = "ready"
)

// NewRaftServer returns a new Raft consensus protocol server
func NewRaftServer(cluster atomix.Cluster, registry *service.ServiceRegistry, electionTimeout time.Duration) *RaftServer {
	log := newMemoryLog()
	reader := log.OpenReader(0)
	writer := log.Writer()
	server := &RaftServer{
		cluster:         newRaftCluster(cluster),
		snapshot:        newMemorySnapshotStore(),
		metadata:        newMemoryMetadataStore(),
		log:             log,
		reader:          reader,
		writer:          writer,
		electionTimeout: electionTimeout,
		status:          RaftStatusStopped,
		readyCh:         make(chan struct{}, 1),
	}
	server.state = newStateManager(server, registry)
	return server
}

// RaftServer implements the Raft consensus protocol server
type RaftServer struct {
	RaftServiceServer
	server           *grpc.Server
	status           RaftStatus
	readyCh          chan struct{}
	role             Role
	state            *stateManager
	term             int64
	leader           string
	lastVotedFor     string
	firstCommitIndex *int64
	commitIndex      int64
	cluster          *RaftCluster
	metadata         MetadataStore
	snapshot         SnapshotStore
	log              RaftLog
	writer           RaftLogWriter
	reader           RaftLogReader
	electionTimeout  time.Duration
	mu               sync.RWMutex
}

func (s *RaftServer) readLock() {
	s.mu.RLock()
}

func (s *RaftServer) readUnlock() {
	s.mu.RUnlock()
}

func (s *RaftServer) writeLock() {
	s.mu.Lock()
}

func (s *RaftServer) writeUnlock() {
	s.mu.Unlock()
}

func (s *RaftServer) setTerm(term int64) {
	if term > s.term {
		s.term = term
		s.leader = ""
		s.lastVotedFor = ""
		s.metadata.StoreTerm(term)
		s.metadata.StoreVote(s.lastVotedFor)
	}
}

func (s *RaftServer) setLeader(leader string) {
	if s.leader != leader {
		if leader == "" {
			s.leader = ""
		} else {
			_, ok := s.cluster.members[leader]
			if ok {
				s.leader = leader
			}
		}

		s.lastVotedFor = ""
		s.metadata.StoreVote(s.lastVotedFor)
	}
}

func (s *RaftServer) setLastVotedFor(candidate string) {
	// If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
	if s.lastVotedFor != "" && candidate != "" {
		log.WithField("memberID", s.cluster.member).Error("Already voted for another candidate")
	}

	// Verify the candidate is a member of the cluster.
	if _, ok := s.cluster.members[candidate]; !ok {
		log.WithField("memberID", s.cluster.member).Errorf("Unknown candidate: %s", candidate)
	}

	s.lastVotedFor = candidate
	s.metadata.StoreVote(candidate)

	if candidate != "" {
		log.WithField("memberID", s.cluster.member).Debugf("Voted for %s", candidate)
	} else {
		log.WithField("memberID", s.cluster.member).Trace("Reset last voted for")
	}
}

func (s *RaftServer) setStatus(status RaftStatus) {
	if s.status != status {
		log.WithField("memberID", s.cluster.member).Infof("Server is %s", status)
		s.status = status
		if status == RaftStatusReady {
			s.readyCh <- struct{}{}
		}
	}
}

func (s *RaftServer) setFirstCommitIndex(commitIndex int64) {
	if s.firstCommitIndex == nil {
		s.firstCommitIndex = &commitIndex
	}
}

func (s *RaftServer) setCommitIndex(commitIndex int64) int64 {
	previousCommitIndex := s.commitIndex
	if commitIndex > previousCommitIndex {
		s.commitIndex = commitIndex
		s.setFirstCommitIndex(commitIndex)
	}
	if s.firstCommitIndex != nil && commitIndex >= *s.firstCommitIndex {
		s.setStatus(RaftStatusReady)
	}
	return previousCommitIndex
}

// Start starts the Raft server
func (s *RaftServer) Start() error {
	// Load the term and vote from stores.
	term := s.metadata.LoadTerm()
	if term != nil {
		s.term = *term
	}
	vote := s.metadata.LoadVote()
	if vote != nil {
		s.lastVotedFor = *vote
	}
	s.setStatus(RaftStatusRunning)

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

// waitForReady blocks the current goroutine until the server is ready
func (s *RaftServer) waitForReady() error {
	_, ok := <-s.readyCh
	if ok {
		return nil
	} else {
		return errors.New("server stopped")
	}
}

func (s *RaftServer) logReceive(name string, message interface{}) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Received %s [%v]", name, message)
}

func (s *RaftServer) logSend(name string, message interface{}) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Sending %s [%v]", name, message)
}

func (s *RaftServer) logError(name string, err error) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Error %s [%v]", name, err)
}

func (s *RaftServer) logSendTo(name string, message interface{}, member string) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Sending %s [%v] to %s", name, message, member)
}

func (s *RaftServer) logReceiveFrom(name string, message interface{}, member string) {
	log.WithField("memberID", s.cluster.member).
		Tracef("Received %s [%v] from %s", name, message, member)
}

func (s *RaftServer) logErrorFrom(name string, err error, member string) {
	log.WithField("memberID", s.cluster.member).
		Warnf("Error %s [%v] from %s", name, err, member)
}

func (s *RaftServer) logRequest(name string, request interface{}) {
	s.logReceive(name, request)
}

func (s *RaftServer) logResponse(name string, response interface{}, err error) error {
	if response != nil {
		s.logSend(name, response)
	}
	if err != nil {
		s.logError(name, err)
	}
	return err
}

// becomeFollower transitions the server's role to follower
func (s *RaftServer) becomeFollower() error {
	return s.setRole(newFollowerRole(s))
}

// becomeCandidate transitions the server's role to candidate
func (s *RaftServer) becomeCandidate() error {
	return s.setRole(newCandidateRole(s))
}

// becomeLeader transitions the server's role to candidate
func (s *RaftServer) becomeLeader() error {
	return s.setRole(newLeaderRole(s))
}

func (s *RaftServer) setRole(role Role) error {
	s.writeLock()
	log.WithField("memberID", s.cluster.member).Infof("Transitioning to %s", role.Name())
	if s.role != nil {
		s.role.stop()
	}
	s.role = role
	s.writeUnlock()
	return role.start()
}

func (s *RaftServer) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	return s.role.Poll(ctx, request)
}

func (s *RaftServer) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	return s.role.Vote(ctx, request)
}

func (s *RaftServer) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	return s.role.Append(ctx, request)
}

func (s *RaftServer) Install(server RaftService_InstallServer) error {
	return s.role.Install(server)
}

func (s *RaftServer) Command(request *CommandRequest, server RaftService_CommandServer) error {
	return s.role.Command(request, server)
}

func (s *RaftServer) Query(request *QueryRequest, server RaftService_QueryServer) error {
	return s.role.Query(request, server)
}

// Stop shuts down the Raft server
func (s *RaftServer) Stop() error {
	s.state.stop()
	s.server.Stop()
	close(s.readyCh)
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
