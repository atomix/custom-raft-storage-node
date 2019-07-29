package raft

import (
	"context"
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
	"time"
)

// PassiveRole implements a Raft follower
type PassiveRole struct {
	raft   *RaftServer
	active bool
}

// start starts the role
func (r *PassiveRole) start() error {
	r.active = true
	return nil
}

// stop stops the role
func (r *PassiveRole) stop() error {
	r.active = false
	return nil
}

func (r *PassiveRole) updateTermAndLeader(term int64, leader string) bool {
	// If the request indicates a term that is greater than the current term or no leader has been
	// set for the current term, update leader and term.
	if term > r.raft.term || (term == r.raft.term && r.raft.leader == "" && leader != "") {
		r.raft.setTerm(term);
		r.raft.setLeader(leader);
		return true
	}
	return false
}

func (r *PassiveRole) Join(ctx context.Context, request *JoinRequest) (*JoinResponse, error) {
	return &JoinResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}, nil
}

func (r *PassiveRole) Leave(ctx context.Context, request *LeaveRequest) (*LeaveResponse, error) {
	return &LeaveResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}, nil
}

func (r *PassiveRole) Configure(ctx context.Context, request *ConfigureRequest) (*ConfigureResponse, error) {
	return &ConfigureResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}, nil
}

func (r *PassiveRole) Reconfigure(ctx context.Context, request *ReconfigureRequest) (*ReconfigureResponse, error) {
	return &ReconfigureResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}, nil
}

func (r *PassiveRole) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	return &PollResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}, nil
}

func (r *PassiveRole) Vote(ctx context.Context, request *VoteRequest) (*VoteResponse, error) {
	return &VoteResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}, nil
}

func (r *PassiveRole) Transfer(ctx context.Context, request *TransferRequest) (*TransferResponse, error) {
	return &TransferResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
	}, nil
}

func (r *PassiveRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.updateTermAndLeader(request.Term, request.Leader)
	return r.handleAppend(ctx, request)
}

func (r *PassiveRole) handleAppend(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	if response := r.checkTerm(request); response != nil {
		return response, nil
	}

	if response := r.checkPreviousEntry(request); response != nil {
		return response, nil
	}
	return r.appendEntries(request)
}

func (r *PassiveRole) checkTerm(request *AppendRequest) *AppendResponse {
	if request.Term < r.raft.term {
		return r.failAppend(r.raft.log.Writer().LastIndex())
	}
	return nil
}

func (r *PassiveRole) checkPreviousEntry(request *AppendRequest) *AppendResponse {
	writer := r.raft.writer
	reader := r.raft.reader

	// If the previous term is set, validate that it matches the local log.
	// We check the previous log term since that indicates whether any entry is present in the leader's
	// log at the previous log index. It's possible that the leader can send a non-zero previous log index
	// with a zero term in the event the leader has compacted its logs and is sending the first entry.
	if request.PrevLogTerm != 0 {
		// Get the last entry written to the log.
		lastEntry := writer.LastEntry()

		// If the local log is non-empty...
		if lastEntry != nil {
			// If the previous log index is greater than the last entry index, fail the attempt.
			if request.PrevLogIndex > lastEntry.Index {
				return r.failAppend(lastEntry.Index)
			}

			// If the previous log index is less than the last written entry index, look up the entry.
			if request.PrevLogIndex < lastEntry.Index {
				// Reset the reader to the previous log index.
				if reader.NextIndex() != request.PrevLogIndex {
					reader.Reset(request.PrevLogIndex)
				}

				// The previous entry should exist in the log if we've gotten this far.
				previousEntry := reader.NextEntry()
				if previousEntry == nil {
					return r.failAppend(lastEntry.Index);
				}

				// Read the previous entry and validate that the term matches the request previous log term.
				if request.PrevLogTerm != previousEntry.Entry.Term {
					return r.failAppend(request.PrevLogIndex - 1)
				}
				// If the previous log term doesn't equal the last entry term, fail the append, sending the prior entry.
			} else if request.PrevLogTerm != lastEntry.Entry.Term {
				return r.failAppend(request.PrevLogIndex - 1)
			}
		} else {
			// If the previous log index is set and the last entry is null, fail the append.
			if request.PrevLogIndex > 0 {
				return r.failAppend(0)
			}
		}
	}
	return nil
}

func (r *PassiveRole) appendEntries(request *AppendRequest) (*AppendResponse, error) {
	// Compute the last entry index from the previous log index and request entry count.
	lastEntryIndex := request.PrevLogIndex + int64(len(request.Entries))

	// Ensure the commitIndex is not increased beyond the index of the last entry in the request.
	commitIndex := int64(math.Max(float64(r.raft.commitIndex), math.Min(float64(request.CommitIndex), float64(lastEntryIndex))))

	// Track the last log index while entries are appended.
	index := request.PrevLogIndex

	if len(request.Entries) > 0 {
		writer := r.raft.writer
		reader := r.raft.reader

		// If the previous term is zero, that indicates the previous index represents the beginning of the log.
		// Reset the log to the previous index plus one.
		if request.PrevLogTerm == 0 {
			writer.Reset(request.PrevLogIndex + 1)
		}

		// Iterate through entries and append them.
		for _, entry := range request.Entries {
			index++

			// Get the last entry written to the log by the writer.
			lastEntry := writer.LastEntry()

			if lastEntry != nil {
				// If the last written entry index is greater than the next append entry index,
				// we need to validate that the entry that's already in the log matches this entry.
				if lastEntry.Index > index {
					// Reset the reader to the current entry index.
					if reader.NextIndex() != index {
						reader.Reset(index)
					}

					// If the reader does not have any next entry, that indicates an inconsistency between the reader and writer.
					existingEntry := reader.NextEntry()
					if existingEntry == nil {
						return nil, errors.New("log reader inconsistent with writer")
					}

					// If the existing entry term doesn't match the leader's term for the same entry, truncate
					// the log and append the leader's entry.
					if existingEntry.Entry.Term != entry.Term {
						writer.Truncate(index - 1)
						writer.Append(entry)
					}
					// If the last written entry is equal to the append entry index, we don't need
					// to read the entry from disk and can just compare the last entry in the writer.
				} else if lastEntry.Index == index {
					// If the last entry term doesn't match the leader's term for the same entry, truncate
					// the log and append the leader's entry.
					if lastEntry.Entry.Term != entry.Term {
						writer.Truncate(index - 1)
						writer.Append(entry)
					}
					// Otherwise, this entry is being appended at the end of the log.
				} else {
					// If the last entry index isn't the previous index, throw an exception because something crazy happened!
					if lastEntry.Index != index-1 {
						return nil, errors.New(fmt.Sprintf("log writer inconsistent with next append entry index %d", index))
					}

					// Append the entry and log a message.
					writer.Append(entry)
				}
				// Otherwise, if the last entry is null just append the entry and log a message.
			} else {
				writer.Append(entry)
			}

			// If the index is less than the commit index, apply the entry
			if index <= commitIndex {
				r.raft.state.enqueueEntry(&IndexedEntry{
					Index: index,
					Entry: entry,
				}, nil)
			}
		}
	}

	// Update the context commit and global indices.
	r.raft.setCommitIndex(commitIndex)

	// Return a successful append response.
	return r.succeedAppend(index), nil
}

func (r *PassiveRole) failAppend(lastIndex int64) *AppendResponse {
	return r.completeAppend(false, lastIndex)
}

func (r *PassiveRole) succeedAppend(lastIndex int64) *AppendResponse {
	return r.completeAppend(true, lastIndex)
}

func (r *PassiveRole) completeAppend(succeeded bool, lastIndex int64) *AppendResponse {
	return &AppendResponse{
		Status:       ResponseStatus_OK,
		Term:         r.raft.term,
		Succeeded:    succeeded,
		LastLogIndex: lastIndex,
	}
}

func (r *PassiveRole) Install(stream RaftService_InstallServer) error {
	var writer io.WriteCloser
	for {
		request, err := stream.Recv()

		// If the stream has ended, close the writer and respond successfully.
		if err == io.EOF {
			writer.Close()
			return stream.SendAndClose(&InstallResponse{
				Status: ResponseStatus_OK,
			})
		}

		// If an error occurred, return the error.
		if err != nil {
			return err
		}

		// Update the term and leader
		r.updateTermAndLeader(request.Term, request.Leader)

		// If the request is for a lesser term, reject the request.
		if request.Term < r.raft.term {
			return stream.SendAndClose(&InstallResponse{
				Status: ResponseStatus_ERROR,
				Error:  RaftError_ILLEGAL_MEMBER_STATE,
			})
		}

		if writer == nil {
			snapshot := r.raft.snapshot.newSnapshot(request.Index, request.Timestamp)
			writer = snapshot.Writer()
		}

		if _, err := writer.Write(request.Data); err != nil {
			return stream.SendAndClose(&InstallResponse{
				Status: ResponseStatus_ERROR,
				Error:  RaftError_PROTOCOL_ERROR,
			})
		}
	}
}

func (r *PassiveRole) Command(request *CommandRequest, server RaftService_CommandServer) error {
	return server.Send(&CommandResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
		Leader: r.raft.leader,
		Term:   r.raft.term,
	})
}

func (r *PassiveRole) Query(request *QueryRequest, server RaftService_QueryServer) error {
	// If this server has not yet applied entries up to the client's session ID, forward the
	// query to the leader. This ensures that a follower does not tell the client its session
	// doesn't exist if the follower hasn't had a chance to see the session's registration entry.
	if r.raft.status != RaftStatusReady {
		log.Trace("State out of sync, forwarding query to leader");
		return r.forwardQuery(request, server)
	}

	// If the session's consistency level is SEQUENTIAL, handle the request here, otherwise forward it.
	if request.ReadConsistency == ReadConsistency_SEQUENTIAL {

		// If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
		// Forward the request to the leader.
		if r.raft.writer.LastIndex() < r.raft.commitIndex {
			log.Trace("State out of sync, forwarding query to leader");
			return r.forwardQuery(request, server)
		}
		return r.applyQuery(request, server);
	} else {
		return r.forwardQuery(request, server);
	}
}

func (r *PassiveRole) applyQuery(request *QueryRequest, server RaftService_QueryServer) error {
	entry := &IndexedEntry{
		Index: r.raft.writer.LastIndex(),
		Entry: &RaftLogEntry{
			Term:      r.raft.term,
			Timestamp: time.Now().UnixNano(),
			Entry: &RaftLogEntry_Query{
				Query: &QueryEntry{
					Value: request.Value,
				},
			},
		},
	}

	// Create a result channel
	ch := make(chan service.Output)

	// Apply the entry to the state machine
	r.raft.state.enqueueEntry(entry, ch)

	// Iterate through results and translate them into QueryResponses.
	for result := range ch {
		if result.Succeeded() {
			err := server.Send(&QueryResponse{
				Status: ResponseStatus_OK,
				Output: result.Value,
			})
			if err != nil {
				return err
			}
		} else {
			err := server.Send(&QueryResponse{
				Status:  ResponseStatus_ERROR,
				Message: result.Error.Error(),
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// forwardQuery forwards a query request to the leader
func (r *PassiveRole) forwardQuery(request *QueryRequest, server RaftService_QueryServer) error {
	leader := r.raft.leader
	if leader == "" {
		return server.Send(&QueryResponse{
			Status: ResponseStatus_ERROR,
			Error:  RaftError_NO_LEADER,
		})
	} else {
		log.Trace("Forwarding %v", request)
		client, err := r.raft.getClient(leader)
		if err != nil {
			return err
		}

		stream, err := client.Query(context.Background(), request)
		if err != nil {
			return err
		}

		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			server.Send(response)
		}
		return nil
	}
}
