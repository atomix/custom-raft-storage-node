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
	"errors"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix/service"
	log "github.com/sirupsen/logrus"
	"io"
	"math"
	"time"
)

func newPassiveRole(server *RaftServer) *PassiveRole {
	return &PassiveRole{
		raftRole: newRaftRole(server),
	}
}

// PassiveRole implements a Raft follower
type PassiveRole struct {
	*raftRole
}

// Name is the name of the role
func (r *PassiveRole) Name() string {
	return "Passive"
}

func (r *PassiveRole) updateTermAndLeader(term Term, leader MemberID) bool {
	// If the request indicates a term that is greater than the current term or no leader has been
	// set for the current term, update leader and term.
	if term > r.server.term || (term == r.server.term && r.server.leader == "" && leader != "") {
		r.server.setTerm(term)
		r.server.setLeader(leader)
		return true
	}
	return false
}

func (r *PassiveRole) Append(ctx context.Context, request *AppendRequest) (*AppendResponse, error) {
	r.server.logRequest("AppendRequest", request)
	r.server.writeLock()
	defer r.server.writeUnlock()
	r.updateTermAndLeader(request.Term, request.Leader)
	response, err := r.handleAppend(ctx, request)
	r.server.logResponse("AppendResponse", response, err)
	return response, err
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
	if request.Term < r.server.term {
		log.WithField("memberID", r.server.cluster.member).
			Debugf("Rejected %v: request term is less than the current term (%d)", request, r.server.term)
		return r.failAppend(r.server.log.Writer().LastIndex())
	}
	return nil
}

func (r *PassiveRole) checkPreviousEntry(request *AppendRequest) *AppendResponse {
	writer := r.server.writer
	reader := r.server.reader

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
				log.WithField("memberID", r.server.cluster.member).
					Debugf("Rejected %v: Previous index (%d) is greater than the local log's last index (%d)", request, request.PrevLogIndex, lastEntry.Index)
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
					log.WithField("memberID", r.server.cluster.member).
						Debugf("Rejected %v: Previous entry does not exist in the local log", request)
					return r.failAppend(lastEntry.Index)
				}

				// Read the previous entry and validate that the term matches the request previous log term.
				if request.PrevLogTerm != previousEntry.Entry.Term {
					log.WithField("memberID", r.server.cluster.member).
						Debugf("Rejected %v: Previous entry term (%d) does not match local log's term for the same entry (%d)", request, request.PrevLogTerm, previousEntry.Entry.Term)
					return r.failAppend(request.PrevLogIndex - 1)
				}
				// If the previous log term doesn't equal the last entry term, fail the append, sending the prior entry.
			} else if request.PrevLogTerm != lastEntry.Entry.Term {
				log.WithField("memberID", r.server.cluster.member).
					Debugf("Rejected %v: Previous entry term (%d) does not equal the local log's last term (%d)", request, request.PrevLogTerm, lastEntry.Entry.Term)
				return r.failAppend(request.PrevLogIndex - 1)
			}
		} else {
			// If the previous log index is set and the last entry is null, fail the append.
			if request.PrevLogIndex > 0 {
				log.WithField("memberID", r.server.cluster.member).
					Debugf("Rejected %v: Previous index (%d) is greater than the local log's last index (0)", request, request.PrevLogIndex)
				return r.failAppend(0)
			}
		}
	}
	return nil
}

func (r *PassiveRole) appendEntries(request *AppendRequest) (*AppendResponse, error) {
	// Compute the last entry index from the previous log index and request entry count.
	lastEntryIndex := request.PrevLogIndex + Index(len(request.Entries))

	// Ensure the commitIndex is not increased beyond the index of the last entry in the request.
	commitIndex := Index(math.Max(float64(r.server.commitIndex), math.Min(float64(request.CommitIndex), float64(lastEntryIndex))))

	// Track the last log index while entries are appended.
	index := request.PrevLogIndex

	// Set the first commit index if necessary.
	r.server.setFirstCommitIndex(request.CommitIndex)
	prevCommitIndex := r.server.commitIndex

	if len(request.Entries) > 0 {
		writer := r.server.writer
		reader := r.server.reader

		// If the previous term is zero, that indicates the previous index represents the beginning of the log.
		// Reset the log to the previous index plus one.
		if request.PrevLogTerm == 0 {
			log.WithField("memberID", r.server.cluster.member).
				Debugf("Reset first index to %d", request.PrevLogIndex+1)
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
						indexed := writer.Append(entry)
						log.WithField("memberID", r.server.cluster.member).
							Tracef("Appended %v", indexed)
					}
					// Otherwise, this entry is being appended at the end of the log.
				} else {
					// If the last entry index isn't the previous index, throw an exception because something crazy happened!
					if lastEntry.Index != index-1 {
						return nil, errors.New(fmt.Sprintf("log writer inconsistent with next append entry index %d", index))
					}

					// Append the entry and log a message.
					indexed := writer.Append(entry)
					log.WithField("memberID", r.server.cluster.member).
						Tracef("Appended %v", indexed)
				}
				// Otherwise, if the last entry is null just append the entry and log a message.
			} else {
				indexed := writer.Append(entry)
				log.WithField("memberID", r.server.cluster.member).
					Tracef("Appended %v", indexed)
			}
		}
	}

	// Update the context commit and global indices.
	if commitIndex > prevCommitIndex {
		r.server.setCommitIndex(commitIndex)
		log.WithField("memberID", r.server.cluster.member).
			Tracef("Committed entries up to index %d", commitIndex)

		// If the commitIndex was for entries that were replicated in a prior request, ensure they're applied
		// to the state machine.
		if commitIndex <= request.PrevLogIndex {
			r.server.state.applyIndex(request.PrevLogIndex)
		}

		// Iterate through entries in the request and apply committed entries to the state machine.
		for i := 0; request.PrevLogIndex+Index(i)+1 <= commitIndex && i < len(request.Entries); i++ {
			r.server.state.applyEntry(&IndexedEntry{
				Index: request.PrevLogIndex + Index(i) + 1,
				Entry: request.Entries[i],
			}, nil)
		}
	}

	// Return a successful append response.
	return r.succeedAppend(index), nil
}

func (r *PassiveRole) failAppend(lastIndex Index) *AppendResponse {
	return r.completeAppend(false, lastIndex)
}

func (r *PassiveRole) succeedAppend(lastIndex Index) *AppendResponse {
	return r.completeAppend(true, lastIndex)
}

func (r *PassiveRole) completeAppend(succeeded bool, lastIndex Index) *AppendResponse {
	return &AppendResponse{
		Status:       ResponseStatus_OK,
		Term:         r.server.term,
		Succeeded:    succeeded,
		LastLogIndex: lastIndex,
	}
}

func (r *PassiveRole) Install(stream RaftService_InstallServer) error {
	var writer io.WriteCloser
	for {
		request, err := stream.Recv()
		r.server.logRequest("InstallRequest", request)

		// If the stream has ended, close the writer and respond successfully.
		if err == io.EOF {
			writer.Close()
			response := &InstallResponse{
				Status: ResponseStatus_OK,
			}
			return r.server.logResponse("InstallResponse", response, stream.SendAndClose(response))
		}

		// If an error occurred, return the error.
		if err != nil {
			return r.server.logResponse("InstallResponse", nil, err)
		}

		// Acquire a write lock to write the snapshot.
		r.server.writeLock()

		// Update the term and leader
		r.updateTermAndLeader(request.Term, request.Leader)

		// If the request is for a lesser term, reject the request.
		if request.Term < r.server.term {
			response := &InstallResponse{
				Status: ResponseStatus_ERROR,
				Error:  RaftError_ILLEGAL_MEMBER_STATE,
			}
			return r.server.logResponse("InstallResponse", response, stream.SendAndClose(response))
		}

		if writer == nil {
			snapshot := r.server.snapshot.newSnapshot(request.Index, request.Timestamp)
			writer = snapshot.Writer()
		}

		_, err = writer.Write(request.Data)
		r.server.writeUnlock()
		if err != nil {
			response := &InstallResponse{
				Status: ResponseStatus_ERROR,
				Error:  RaftError_PROTOCOL_ERROR,
			}
			return r.server.logResponse("InstallResponse", response, stream.SendAndClose(response))
		}
	}
}

func (r *PassiveRole) Command(request *CommandRequest, server RaftService_CommandServer) error {
	r.server.logRequest("CommandRequest", request)
	r.server.readLock()
	response := &CommandResponse{
		Status: ResponseStatus_ERROR,
		Error:  RaftError_ILLEGAL_MEMBER_STATE,
		Leader: r.server.leader,
		Term:   r.server.term,
	}
	r.server.readUnlock()
	return r.server.logResponse("CommandResponse", response, server.Send(response))
}

func (r *PassiveRole) Query(request *QueryRequest, server RaftService_QueryServer) error {
	r.server.logRequest("QueryRequest", request)
	r.server.readLock()
	leader := r.server.leader

	// If this server has not yet applied entries up to the client's session ID, forward the
	// query to the leader. This ensures that a follower does not tell the client its session
	// doesn't exist if the follower hasn't had a chance to see the session's registration entry.
	if r.server.status != RaftStatusReady {
		r.server.readUnlock()
		log.WithField("memberID", r.server.cluster.member).
			Tracef("State out of sync, forwarding query to leader")
		return r.forwardQuery(request, leader, server)
	}

	// If the session's consistency level is SEQUENTIAL, handle the request here, otherwise forward it.
	if request.ReadConsistency == ReadConsistency_SEQUENTIAL {
		// If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
		// Forward the request to the leader.
		if r.server.writer.LastIndex() < r.server.commitIndex {
			r.server.readUnlock()
			log.WithField("memberID", r.server.cluster.member).
				Tracef("State out of sync, forwarding query to leader")
			return r.forwardQuery(request, leader, server)
		}

		entry := &IndexedEntry{
			Index: r.server.writer.LastIndex(),
			Entry: &RaftLogEntry{
				Term:      r.server.term,
				Timestamp: time.Now(),
				Entry: &RaftLogEntry_Query{
					Query: &QueryEntry{
						Value: request.Value,
					},
				},
			},
		}

		// Release the read lock before applying the entry.
		r.server.readUnlock()

		return r.applyQuery(entry, server)
	} else {
		r.server.readUnlock()
		return r.forwardQuery(request, leader, server)
	}
}

func (r *PassiveRole) applyQuery(entry *IndexedEntry, server RaftService_QueryServer) error {
	// Create a result channel
	ch := make(chan service.Output)

	// Apply the entry to the state machine
	r.server.state.applyEntry(entry, ch)

	// Iterate through results and translate them into QueryResponses.
	for result := range ch {
		if result.Succeeded() {
			response := &QueryResponse{
				Status: ResponseStatus_OK,
				Output: result.Value,
			}
			err := r.server.logResponse("QueryResponse", response, server.Send(response))
			if err != nil {
				return err
			}
		} else {
			response := &QueryResponse{
				Status:  ResponseStatus_ERROR,
				Message: result.Error.Error(),
			}
			err := r.server.logResponse("QueryResponse", response, server.Send(response))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// forwardQuery forwards a query request to the leader
func (r *PassiveRole) forwardQuery(request *QueryRequest, leader MemberID, server RaftService_QueryServer) error {
	if leader == "" {
		response := &QueryResponse{
			Status: ResponseStatus_ERROR,
			Error:  RaftError_NO_LEADER,
		}
		return r.server.logResponse("QueryResponse", response, server.Send(response))
	} else {
		log.WithField("memberID", r.server.cluster.member).
			Tracef("Forwarding %v", request)
		client, err := r.server.cluster.getClient(leader)
		if err != nil {
			return r.server.logResponse("QueryResponse", nil, err)
		}

		stream, err := client.Query(context.Background(), request)
		if err != nil {
			return r.server.logResponse("QueryResponse", nil, err)
		}

		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return r.server.logResponse("QueryResponse", nil, err)
			}
			r.server.logRequest("QueryResponse", response)
			r.server.logResponse("QueryResponse", response, server.Send(response))
		}
		return nil
	}
}
