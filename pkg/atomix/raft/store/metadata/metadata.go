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

package metadata

import raft "github.com/atomix/atomix-raft-node/pkg/atomix/raft/protocol"

// NewMemoryMetadataStore creates a new in-memory metadata store
func NewMemoryMetadataStore() MetadataStore {
	return &memoryMetadataStore{}
}

// MetadataStore stores metadata for a Raft server
type MetadataStore interface {
	// StoreTerm stores the Raft term
	StoreTerm(term raft.Term)

	// LoadTerm loads the Raft term
	LoadTerm() *raft.Term

	// StoreVote stores the Raft vote
	StoreVote(vote *raft.MemberID)

	// LoadVote loads the Raft vote
	LoadVote() *raft.MemberID

	// Close closes the store
	Close() error
}

// memoryMetadataStore implements MetadataStore in memory
type memoryMetadataStore struct {
	term *raft.Term
	vote *raft.MemberID
}

func (s *memoryMetadataStore) StoreTerm(term raft.Term) {
	s.term = &term
}

func (s *memoryMetadataStore) LoadTerm() *raft.Term {
	return s.term
}

func (s *memoryMetadataStore) StoreVote(vote *raft.MemberID) {
	s.vote = vote
}

func (s *memoryMetadataStore) LoadVote() *raft.MemberID {
	return s.vote
}

func (c *memoryMetadataStore) Close() error {
	return nil
}
