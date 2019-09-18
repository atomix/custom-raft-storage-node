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

// newMemoryMetadataStore creates a new in-memory metadata store
func newMemoryMetadataStore() MetadataStore {
	return &memoryMetadataStore{}
}

// MetadataStore stores metadata for a Raft server
type MetadataStore interface {
	// StoreTerm stores the Raft term
	StoreTerm(term Term)

	// LoadTerm loads the Raft term
	LoadTerm() *Term

	// StoreVote stores the Raft vote
	StoreVote(vote *MemberID)

	// LoadVote loads the Raft vote
	LoadVote() *MemberID

	// Close closes the store
	Close() error
}

// memoryMetadataStore implements MetadataStore in memory
type memoryMetadataStore struct {
	term *Term
	vote *MemberID
}

func (s *memoryMetadataStore) StoreTerm(term Term) {
	s.term = &term
}

func (s *memoryMetadataStore) LoadTerm() *Term {
	return s.term
}

func (s *memoryMetadataStore) StoreVote(vote *MemberID) {
	s.vote = vote
}

func (s *memoryMetadataStore) LoadVote() *MemberID {
	return s.vote
}

func (c *memoryMetadataStore) Close() error {
	return nil
}
