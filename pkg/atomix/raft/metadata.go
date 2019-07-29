package raft

// MetadataStore stores metadata for a Raft server
type MetadataStore interface {
	StoreTerm(term int64)
	LoadTerm() *int64
	StoreVote(vote string)
	LoadVote() *string
}

func newMemoryMetadataStore() MetadataStore {
	return &memoryMetadataStore{}
}

// memoryMetadataStore implements MetadataStore in memory
type memoryMetadataStore struct {
	term *int64
	vote *string
}

func (s *memoryMetadataStore) StoreTerm(term int64) {
	s.term = &term
}

func (s *memoryMetadataStore) LoadTerm() *int64 {
	return s.term
}

func (s *memoryMetadataStore) StoreVote(vote string) {
	s.vote = &vote
}

func (s *memoryMetadataStore) LoadVote() *string {
	return s.vote
}
