package raft

// MetadataStore stores metadata for a Raft server
type MetadataStore interface {
	StoreTerm(term Term)
	LoadTerm() *Term
	StoreVote(vote *MemberID)
	LoadVote() *MemberID
}

func newMemoryMetadataStore() MetadataStore {
	return &memoryMetadataStore{}
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
