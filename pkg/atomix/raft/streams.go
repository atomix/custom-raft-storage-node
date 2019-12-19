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
	"github.com/atomix/atomix-go-node/pkg/atomix/stream"
	"sync"
)

type streamID uint64

// newStreamManager returns a new stream manager
func newStreamManager() *streamManager {
	return &streamManager{
		streams: make(map[streamID]chan stream.Result),
	}
}

// streamManager is a manager of client streams
type streamManager struct {
	streams map[streamID]chan stream.Result
	nextID  streamID
	mu      sync.RWMutex
}

// newStream creates a new stream
func (r *streamManager) newStream() (streamID, chan stream.Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	streamID := r.nextID
	ch := make(chan stream.Result)
	r.streams[streamID] = ch
	return streamID, ch
}

// getStream gets a stream by ID
func (r *streamManager) getStream(streamID streamID) chan stream.Result {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.streams[streamID]
}

// deleteStream deletes a stream
func (r *streamManager) deleteStream(streamID streamID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.streams, streamID)
}
