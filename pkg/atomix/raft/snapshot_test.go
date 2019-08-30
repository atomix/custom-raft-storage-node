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
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSnapshot(t *testing.T) {
	store := newMemorySnapshotStore()
	assert.Nil(t, store.CurrentSnapshot())

	ts := time.Now()
	snapshot := store.newSnapshot(Index(1), ts)
	assert.Equal(t, Index(1), snapshot.Index())
	assert.Equal(t, ts, snapshot.Timestamp())

	writer := snapshot.Writer()
	_, err := writer.Write([]byte("Hello world!"))
	assert.NoError(t, err)
	err = writer.Close()
	assert.NoError(t, err)

	reader := snapshot.Reader()
	bytes := make([]byte, len([]byte("Hello world!")))
	_, err = reader.Read(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "Hello world!", string(bytes))
	err = reader.Close()
	assert.NoError(t, err)

	snapshot = store.CurrentSnapshot()
	reader = snapshot.Reader()
	bytes = make([]byte, len([]byte("Hello world!")))
	_, err = reader.Read(bytes)
	assert.NoError(t, err)
	assert.Equal(t, "Hello world!", string(bytes))
	err = reader.Close()
	assert.NoError(t, err)
}
