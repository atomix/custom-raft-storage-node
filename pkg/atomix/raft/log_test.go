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

func TestMemoryLog(t *testing.T) {
	log := newMemoryLog()
	writer := log.Writer()
	reader := log.OpenReader(0)

	assert.Equal(t, int64(0), writer.LastIndex())

	entry := writer.Append(&RaftLogEntry{
		Term:      1,
		Timestamp: time.Now(),
		Entry:     &RaftLogEntry_Initialize{},
	})
	assert.Equal(t, int64(1), entry.Index)
	assert.Equal(t, int64(1), entry.Entry.Term)

	assert.Equal(t, int64(0), reader.CurrentIndex())
	assert.Nil(t, reader.CurrentEntry())

	assert.Equal(t, int64(1), reader.NextIndex())
	entry = reader.NextEntry()
	assert.NotNil(t, entry)
	assert.Equal(t, int64(1), entry.Index)
	assert.Equal(t, int64(1), entry.Entry.Term)

	assert.Equal(t, int64(2), reader.NextIndex())
	assert.Nil(t, reader.NextEntry())

	entry = writer.Append(&RaftLogEntry{
		Term:      1,
		Timestamp: time.Now(),
		Entry:     &RaftLogEntry_Initialize{},
	})
	assert.Equal(t, int64(2), entry.Index)
	assert.Equal(t, int64(1), entry.Entry.Term)
	assert.Equal(t, int64(2), writer.LastIndex())
	assert.Equal(t, int64(1), writer.LastEntry().Entry.Term)

	assert.Equal(t, int64(2), reader.NextIndex())
	entry = reader.NextEntry()
	assert.NotNil(t, entry)
	assert.Equal(t, int64(2), entry.Index)
	assert.Equal(t, int64(1), entry.Entry.Term)

	assert.Equal(t, int64(1), reader.FirstIndex())

	writer.Append(&RaftLogEntry{
		Term:      1,
		Timestamp: time.Now(),
		Entry:     &RaftLogEntry_Initialize{},
	})
	writer.Append(&RaftLogEntry{
		Term:      1,
		Timestamp: time.Now(),
		Entry:     &RaftLogEntry_Initialize{},
	})
	writer.Append(&RaftLogEntry{
		Term:      1,
		Timestamp: time.Now(),
		Entry:     &RaftLogEntry_Initialize{},
	})

	assert.Equal(t, int64(5), writer.LastIndex())

	assert.NotNil(t, reader.NextEntry())
	assert.NotNil(t, reader.NextEntry())
	assert.NotNil(t, reader.NextEntry())

	reader.Reset(2)
	assert.Equal(t, int64(2), reader.NextIndex())
	entry = reader.NextEntry()
	assert.NotNil(t, entry)
	assert.Equal(t, int64(2), entry.Index)
	assert.Equal(t, int64(1), entry.Entry.Term)

	assert.Equal(t, int64(3), reader.NextEntry().Index)
	assert.Equal(t, int64(4), reader.NextEntry().Index)
	assert.Equal(t, int64(5), reader.NextEntry().Index)

	writer.Truncate(3)
	assert.Equal(t, int64(3), writer.LastIndex())
	assert.Equal(t, int64(4), reader.NextIndex())
	assert.Nil(t, reader.NextEntry())
	entry = writer.Append(&RaftLogEntry{
		Term:      2,
		Timestamp: time.Now(),
		Entry:     &RaftLogEntry_Initialize{},
	})
	assert.Equal(t, int64(4), entry.Index)
	assert.Equal(t, int64(4), reader.NextIndex())
	entry = reader.NextEntry()
	assert.Equal(t, int64(4), entry.Index)
	assert.Equal(t, int64(2), entry.Entry.Term)

	writer.Reset(10)
	assert.Equal(t, int64(9), writer.LastIndex())
	assert.Nil(t, writer.LastEntry())
	assert.Equal(t, int64(10), reader.FirstIndex())
	assert.Equal(t, int64(9), reader.CurrentIndex())
	assert.Nil(t, reader.CurrentEntry())
	assert.Equal(t, int64(10), reader.NextIndex())
	assert.Nil(t, reader.NextEntry())
}
