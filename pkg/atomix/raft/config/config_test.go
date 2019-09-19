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

package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestAdditionalConfigFunctions(t *testing.T) {
	config := &ProtocolConfig{}
	assert.Equal(t, defaultElectionTimeout, config.GetElectionTimeoutOrDefault())
	assert.Equal(t, defaultHeartbeatInterval, config.GetHeartbeatIntervalOrDefault())

	electionTimeout := 30 * time.Second
	heartbeatInterval := 1 * time.Second
	config = &ProtocolConfig{
		ElectionTimeout:   &electionTimeout,
		HeartbeatInterval: &heartbeatInterval,
	}
	assert.Equal(t, electionTimeout, config.GetElectionTimeoutOrDefault())
	assert.Equal(t, heartbeatInterval, config.GetHeartbeatIntervalOrDefault())
}
