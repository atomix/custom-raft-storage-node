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

import "time"

const (
	defaultElectionTimeout   = 5 * time.Second
	defaultHeartbeatInterval = 500 * time.Millisecond
	defaultSnapshotInterval  = 1 * time.Minute
	defaultSnapshotThreshold = 1000
)

// GetElectionTimeoutOrDefault returns the configured election timeout if set, otherwise the default election timeout
func (c *ProtocolConfig) GetElectionTimeoutOrDefault() time.Duration {
	timeout := c.GetElectionTimeout()
	if timeout != nil {
		return *timeout
	}
	return defaultElectionTimeout
}

// GetHeartbeatIntervalOrDefault returns the configured heartbeat interval if set, otherwise the default heartbeat interval
func (c *ProtocolConfig) GetHeartbeatIntervalOrDefault() time.Duration {
	interval := c.GetHeartbeatInterval()
	if interval != nil {
		return *interval
	}
	return defaultHeartbeatInterval
}

// GetSnapshotIntervalOrDefault returns the configured snapshot interval if set, otherwise the default snapshot interval
func (c *ProtocolConfig) GetSnapshotIntervalOrDefault() time.Duration {
	interval := c.GetSnapshotInterval()
	if interval != nil {
		return *interval
	}
	return defaultSnapshotInterval
}

// GetSnapshotThresholdOrDefault returns the configured snapshot threshold if set, otherwise the default snapshot threshold
func (c *ProtocolConfig) GetSnapshotThresholdOrDefault() uint64 {
	threshold := c.GetSnapshotThreshold()
	if threshold > 0 {
		return threshold
	}
	return defaultSnapshotThreshold
}
