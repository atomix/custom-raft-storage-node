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
	"github.com/atomix/atomix-api/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/pkg/atomix/registry"
	"github.com/atomix/atomix-raft-node/pkg/atomix/raft/config"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestProtocol(t *testing.T) {
	c := &controller.PartitionConfig{
		Partition: &controller.PartitionId{
			Partition: 1,
			Group: &controller.PartitionGroupId{
				Name:      "test",
				Namespace: "default",
			},
		},
		Members: []*controller.NodeConfig{
			{
				ID:   "foo",
				Host: "localhost",
				Port: 5679,
			},
		},
	}
	protocol := NewProtocol(&config.ProtocolConfig{})
	node := atomix.NewNode("foo", c, protocol, registry.Registry)
	assert.NoError(t, node.Start())
	time.Sleep(1 * time.Second)
	defer node.Stop()
}
