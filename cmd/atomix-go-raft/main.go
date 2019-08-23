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

package main

import (
	"bytes"
	"fmt"
	"github.com/atomix/atomix-api/proto/atomix/controller"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-raft/pkg/atomix/raft"
	"github.com/golang/protobuf/jsonpb"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
)

func main() {
	log.SetLevel(log.TraceLevel)
	log.SetOutput(os.Stdout)

	nodeID := os.Args[1]
	partitionConfig := parsePartitionConfig()
	protocolConfig := parseProtocolConfig()

	node := atomix.NewNode(nodeID, partitionConfig, raft.NewRaftProtocol(protocolConfig))
	if err := node.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func parsePartitionConfig() *controller.PartitionConfig {
	nodeConfigFile := os.Args[2]
	nodeConfig := &controller.PartitionConfig{}
	nodeBytes, err := ioutil.ReadFile(nodeConfigFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := jsonpb.Unmarshal(bytes.NewReader(nodeBytes), nodeConfig); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return nodeConfig
}

func parseProtocolConfig() *raft.RaftProtocolConfig {
	protocolConfigFile := os.Args[3]
	protocolConfig := &raft.RaftProtocolConfig{}
	protocolBytes, err := ioutil.ReadFile(protocolConfigFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := jsonpb.Unmarshal(bytes.NewReader(protocolBytes), protocolConfig); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return protocolConfig
}
