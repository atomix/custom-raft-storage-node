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
	"io/ioutil"
	"os"
	"os/signal"

	"github.com/atomix/api/proto/atomix/controller"
	"github.com/atomix/go-framework/pkg/atomix"
	"github.com/atomix/go-framework/pkg/atomix/registry"
	"github.com/atomix/raft-replica/pkg/atomix/raft"
	"github.com/atomix/raft-replica/pkg/atomix/raft/config"
	"github.com/gogo/protobuf/jsonpb"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.TraceLevel)
	log.SetOutput(os.Stdout)

	nodeID := os.Args[1]
	partitionConfig := parsePartitionConfig()
	protocolConfig := parseProtocolConfig()

	// Start the node. The node will be started in its own goroutine.
	node := atomix.NewNode(nodeID, partitionConfig, raft.NewProtocol(protocolConfig), registry.Registry)
	if err := node.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Wait for an interrupt signal
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch

	// Stop the node after an interrupt
	if err := node.Stop(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func parseClusterConfig() *controller.ClusterConfig {
	clusterConfigFile := os.Args[2]
	clusterConfig := &controller.ClusterConfig{}
	clusterConfigBytes, err := ioutil.ReadFile(clusterConfigFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := jsonpb.Unmarshal(bytes.NewReader(clusterConfigBytes), clusterConfig); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return clusterConfig
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

func parseProtocolConfig() *config.ProtocolConfig {
	protocolConfigFile := os.Args[3]
	protocolConfig := &config.ProtocolConfig{}
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
