package main

import (
	"bytes"
	"fmt"
	"github.com/atomix/atomix-go-node/pkg/atomix"
	"github.com/atomix/atomix-go-node/proto/atomix/controller"
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
