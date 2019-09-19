#!/bin/sh

proto_imports="./pkg:./test:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/raft/config,plugins=grpc:pkg pkg/atomix/raft/config/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/raft/protocol,plugins=grpc:pkg pkg/atomix/raft/protocol/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/raft/snapshot,plugins=grpc:pkg pkg/atomix/raft/store/snapshot/*.proto
protoc -I=$proto_imports --gogofaster_out=import_path=atomix/raft/roles,plugins=grpc:pkg pkg/atomix/raft/roles/*.proto
protoc -I=$proto_imports --gogofaster_out=import_path=test,plugins=grpc:test test/*.proto