#!/bin/sh

proto_imports="./pkg:./test:${GOPATH}/src/github.com/gogo/protobuf:${GOPATH}/src/github.com/gogo/protobuf/protobuf:${GOPATH}/src"

protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/raft,plugins=grpc:pkg pkg/atomix/raft/*.proto
protoc -I=$proto_imports --gogofaster_out=Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,import_path=atomix/raft/config,plugins=grpc:pkg pkg/atomix/raft/config/*.proto
protoc -I=$proto_imports --gogofaster_out=import_path=test,plugins=grpc:test test/*.proto