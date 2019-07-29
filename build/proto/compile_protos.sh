#!/bin/sh

go get -u github.com/golang/protobuf/protoc-gen-go

proto_imports="./pkg:${GOPATH}/src/github.com/google/protobuf/src:${GOPATH}/src"

protoc -I=$proto_imports --go_out=import_path=atomix/raft,plugins=grpc:pkg pkg/atomix/raft/*.proto