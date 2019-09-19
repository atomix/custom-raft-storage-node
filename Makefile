export CGO_ENABLED=0
export GO111MODULE=on

.PHONY: build

ATOMIX_RAFT_NODE_VERSION := latest

all: build

build: # @HELP build the source code
build:
	GOOS=linux GOARCH=amd64 go build -o build/_output/atomix-raft-node ./cmd/atomix-raft-node

test: # @HELP run the unit tests and source code validation
test: build license_check linters
	go test github.com/atomix/atomix-raft-node/...

coverage: # @HELP generate unit test coverage data
coverage: build linters license_check
	#go test github.com/atomix/atomix-raft-node/... -coverprofile=coverage.out -covermode=count -coverpkg=`go list github.com/atomix/atomix-raft-node/...`
	go test github.com/atomix/atomix-raft-node/pkg/... -coverprofile=coverage.out -covermode=count

linters: # @HELP examines Go source code and reports coding problems
	golangci-lint run

license_check: # @HELP examine and ensure license headers exist
	./build/licensing/boilerplate.py -v

proto: # @HELP build Protobuf/gRPC generated types
proto:
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-raft-node \
		-w /go/src/github.com/atomix/atomix-raft-node \
		--entrypoint build/bin/compile_protos.sh \
		onosproject/protoc-go:stable

image: # @HELP build atomix-raft-node Docker image
image: build
	docker build . -f build/docker/Dockerfile -t atomix/atomix-raft-node:${ATOMIX_RAFT_NODE_VERSION}

push: # @HELP push atomix-raft-node Docker image
	docker push atomix/atomix-raft-node:${ATOMIX_RAFT_NODE_VERSION}
