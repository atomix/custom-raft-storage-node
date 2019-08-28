export CGO_ENABLED=0
export GO111MODULE=on

.PHONY: build

ATOMIX_GO_RAFT_VERSION := latest

all: build

build: # @HELP build the source code
build:
	GOOS=linux GOARCH=amd64 go build -o build/_output/atomix-go-raft ./cmd/atomix-go-raft

test: # @HELP run the unit tests and source code validation
test: build license_check linters
	go test github.com/atomix/atomix-go-raft/pkg/...

coverage: # @HELP generate unit test coverage data
coverage: build linters license_check
	#go test github.com/atomix/atomix-go-raft/... -coverprofile=coverage.out -covermode=count -coverpkg=`go list github.com/atomix/atomix-go-raft/...`
	go test github.com/atomix/atomix-go-raft/... -coverprofile=coverage.out -covermode=count -coverpkg=./...

linters: # @HELP examines Go source code and reports coding problems
	golangci-lint run

license_check: # @HELP examine and ensure license headers exist
	./build/licensing/boilerplate.py -v

proto: # @HELP build Protobuf/gRPC generated types
proto:
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-go-raft \
		-w /go/src/github.com/atomix/atomix-go-raft \
		--entrypoint build/bin/compile_protos.sh \
		onosproject/protoc-go:stable

image: # @HELP build atomix-go-raft Docker image
image: build
	docker build . -f build/docker/Dockerfile -t atomix/atomix-go-raft:${ATOMIX_GO_RAFT_VERSION}
