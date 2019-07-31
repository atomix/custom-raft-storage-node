.PHONY: build proto

ONOS_BUILD_VERSION := stable
ATOMIX_GO_RAFT_VERSION := latest

all: build

build: # @HELP build the source code
build:
	go build -o build/_output/atomix-go-raft ./cmd/atomix-go-raft

proto: # @HELP build Protobuf/gRPC generated types
proto:
	docker build -t atomix/atomix-go-build:0.2 build/proto
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-go-raft atomix/atomix-go-build:0.2 build/proto

test: # @HELP run the unit tests and source code validation
test: build deps lint vet gofmt cyclo misspell ineffassign
	go test github.com/atomix/atomix-go-raft/pkg/...

coverage: # @HELP generate unit test coverage data
coverage: build deps lint vet gofmt cyclo misspell ineffassign
	./build/bin/coveralls-coverage

deps: # @HELP ensure that the required dependencies are in place
	go build -v ./...
	bash -c "diff -u <(echo -n) <(git diff go.mod)"
	bash -c "diff -u <(echo -n) <(git diff go.sum)"

lint: # @HELP run the linters for Go source code
	golint -set_exit_status github.com/atomix/atomix-go-raft/pkg/...

vet: # @HELP examines Go source code and reports suspicious constructs
	go vet github.com/atomix/atomix-go-raft/pkg/...

cyclo: # @HELP examines Go source code and reports complex cycles in code
	gocyclo -over 25 pkg/

misspell: # @HELP examines Go source code and reports misspelled words
	misspell -error -source=text pkg/

ineffassign: # @HELP examines Go source code and reports inefficient assignments
	ineffassign pkg/

gofmt: # @HELP run the Go format validation
	bash -c "diff -u <(echo -n) <(gofmt -d pkg/)"

image: # @HELP build atomix-go-raft Docker image
	@go mod vendor
	docker build . -f build/docker/Dockerfile \
		--build-arg ONOS_BUILD_VERSION=${ONOS_BUILD_VERSION} \
		-t atomix/atomix-go-raft:${ATOMIX_GO_RAFT_VERSION}
	@rm -rf vendor
