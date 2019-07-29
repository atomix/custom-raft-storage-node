.PHONY: build proto

all: build

build:
	go build -v ./...
test: build
	go test github.com/atomix/atomix-go-node/pkg/atomix
proto:
	docker build -t atomix/atomix-go-build:0.2 build/proto
	docker run -it -v `pwd`:/go/src/github.com/atomix/atomix-go-raft atomix/atomix-go-build:0.2 build/proto
