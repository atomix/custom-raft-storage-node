ARG ONOS_BUILD_VERSION=latest

FROM onosproject/golang-build:$ONOS_BUILD_VERSION AS builder

ENV GO111MODULE=on
COPY . /go/src/github.com/atomix/atomix-go-raft
RUN cd /go/src/github.com/atomix/atomix-go-raft && GOFLAGS=-mod=vendor make build

FROM alpine:3.9

USER nobody

COPY --from=builder /go/src/github.com/atomix/atomix-go-raft/build/_output/atomix-go-raft /usr/local/bin/atomix-go-raft

ENTRYPOINT ["atomix-go-raft"]
