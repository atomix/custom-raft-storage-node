FROM alpine:3.9

USER nobody

ADD build/_output/atomix-go-raft /usr/local/bin/atomix-go-raft

ENTRYPOINT ["atomix-go-raft"]
