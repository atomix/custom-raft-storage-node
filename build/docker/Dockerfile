FROM alpine:3.9

USER nobody

ADD build/_output/atomix-raft-node /usr/local/bin/atomix-raft-node

ENTRYPOINT ["atomix-raft-node"]
