FROM alpine:3.9

USER nobody

ADD build/raft-storage-node/_output/raft-storage-node /usr/local/bin/raft-storage-node

ENTRYPOINT ["raft-storage-node"]
