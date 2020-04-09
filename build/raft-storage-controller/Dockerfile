FROM alpine:3.9

USER nobody

ADD build/_output/raft-storage-controller /usr/local/bin/raft-storage-controller

ENTRYPOINT ["raft-storage-controller"]
