FROM alpine:3.9

USER nobody

ADD build/_output/raft-storage /usr/local/bin/raft-storage

ENTRYPOINT ["raft-storage"]
