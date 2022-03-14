# Copyright (c) 2022 Fujitsu Limited

FROM golang:latest

ENV http_proxy=${HTTP_PROXY} \
	https_proxy=${HTTPS_PROXY} \
	no_proxy=${NO_PROXY}

RUN apt-get update && \
	apt-get install -y fuse

COPY hack/test/go.mod hack/test/go.sum /mnt/

RUN cd /mnt && go mod download && rm go.mod go.sum

COPY hack/test/docker_test.sh /usr/local/bin/

WORKDIR /src/dsc

CMD ["/usr/local/bin/docker_test.sh"]
