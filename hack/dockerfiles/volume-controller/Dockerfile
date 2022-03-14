# Copyright (c) 2022 Fujitsu Limited

FROM alpine:latest

RUN apk update && \
	apk add fuse

COPY shadowy-volume-controller /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/shadowy-volume-controller"]
