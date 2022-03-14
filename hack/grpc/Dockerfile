# Copyright (c) 2022 Fujitsu Limited

FROM golang:latest

ARG SRC_ROOT=/src
ARG DSC=dsc
ARG CODEGEN_ROOT=${SRC_ROOT}/${DSC}

ARG DUMMY_UUID_ARG

ENV http_proxy=${HTTP_PROXY} \
	https_proxy=${HTTPS_PROXY} \
	no_proxy=${NO_PROXY}

# [REF] grpc/golang Dockerfile

# Install protobuf from source
RUN apt-get update && \
	apt-get install -y \
	git \
	unzip \
	build-essential \
	autoconf \
	libtool

RUN git clone https://github.com/google/protobuf.git && \
	cd protobuf && \
	./autogen.sh && \
	./configure && \
	make && \
	make install && \
	ldconfig && \
	make clean && \
	cd .. && \
	rm -r protobuf

# Get the source from GitHub
RUN go get google.golang.org/grpc

# Install protoc-gen-go
RUN go get github.com/golang/protobuf/protoc-gen-go

WORKDIR ${CODEGEN_ROOT}
