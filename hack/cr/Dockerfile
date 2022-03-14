# Copyright (c) 2022 Fujitsu Limited

FROM golang:latest

ARG CODEGEN_VERSION
ARG SRC_ROOT=/src
ARG K8S_ROOT=${SRC_ROOT}/k8s.io
ARG CODEGEN_DIR=code-generator
ARG CODEGEN_ROOT=${K8S_ROOT}/${CODEGEN_DIR}

ARG GOMOD_DSC=lcm
ARG DUMMY_UUID_ARG

ENV GOMOD_DSC=${GOMOD_DSC} \
	DSC_ROOT=${GOPATH}/src/${GOMOD_DSC} \
	http_proxy=${HTTP_PROXY} \
	https_proxy=${HTTPS_PROXY} \
	no_proxy=${NO_PROXY}


RUN mkdir -p ${DSC_ROOT} && mkdir -p ${K8S_ROOT} && \
	cd ${K8S_ROOT} && \
	git clone http://github.com/kubernetes/code-generator \
		-b ${CODEGEN_VERSION} && \
	cd ${CODEGEN_DIR} && \
	sed -i -e "$ s:):&\n\nreplace ${GOMOD_DSC} => ${DSC_ROOT}\n:g" \
		${CODEGEN_ROOT}/go.mod

WORKDIR ${CODEGEN_ROOT}
