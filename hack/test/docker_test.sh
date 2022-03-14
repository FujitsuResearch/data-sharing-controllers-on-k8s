#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

GO=go

GOMODULE=github.com/FujitsuResearch/data-sharing-controllers-on-k8s

${GO} test ${TFLAGS} ${GOMODULE}/cmd/lifetimer/app/options/...
${GO} test ${TFLAGS} ${GOMODULE}/cmd/volume-controller/app/options/...
${GO} test ${TFLAGS} ${GOMODULE}/cmd/policy-validator/app/options/...

KAFKA_BROKER_ADDRESS=${KAFKA_BROKER_ADDRESS} \
	${GO} test ${TFLAGS} ${GOMODULE}/pkg/controller/lifetimes
KAFKA_BROKER_ADDRESS=${KAFKA_BROKER_ADDRESS} \
	${GO} test ${TFLAGS} ${GOMODULE}/pkg/lifetimes/storage/filesystem
${GO} test ${TFLAGS} ${GOMODULE}/pkg/lifetimes/volumecontrol
${GO} test ${TFLAGS} ${GOMODULE}/pkg/lifetimes/trigger

${GO} test ${TFLAGS} ${GOMODULE}/pkg/volumectl

${GO} test ${TFLAGS} ${GOMODULE}/pkg/client/csi
${GO} test ${TFLAGS} ${GOMODULE}/pkg/runtime/config
${GO} test ${TFLAGS} ${GOMODULE}/pkg/runtime/containerd
${GO} test ${TFLAGS} ${GOMODULE}/pkg/runtime/docker

${GO} test ${TFLAGS} ${GOMODULE}/pkg/controller/validation
${GO} test ${TFLAGS} ${GOMODULE}/pkg/datastore
${GO} test ${TFLAGS} ${GOMODULE}/pkg/datastore/config
${GO} test ${TFLAGS} ${GOMODULE}/pkg/datastore/redis

KAFKA_BROKER_ADDRESS=${KAFKA_BROKER_ADDRESS} \
	${GO} test ${TFLAGS} ${GOMODULE}/pkg/messagequeue/...

${GO} test ${TFLAGS} ${GOMODULE}/pkg/controller/volume
${GO} test ${TFLAGS} ${GOMODULE}/pkg/volume/messagequeue/publish
${GO} test ${TFLAGS} ${GOMODULE}/pkg/volume/messagequeue/subscribe
KAFKA_BROKER_ADDRESS=${KAFKA_BROKER_ADDRESS} \
	${GO} test ${TFLAGS} ${GOMODULE}/pkg/volume/fuse
${GO} test ${TFLAGS} ${GOMODULE}/pkg/volume/plugins
