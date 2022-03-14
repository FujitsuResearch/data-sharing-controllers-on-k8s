#!/usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -e

PROTOC=protoc

${PROTOC} --go_out=plugins=grpc:./ --go_opt=paths=source_relative \
	./pkg/apis/grpc/volumecontrol/v1alpha1/volumectl.proto
