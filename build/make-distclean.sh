#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -e

BUILD_DIR=$(dirname "${BASH_SOURCE[0]}")

source "${BUILD_DIR}/common.sh"

source "${HACK_DIR}/custom_resources.sh"
source "${HACK_DIR}/grpc.sh"
source "${HACK_DIR}/golang.sh"
source "${HACK_DIR}/docker.sh"
source "${HACK_DIR}/test/test.sh"


dsc::cr::clean

dsc::grpc::clean

dsc::golang::clean

dsc::docker::clean

dsc::test::clean
