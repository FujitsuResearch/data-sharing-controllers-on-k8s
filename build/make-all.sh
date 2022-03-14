#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -e

BUILD_DIR=$(dirname "${BASH_SOURCE[0]}")

source "${BUILD_DIR}/common.sh"

source "${HACK_DIR}/custom_resources.sh"
source "${HACK_DIR}/grpc.sh"
source "${HACK_DIR}/golang.sh"
source "${HACK_DIR}/docker.sh"

dsc::cr::generate_code

dsc::grpc::generate_code

dsc::golang::build_executables

dsc::docker::build_docker_images
