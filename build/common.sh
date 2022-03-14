#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

DSC_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd -P)

readonly BIN_DIR="${DSC_ROOT}/bin"
readonly BIN_PREFIX="shadowy-"

readonly BUILD_DIR="${DSC_ROOT}/build"

readonly HACK_DIR="${DSC_ROOT}/hack"

readonly DSC_SERVER_TARGETS=(
	lifetimer
	volume-controller
	policy-validator
)

readonly DSC_CLIENT_TARGETS=(
	volumectl
)

readonly DSC_ALL_TARGETS=(
	"${DSC_SERVER_TARGETS[@]}"
	"${DSC_CLIENT_TARGETS[@]}"
)

readonly GO=go

readonly DOCKER=docker

readonly FUSERMOUNT=fusermount

readonly SUDO=sudo

function dsc::build::has_docker() {
	which "${DOCKER}" &> /dev/null
}

function dsc::build::has_fusermount() {
	which "${FUSERMOUNT}" &> /dev/null
}

function dsc::build::has_docker_image() {
	[[ "$(${DOCKER} images -q $1 2> /dev/null)" != "" ]]
}
