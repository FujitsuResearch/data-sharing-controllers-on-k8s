#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -e

BUILD_DIR=$(dirname "${BASH_SOURCE[0]}")

source "${BUILD_DIR}/common.sh"

source "${HACK_DIR}/test/test.sh"

dsc::test::run
