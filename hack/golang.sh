#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

function dsc::golang::build_executables() {
	for executable in "${DSC_ALL_TARGETS[@]}"
	do
		echo "Building '${executable}' ..."
		CGO_ENABLED=0 ${GO} build \
			"-ldflags=-s -w -buildid=" \
			-trimpath \
			-o "${BIN_DIR}/${BIN_PREFIX}${executable}" \
			"${DSC_ROOT}/cmd/${executable}/main.go"
	done
}

function dsc::golang::clean() {
	if [[ -d "${BIN_DIR}" ]]
	then
		echo "Deleting '${BIN_DIR}' ..."
		rm -r "${BIN_DIR}"
	fi
}
