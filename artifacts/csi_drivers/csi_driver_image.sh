#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -eu

CD=cd
DOCKER=docker
GIT=git
MAKE=make
MKDIR=mkdir
PATCH=patch
POPD=popd
PUSHD=pushd
READLINK=readlink
RM=rm

CEPHFS_DIR=cephfs
EFS_DIR=efs
FILESTORE_DIR=filestore
IMAGE_NAME=${IMAGE_NAME:-}
PATCH_ROOT_DIR=$(dirname "${BASH_SOURCE[0]}")
WORK_ROOT_DIR=/tmp/csidriver

source "${PATCH_ROOT_DIR}/${CEPHFS_DIR}/csi.sh"
source "${PATCH_ROOT_DIR}/${EFS_DIR}/csi.sh"
source "${PATCH_ROOT_DIR}/${FILESTORE_DIR}/csi.sh"

function usage() {
echo "usage: $0 generate [cephfs|efs|filestore]"
echo "       $0 remove [cephfs|efs|filestore]"
exit 1
}

function generate_for_each_driver() {
	case $1 in
		cephfs)
			csi::cephfs::generate
			;;
		efs)
			csi::efs::generate
			;;
		filestore)
			csi::filestore::generate
			;;
		*)
			echo "Not support $1 at this time"
			usage
	esac
}

function generate() {
	local -r patch_dir=$(${READLINK} -f "${PATCH_ROOT_DIR}")

        if [[ -d "${WORK_ROOT_DIR}" ]]; then
                rm -rf "${WORK_ROOT_DIR}"
        fi

	${MKDIR} "${WORK_ROOT_DIR}"

	${PUSHD} "${WORK_ROOT_DIR}"

	generate_for_each_driver $1

	${POPD}

	${RM} -rf "${WORK_ROOT_DIR}"
}

function remove() {
	case $1 in
		cephfs)
			csi::cephfs::remove
			;;
		efs)
			csi::efs::remove
			;;
		filestore)
			csi::filestore::remove
			;;
		*)
			echo "Not support $1 at this time"
			usage
	esac
}

if [ $# -ne 2 ]; then
	usage
fi

case $1 in
	generate)
		generate "$2"
		;;

	remove)
		remove "$2"
		;;

	*)
		usage
		;;
esac
