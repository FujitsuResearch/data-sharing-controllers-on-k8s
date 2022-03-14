#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -eu

CSI_CEPHFS_DIR=ceph-csi
CSI_CEPHFS_VERSION=v3.5.1
CEPHFS_PATCH_FILE=${CEPHFS_DIR}/${CSI_CEPHFS_VERSION//./_}.patch

function csi::cephfs::generate() {
	${GIT} clone https://github.com/ceph/ceph-csi.git \
		-b "${CSI_CEPHFS_VERSION}"

	${CD} "${CSI_CEPHFS_DIR}"

	${PATCH} -p1 < "${patch_dir}/${CEPHFS_PATCH_FILE}"

	if [ -z "${IMAGE_NAME}" ]
	then
		IMAGE_NAME=cephcsi/cephcsi
	fi

	ENV_CSI_IMAGE_NAME="${IMAGE_NAME}" ${MAKE} image-cephcsi \
		&& ${DOCKER} image prune -f

	echo -n "Generated a docker image for the CephFS CSI driver "
	echo "'${IMAGE_NAME}:${CSI_CEPHFS_VERSION}'"
}

function csi::cephfs::remove() {
	if [ -z "${IMAGE_NAME}" ]
	then
		IMAGE_NAME=cephcsi/cephcsi
	fi

	echo "Removing the docker image for the CephFS CSI driver ..."
	${DOCKER} rmi "${IMAGE_NAME}:${CSI_CEPHFS_VERSION}"
}
