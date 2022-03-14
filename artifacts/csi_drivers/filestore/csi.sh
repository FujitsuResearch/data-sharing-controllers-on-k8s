#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -eu

CSI_FILESTORE_DIR=gcp-filestore-csi-driver
CSI_FILESTORE_VERSION=v1.1.3
FILESTORE_PATCH_FILE=${FILESTORE_DIR}/${CSI_FILESTORE_VERSION//./_}.patch

function csi::filestore::generate() {
	${GIT} clone \
		https://github.com/kubernetes-sigs/gcp-filestore-csi-driver \
		-b "${CSI_FILESTORE_VERSION}"

	${CD} "${CSI_FILESTORE_DIR}"

	${PATCH} -p1 < "${patch_dir}/${FILESTORE_PATCH_FILE}"

	if [ -z "${IMAGE_NAME}" ]
	then
		IMAGE_NAME=gcr.io/gcp-filestore-csi-driver
	fi

	GCP_FS_CSI_STAGING_IMAGE="${IMAGE_NAME}" \
	GCP_FS_CSI_STAGING_VERSION="${CSI_FILESTORE_VERSION}" ${MAKE} \
		&& ${DOCKER} image prune -f

	echo -n "Generated a docker image for the Filestore CSI driver "
	echo "'${IMAGE_NAME}:${CSI_FILESTORE_VERSION}'"
}

function csi::filestore::remove() {
	if [ -z "${IMAGE_NAME}" ]
	then
		IMAGE_NAME=gcr.io/gcp-filestore-csi-driver
	fi

	echo "Removing the docker image for the Filestore CSI driver ..."
	${DOCKER} rmi "${IMAGE_NAME}:${CSI_FILESTORE_VERSION}"
}
