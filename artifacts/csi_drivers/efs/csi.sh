#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -eu

CSI_EFS_DIR=aws-efs-csi-driver
CSI_EFS_VERSION=573e1724d46113da4c45141fe5704aa7966fd794
EFS_PATCH_FILE=${EFS_DIR}/master.patch

function csi::efs::generate() {
	${GIT} clone https://github.com/kubernetes-sigs/aws-efs-csi-driver

	${CD} "${CSI_EFS_DIR}"

	${GIT} reset --hard "${CSI_EFS_VERSION}"

	${PATCH} -p1 < "${patch_dir}/${EFS_PATCH_FILE}"

	if [ -z "${IMAGE_NAME}" ]
	then
		IMAGE_NAME=amazon/aws-efs-csi-driver
	fi

	IMAGE="${IMAGE_NAME}" ${MAKE} image \
		&& ${DOCKER} image prune -f

	echo -n "Generated a docker image for the EFS CSI driver "
	echo "'${IMAGE_NAME}:master'"
}

function csi::efs::remove() {
	if [ -z "${IMAGE_NAME}" ]
	then
		IMAGE_NAME=amazon/aws-efs-csi-driver
	fi

	echo "Removing the docker image for the EFS CSI driver ..."
	${DOCKER} rmi "${IMAGE_NAME}:master"
}
