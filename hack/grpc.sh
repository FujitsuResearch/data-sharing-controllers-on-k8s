#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

readonly GRPC_IMAGE_NAME="grpc-codegen:latest"

readonly CHOWN=chown

function dsc::grpc::generate_code() {
	if ! dsc::build::has_docker
	then
		echo "error: '${DOCKER}' is required."
		exit 1
	fi

	echo "Building a grpc-codegen docker image ..."
	local -r hack_grpc_dir="${HACK_DIR}/grpc"

	${DOCKER} build -f "${hack_grpc_dir}/Dockerfile" \
	    --build-arg DUMMY_UUID_ARG="${CODEGEN_BUILD_CACHE:-${RANDOM}}" \
	    --build-arg HTTP_PROXY="${http_proxy}" \
	    --build-arg HTTPS_PROXY="${https_proxy}" \
	    --build-arg NO_PROXY="${no_proxy}" \
	    --pull -t "${GRPC_IMAGE_NAME}" "${DSC_ROOT}"
	
	echo "Generating the client codes ..."
	local -r dsc="dsc"
	local -r gen_cmd="/src/${dsc}/hack/grpc/docker_codegen.sh"

	${DOCKER} run --rm \
		-v "$(realpath ./):/src/${dsc}" "${GRPC_IMAGE_NAME}" \
		"${gen_cmd}"

	${SUDO} ${CHOWN} "${USER}:${USER}" -R \
		"${DSC_ROOT}/pkg/apis/grpc/volumecontrol/v1alpha1/"
	
	echo "Pruning the docker image ..."
	${DOCKER} image prune -f
}

function dsc::grpc::clean() {
	echo "Deleting '*.pb.go' in 'pkg/apis/grpc' ..."
	find "${DSC_ROOT}/pkg/apis/grpc" \
		-name '*.pb.go' \
		-exec rm {} \;

	if dsc::build::has_docker_image "${GRPC_IMAGE_NAME}"
	then
		echo "Deleting ${GRPC_IMAGE_NAME}..."
		${DOCKER} rmi ${GRPC_IMAGE_NAME}
	fi
}
