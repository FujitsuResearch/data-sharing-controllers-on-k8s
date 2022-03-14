#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

readonly CR_IMAGE_NAME="kubernetes-codegen:latest"

readonly AWK=awk
readonly CAT=cat
readonly GREP=grep

function dsc::cr::generate_code() {
	local -r gomod_dsc="github.com/FujitsuResearch/data-sharing-controllers-on-k8s"

	if ! dsc::build::has_docker
	then
		echo "error: '${DOCKER}' is required."
		exit 1
	fi

	echo "Building a cr-codegen docker image ..."
	local -r codegen_version=$(${CAT} "${DSC_ROOT}/go.mod" | \
		${GREP} client-go | ${AWK} '{print $2}')
	local -r hack_cr_dir="${HACK_DIR}/cr"
	echo "codegen_version: ${codegen_version}"

	${DOCKER} build -f "${hack_cr_dir}/Dockerfile" \
            --build-arg CODEGEN_VERSION="${codegen_version}" \
	    --build-arg DUMMY_UUID_ARG="${CODEGEN_BUILD_CACHE:-${RANDOM}}" \
	    --build-arg GOMOD_DSC="${gomod_dsc}" \
	    --build-arg HTTP_PROXY="${http_proxy}" \
	    --build-arg HTTPS_PROXY="${https_proxy}" \
	    --build-arg NO_PROXY="${no_proxy}" \
	    --pull -t "${CR_IMAGE_NAME}" \
	    "${DSC_ROOT}"
	

	echo "Generating the client codes ..."
	local -r docker_gopath="/go"
	local -r dsc_directory="${docker_gopath}/src/${gomod_dsc}"
	local -r gen_cmd="${dsc_directory}/hack/cr/docker_codegen.sh"

	${DOCKER} run --rm \
		-e GOMOD_DSC="${gomod_dsc}" \
		-e DSC_DIR="${dsc_directory}" \
		-v "$(realpath ./):${docker_gopath}/src/${gomod_dsc}" \
		"${CR_IMAGE_NAME}" "${gen_cmd}"
	
	${SUDO} chown "${USER}:${USER}" -R "${DSC_ROOT}/pkg"
	
	echo "Pruning the docker image ..."
	${DOCKER} image prune -f
}

function dsc::cr::clean() {
	if [[ -d "${DSC_ROOT}/pkg/client/cr" ]]; then
		rm -r "${DSC_ROOT}/pkg/client/cr"
	fi

	echo "Deleting 'zz_generated.deepcopy.go' in 'pkg/apis/cr' ..."
	find "${DSC_ROOT}/pkg/apis/cr" \
		-name 'zz_generated.deepcopy.go' \
		-exec rm {} \;

	if dsc::build::has_docker_image "${CR_IMAGE_NAME}"
	then
		echo "Deleting ${CR_IMAGE_NAME}..."
		${DOCKER} rmi ${CR_IMAGE_NAME}
	fi
}
