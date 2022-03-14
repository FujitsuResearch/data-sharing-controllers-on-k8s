#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

function dsc::docker::build_docker_images() {
	local -r dockerfiles_dir=${HACK_DIR}/dockerfiles

	if ! dsc::build::has_docker
	then
		echo "error: '${DOCKER}' is required."
		exit 1
	fi

        local docker_registry_prefix=""
	if [[ "${DOCKER_REGISTRY}" ]]
	then
	        docker_registry_prefix="${DOCKER_REGISTRY}/"
	fi

	for executable in "${DSC_ALL_TARGETS[@]}"
	do
		local bin_name="${BIN_PREFIX}${executable}"
		local bin_path="${BIN_DIR}/${bin_name}"
		local dockerfile_dir="${dockerfiles_dir}/${executable}"
		local docker_image_tag="${docker_registry_prefix}${bin_name}"

		cp "${bin_path}" "${dockerfile_dir}"
		cd ${dockerfile_dir}

		echo "Building '${docker_image_tag}' ..."
		${DOCKER} build --pull -t "${docker_image_tag}" .

		echo "Pushing '${docker_image_tag}' ..."
		${DOCKER} push ${docker_image_tag}

		echo "Pruning the docker image ..."
		${DOCKER} image prune -f

		rm "${dockerfile_dir}/${bin_name}"
	done
}

function dsc::docker::clean() {
        local docker_registry_prefix=""
	if [[ "${DOCKER_REGISTRY}" ]]
	then
	        docker_registry_prefix="${DOCKER_REGISTRY}/"
	fi

	for executable in "${DSC_ALL_TARGETS[@]}"
	do
		local bin_name="${BIN_PREFIX}${executable}"
		local docker_image_tag="${docker_registry_prefix}${bin_name}"

		echo "Deleting '${docker_image_tag}' ..."
		if dsc::build::has_docker_image "${docker_image_tag}"; then
			${DOCKER} rmi "${docker_image_tag}"
		fi
	done
}
