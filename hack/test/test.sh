#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

readonly TEST_IMAGE_NAME="shadowy-test:latest"

function dsc::test::run() {
	if ! dsc::build::has_docker
	then
		echo "error: '${DOCKER}' is required."
		exit 1
	fi

	echo "Building a docker image for data sharing controllers tests ..."
	local -r gomod_file="go.mod"
	local -r gosum_file="go.sum"
	local -r gomod_path="${DSC_ROOT}/${gomod_file}"
	local -r gosum_path="${DSC_ROOT}/${gosum_file}"
	local -r test_dir="${HACK_DIR}/test"

	cp "${gomod_path}" "${test_dir}"
	cp "${gosum_path}" "${test_dir}"
	${DOCKER} build -f "${test_dir}/Dockerfile" \
	    --build-arg HTTP_PROXY="${http_proxy}" \
	    --build-arg HTTPS_PROXY="${https_proxy}" \
	    --build-arg NO_PROXY="${no_proxy}" \
	    --pull -t "${TEST_IMAGE_NAME}" \
	    "${DSC_ROOT}"
	rm "${test_dir}/${gomod_file}"
	rm "${test_dir}/${gosum_file}"

	echo "Running data sharing controllers tests ..."
	local -r dsc_root="/src/dsc"
	if [ -z "${GO_TEST}" ]
	then
		${DOCKER} run --rm -v "$(realpath ./):${dsc_root}" \
		    --net=host -e KAFKA_BROKER_ADDRESS="localhost:9093" \
		    -e TFLAGS="${TFLAGS}" --privileged=true \
		    "${TEST_IMAGE_NAME}"
	else
		${DOCKER} run --rm -v "$(realpath ./):${dsc_root}" \
		    --net=host -e KAFKA_BROKER_ADDRESS="localhost:9093" \
		    --privileged=true "${TEST_IMAGE_NAME}" ${GO_TEST}
	fi

	${SUDO} chown "${USER}:${USER}" -R "${DSC_ROOT}/cmd"
	${SUDO} chown "${USER}:${USER}" -R "${DSC_ROOT}/pkg"
	
	echo "Pruning the docker image ..."
	${DOCKER} image prune -f
}

function dsc::test::clean() {
	if dsc::build::has_docker_image "${TEST_IMAGE_NAME}"
	then
		echo "Deleting ${TEST_IMAGE_NAME}..."
		${DOCKER} rmi ${TEST_IMAGE_NAME}
	fi
}
