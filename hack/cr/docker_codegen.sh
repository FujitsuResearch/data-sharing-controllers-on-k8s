#!/usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -e

CUSTOM_RESOURCE_ROOT="${DSC_ROOT}/pkg/apis/cr"
CURRENT_WORKING_DIRECTORY="$(pwd)"
GEN_SCRIPT="${CURRENT_WORKING_DIRECTORY}/generate-groups.sh"

echo "... Generating client codes in docker ..."

cd ${DSC_DIR}

for crn in ${CUSTOM_RESOURCE_ROOT}/*
do
	cr_name=$(basename ${crn})
	for crv in ${crn}/v*
	do
		cr_version=$(basename ${crv})
		echo -e "\tfor ${cr_name}:${cr_version}"
		${GEN_SCRIPT} "deepcopy,client,informer,lister" \
		    "${GOMOD_DSC}/pkg/client/cr/${cr_name}" \
		    "${GOMOD_DSC}/pkg/apis/cr" \
		    ${cr_name}:${cr_version} \
		    --go-header-file \
		    "${CURRENT_WORKING_DIRECTORY}/hack/boilerplate.go.txt"
	done
done
