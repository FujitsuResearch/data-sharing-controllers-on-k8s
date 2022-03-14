#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -eu

AWK=awk
CAT=cat
KUBECTL=kubectl
OPENSSL=openssl
RM=rm

BASE64_CSR=csr.yaml
CERTIFICATE_AUTHORITY=/etc/kubernetes/pki/ca.crt
CERTIFICATE_SIGN_REQUEST=ca.csr
CLIENT_CERTIFICATE=k8s.crt
CLIENT_PRIVATE_KEY=ca.key
CLUSTER_NAME=kubernetes
CONFIG=config
PREFIX_CSR_RESOURCE_NAME=user-request
USER_OUTPUT_DIR=/tmp/k8s_user_account

function usage() {
echo "usage: $0 generate [user name] [API server URL]"
echo "       $0 remove [user name]"
exit 1
}

function generate() {
	if [[ ! -e "${USER_OUTPUT_DIR}" ]]; then
	    mkdir -p "${USER_OUTPUT_DIR}"
	fi

	pushd "${USER_OUTPUT_DIR}"

	${OPENSSL} genrsa -out "${CLIENT_PRIVATE_KEY}" 4096

	${OPENSSL} req -new -key "${CLIENT_PRIVATE_KEY}" \
		-out "${CERTIFICATE_SIGN_REQUEST}" \
		 -subj "/CN=$1"

	${CAT} <<EOF >"${BASE64_CSR}"
apiVersion: certificates.k8s.io/v1
kind: CertificateSigningRequest
metadata:
  name: "${PREFIX_CSR_RESOURCE_NAME}-$1"
spec:
  groups:
  - system:authenticated
  request: $(${CAT} "${CERTIFICATE_SIGN_REQUEST}" | base64 | tr -d '\n')
  signerName: kubernetes.io/kube-apiserver-client
  usages:
  - digital signature
  - key encipherment
  - client auth
EOF

	${KUBECTL} create -f "${BASE64_CSR}"
	${KUBECTL} certificate approve "${PREFIX_CSR_RESOURCE_NAME}-$1"

	${KUBECTL} get csr "${PREFIX_CSR_RESOURCE_NAME}-$1" \
		-o jsonpath='{.status.certificate}' | \
		base64 -d > "${CLIENT_CERTIFICATE}"

	dns_ip="$(echo "$2" | ${AWK} -F[/:] '{print $4}')"
	echo "dns_ip: ${dns_ip}"

	${KUBECTL} config --kubeconfig="${CONFIG}" set-cluster kubernetes \
		--certificate-authority=${CERTIFICATE_AUTHORITY} --server=$2 \
		--embed-certs=true
	${KUBECTL} config --kubeconfig="${CONFIG}" set-credentials $1 \
		--client-certificate="${CLIENT_CERTIFICATE}" \
		--client-key="${CLIENT_PRIVATE_KEY}" --embed-certs=true
	${KUBECTL} config --kubeconfig="${CONFIG}" set-context \
		"$1@${CLUSTER_NAME}" --cluster="${CLUSTER_NAME}" --user=$1
	${KUBECTL} config --kubeconfig="${CONFIG}" use-context "$1@${CLUSTER_NAME}"

	popd
}

function remove() {
	${KUBECTL} delete \
		certificatesigningrequests/"${PREFIX_CSR_RESOURCE_NAME}-$1"

	${RM} -r "${USER_OUTPUT_DIR}"
}

if [ $# -eq 0 ]; then
	usage
fi

case $1 in
	generate)
		if [ $# -ne 3 ]; then
		        usage
		fi

		generate "${@:2}"
		;;

	remove)
		if [ $# -ne 2 ]; then
		        usage
		fi

		remove "$2"
		;;

	*)
		usage
		;;
esac
