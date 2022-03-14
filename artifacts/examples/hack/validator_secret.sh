#! /usr/bin/env bash

# Copyright (c) 2022 Fujitsu Limited

set -eu

EXAMPLES_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
VALIDATION_DIR="${EXAMPLES_ROOT}/kustomize/admission/validation"

CAT=cat
EGREP=egrep
KUBECTL=kubectl
OPENSSL=openssl
READLINK=readlink
RM=rm

CA_CERTIFICATE=ca.crt
CA_CN=validating.admission.webhook.ca
CA_PRIVATE_KEY=ca.key
CN_DNS_IP=policy-validator.dsc.svc
CSR_CONFIG=csr.config
NS_DSC=dsc
SECRET_NAME=webhook-policy-validation-tls
SERVER_CERTIFICATE=server.crt
SERVER_CERTIFICATE_SIGN_REQUEST=server.csr
SERVER_PRIVATE_KEY=server.key
TLS_OUTPUT_DIR=/tmp/tls
WEBHOOK_NAME=policy-validator.dsc.io

function usage() {
echo "usage: $0 [generate|remove]"
exit 1
}

function generate_ca_certificate() {
	${OPENSSL} genrsa -out "${CA_PRIVATE_KEY}" 4096
	${OPENSSL} req -x509 -new -nodes -key "${CA_PRIVATE_KEY}" \
		-subj "/CN=${CA_CN}" -out "${CA_CERTIFICATE}"
}

function generate_server_csr_config() {
	${CAT} <<EOF >"${CSR_CONFIG}"
[ req ]
default_bits = 4096
prompt = no
default_md = sha256
req_extensions = req_ext
distinguished_name = dn

[ dn ]
CN = "${CA_CN}"

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = "${CN_DNS_IP}"

[ v3_ext ]
authorityKeyIdentifier=keyid,issuer:always
basicConstraints=CA:FALSE
keyUsage=keyEncipherment,dataEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=@alt_names
EOF
}

function generate_certificates {
	if [[ ! -e "${TLS_OUTPUT_DIR}" ]]; then
	    mkdir -p "${TLS_OUTPUT_DIR}"
	fi

	pushd "${TLS_OUTPUT_DIR}"

	generate_ca_certificate

	${OPENSSL} genrsa -out "${SERVER_PRIVATE_KEY}" 4096

	generate_server_csr_config

	${OPENSSL} req -new -key "${SERVER_PRIVATE_KEY}" \
		-out "${SERVER_CERTIFICATE_SIGN_REQUEST}" \
		-config "${CSR_CONFIG}"

	${OPENSSL} x509 -req -in "${SERVER_CERTIFICATE_SIGN_REQUEST}" \
		-CA "${CA_CERTIFICATE}" -CAkey "${CA_PRIVATE_KEY}" \
		-CAcreateserial -extensions v3_ext -extfile "${CSR_CONFIG}" \
		-out "${SERVER_CERTIFICATE}"

	popd
}

function generate_k8s_manifests() {
	${CAT} <<EOF >"${abs_validation_dir}/overlays/admissionregistration.yaml"
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: policy-validation-webhook
webhooks:
  - name: ${WEBHOOK_NAME}
    clientConfig:
      caBundle: `${OPENSSL} base64 -A <"${TLS_OUTPUT_DIR}/${SERVER_CERTIFICATE}"`
EOF

	${CAT} <<EOF >"${abs_validation_dir}/overlays/webhook-tls.yaml"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: policy-validator
  namespace: ${NS_DSC}
spec:
  template:
    spec:
      volumes:
      - name: webhook-tls-certs
        secret:
          secretName: ${SECRET_NAME}
EOF
}

function generate() {
	echo "${READLINK} ${VALIDATION_DIR}"
	abs_validation_dir=$(${READLINK} -f "${VALIDATION_DIR}")

	generate_certificates

	generate_k8s_manifests

	ns_dsc=$(${KUBECTL} get namespace "${NS_DSC}" 2> /dev/null || true)
	if [ -z "${ns_dsc}" ]
	then
		${KUBECTL} create namespace "${NS_DSC}"
	fi

	if ${KUBECTL} get -n "${NS_DSC}" secret | \
		${EGREP} --quiet "^${SECRET_NAME} "
	then
		${KUBECTL} delete -n "${NS_DSC}" secret "${SECRET_NAME}"
	fi

	${KUBECTL} create -n "${NS_DSC}" secret tls "${SECRET_NAME}" \
		--cert "${TLS_OUTPUT_DIR}/${SERVER_CERTIFICATE}" \
		--key "${TLS_OUTPUT_DIR}/${SERVER_PRIVATE_KEY}"

	${RM} -r "${TLS_OUTPUT_DIR}"
}

function remove() {
	${KUBECTL} delete -n "${NS_DSC}" secret "${SECRET_NAME}"
		
	abs_validation_dir=$(${READLINK} -f "${VALIDATION_DIR}")
	${RM} "${abs_validation_dir}/overlays/admissionregistration.yaml"
	${RM} "${abs_validation_dir}/overlays/webhook-tls.yaml"
}

if [ $# -ne 1 ]; then
	usage
fi

case $1 in
	generate)
		generate
		;;

	remove)
		remove
		;;

	*)
		usage
		;;
esac
