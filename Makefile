
MAKE := make

.PHONY: quick-all
quick-all:
	${MAKE} quick-build
	${MAKE} docker

.PHONY: all
all:
	./build/make-all.sh

.PHONY: quick-build
quick-build:
	./build/make-executables-build.sh

.PHONY: client-codegen
client-codegen:
	${MAKE} cr-codegen
	${MAKE} grpc-codegen

.PHONY: cr-codegen
cr-codegen:
	./build/make-cr-codegen.sh

.PHONY: grpc-codegen
grpc-codegen:
	./build/make-grpc-codegen.sh

.PHONY: docker
docker:
	./build/make-docker.sh

.PHONY: pkg-update
pkg-update:
	./build/make-update-packages.sh

.PHONY: test
test:
	./build/make-test.sh

.PHONY: clean
clean:
	./build/make-executables-clean.sh

.PHONY: codegen-clean
codegen-clean:
	${MAKE} cr-clean
	${MAKE} grpc-clean

.PHONY: cr-clean
cr-clean:
	./build/make-cr-clean.sh

.PHONY: grpc-clean
grpc-clean:
	./build/make-grpc-clean.sh

.PHONY: docker-clean
docker-clean:
	./build/make-docker-clean.sh

.PHONY: distclean
distclean:
	./build/make-distclean.sh
