SHELL:=/bin/bash

DOCKER:= $(shell command -v docker 2> /dev/null)
ifndef DOCKER
DOCKER:= $(shell command -v podman 2> /dev/null)
endif

DOCKER_PUSH?=false
DOCKER_BUILD_ARGS?=
IMAGE_NAMESPACE?=quay.io/numaproj
VERSION?=latest
NUMAFLOW_EXT_BINARY_NAME:=numaflow-ext
DOCKERFILE:=Dockerfile

ifeq (${DOCKER_PUSH},true)
PUSH_OPTION="--push"
ifndef IMAGE_NAMESPACE
$(error IMAGE_NAMESPACE must be set to push images (e.g. IMAGE_NAMESPACE=quay.io/numaproj))
endif
endif

.PHONY: test
test:
	cargo test

.PHONY: all-tests
all-tests:
	cargo test --all-features

set-qemu:
	$(DOCKER) pull tonistiigi/binfmt:latest
	$(DOCKER) run --rm --privileged tonistiigi/binfmt:latest --install amd64,arm64

.PHONY: clean
clean:
	cargo clean

.PHONY: image
image: clean
	 DOCKER_BUILDKIT=1 $(DOCKER) build --build-arg "VERSION=$(VERSION)" --build-arg "BASE_IMAGE=$(DEV_BASE_IMAGE)" $(DOCKER_BUILD_ARGS) -t $(IMAGE_NAMESPACE)/$(NUMAFLOW_EXT_BINARY_NAME):$(VERSION) --target $(NUMAFLOW_EXT_BINARY_NAME) -f $(DOCKERFILE) .
	 @if [[ "$(DOCKER_PUSH)" = "true" ]]; then $(DOCKER) push $(IMAGE_NAMESPACE)/$(NUMAFLOW_EXT_BINARY_NAME):$(VERSION); fi
ifdef IMAGE_IMPORT_CMD
	 $(IMAGE_IMPORT_CMD) $(IMAGE_NAMESPACE)/$(NUMAFLOW_EXT_BINARY_NAME):$(VERSION)
endif

.PHONY: image-multi
image-multi: set-qemu
	$(DOCKER) buildx build --sbom=false --provenance=false --build-arg "BASE_IMAGE=$(RELEASE_BASE_IMAGE)" --build-arg "VERSION=$(VERSION)" $(DOCKER_BUILD_ARGS) -t $(IMAGE_NAMESPACE)/$(NUMAFLOW_EXT_BINARY_NAME):$(VERSION) --platform linux/amd64,linux/arm64 --file $(DOCKERFILE) ${PUSH_OPTION} .
