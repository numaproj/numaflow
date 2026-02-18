SHELL:=/bin/bash

PACKAGE=github.com/numaproj/numaflow
CURRENT_DIR=$(shell pwd)

HOST_ARCH=$(shell uname -m)
# Github actions instances are x86_64
ifeq ($(HOST_ARCH),x86_64)
	HOST_ARCH=amd64
endif
ifeq ($(HOST_ARCH),aarch64)
	HOST_ARCH=arm64
endif

DIST_DIR=${CURRENT_DIR}/dist
BINARY_NAME:=numaflow
DOCKERFILE:=Dockerfile
DEV_BASE_IMAGE:=debian:bookworm
RELEASE_BASE_IMAGE:=scratch

BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(shell git rev-parse HEAD)
GIT_BRANCH=$(shell git rev-parse --symbolic-full-name --verify --quiet --abbrev-ref HEAD)
GIT_TAG=$(shell if [[ -z "`git status --porcelain`" ]]; then git describe --exact-match --tags HEAD 2>/dev/null; fi)
GIT_TREE_STATE=$(shell if [[ -z "`git status --porcelain`" ]]; then echo "clean" ; else echo "dirty"; fi)

ifndef GOPATH
GOPATH=$(shell go env GOPATH)
endif

DOCKER_PUSH?=false
IMAGE_NAMESPACE?=quay.io/numaproj
VERSION?=latest
BASE_VERSION:=latest

override LDFLAGS += \
  -X ${PACKAGE}.version=${VERSION} \
  -X ${PACKAGE}.buildDate=${BUILD_DATE} \
  -X ${PACKAGE}.gitCommit=${GIT_COMMIT} \
  -X ${PACKAGE}.gitTreeState=${GIT_TREE_STATE}

ifeq (${DOCKER_PUSH},true)
PUSH_OPTION="--push"
ifndef IMAGE_NAMESPACE
$(error IMAGE_NAMESPACE must be set to push images (e.g. IMAGE_NAMESPACE=quay.io/numaproj))
endif
endif

ifneq (${GIT_TAG},)
VERSION=$(GIT_TAG)
override LDFLAGS += -X ${PACKAGE}.gitTag=${GIT_TAG}
endif

DOCKER_BUILD_ARGS=--build-arg "VERSION=$(VERSION)" --build-arg "BUILD_DATE=$(BUILD_DATE)" --build-arg "GIT_COMMIT=$(GIT_COMMIT)" --build-arg "GIT_BRANCH=$(GIT_BRANCH)" --build-arg "GIT_TAG=$(GIT_TAG)" --build-arg "GIT_TREE_STATE=$(GIT_TREE_STATE)"
DOCKER_ENV_ARGS=--env "VERSION=$(VERSION)" --env "BUILD_DATE=$(BUILD_DATE)" --env "GIT_COMMIT=$(GIT_COMMIT)" --env "GIT_BRANCH=$(GIT_BRANCH)" --env "GIT_TAG=$(GIT_TAG)" --env "GIT_TREE_STATE=$(GIT_TREE_STATE)"

# Check Python
PYTHON:=$(shell command -v python 2> /dev/null)
ifndef PYTHON
PYTHON:=$(shell command -v python3 2> /dev/null)
endif
ifndef PYTHON
$(error "Python is not available, please install.")
endif

CURRENT_CONTEXT:=$(shell [[ "`command -v kubectl`" != '' ]] && kubectl config current-context 2> /dev/null || echo "unset")
IMAGE_IMPORT_CMD:=$(shell [[ "`command -v k3d`" != '' ]] && [[ "$(CURRENT_CONTEXT)" =~ k3d-* ]] && echo "k3d image import -c `echo $(CURRENT_CONTEXT) | cut -c 5-`")
ifndef IMAGE_IMPORT_CMD
IMAGE_IMPORT_CMD:=$(shell [[ "`command -v minikube`" != '' ]] && [[ "$(CURRENT_CONTEXT)" =~ minikube* ]] && echo "minikube image load")
endif
ifndef IMAGE_IMPORT_CMD
IMAGE_IMPORT_CMD:=$(shell [[ "`command -v kind`" != '' ]] && [[ "$(CURRENT_CONTEXT)" =~ kind-* ]] && echo "kind load docker-image")
endif

DOCKER:=$(shell command -v docker 2> /dev/null)
ifndef DOCKER
DOCKER:=$(shell command -v podman 2> /dev/null)
endif

.PHONY: build
build: dist/$(BINARY_NAME)-linux-amd64.gz dist/$(BINARY_NAME)-linux-arm64.gz dist/$(BINARY_NAME)-linux-arm.gz dist/$(BINARY_NAME)-linux-ppc64le.gz dist/$(BINARY_NAME)-linux-s390x.gz dist/e2eapi

dist/$(BINARY_NAME)-%.gz: dist/$(BINARY_NAME)-%
	@[[ -e dist/$(BINARY_NAME)-$*.gz ]] || gzip -k dist/$(BINARY_NAME)-$*

dist/$(BINARY_NAME): GOARGS = GOOS= GOARCH=
dist/$(BINARY_NAME)-linux-amd64: GOARGS = GOOS=linux GOARCH=amd64
dist/$(BINARY_NAME)-linux-arm64: GOARGS = GOOS=linux GOARCH=arm64
dist/$(BINARY_NAME)-linux-arm: GOARGS = GOOS=linux GOARCH=arm
dist/$(BINARY_NAME)-linux-ppc64le: GOARGS = GOOS=linux GOARCH=ppc64le
dist/$(BINARY_NAME)-linux-s390x: GOARGS = GOOS=linux GOARCH=s390x

dist/$(BINARY_NAME):
	go build -v -ldflags '${LDFLAGS}' -o ${DIST_DIR}/$(BINARY_NAME) ./cmd

dist/e2eapi:
	CGO_ENABLED=0 GOOS=linux go build -v -ldflags '${LDFLAGS}' -o ${DIST_DIR}/e2eapi ./test/e2e-api

dist/$(BINARY_NAME)-%:
	CGO_ENABLED=0 $(GOARGS) go build -v -ldflags '${LDFLAGS}' -o ${DIST_DIR}/$(BINARY_NAME)-$* ./cmd

.PHONY: test
test:
	go test $(shell go list ./... | grep -v /vendor/ | grep -v /numaflow/test/) -race -short -v -timeout 60s

.PHONY: test-coverage
test-coverage:
	go test -v -timeout 7m -covermode=atomic -coverprofile=test/profile.cov $(shell go list ./... | grep -v /vendor/ | grep -v /numaflow/test/ | grep -v /pkg/client/ | grep -v /pkg/proto/ | grep -v /hack/)
	go tool cover -func=test/profile.cov

test-e2e:
test-kafka-e2e:
test-map-e2e:
test-reduce-one-e2e:
test-reduce-two-e2e:
test-api-e2e:
test-udsource-e2e:
test-transformer-e2e:
test-diamond-e2e:
test-sideinputs-e2e:
test-monovertex-e2e:
test-idle-source-e2e:
test-builtin-source-e2e:
test-%:
	$(MAKE) cleanup-e2e
ifndef SKIP_IMAGE_BUILD
	# Skip building image in CI since the image would have been built during "make start"
	$(MAKE) image
endif
	$(MAKE) e2eapi-image
	$(MAKE) restart-control-plane-components
	cat test/manifests/e2e-api-pod.yaml | sed 's@quay.io/numaproj/@$(IMAGE_NAMESPACE)/@' | sed 's/:latest/:$(VERSION)/' | kubectl -n numaflow-system apply -f -
	go generate $(shell find ./test/$* -name '*.go')
	go test -v -timeout 20m -count 1 --tags test -p 1 ./test/$*
	$(MAKE) cleanup-e2e

image-restart:
	$(MAKE) image
	$(MAKE) restart-control-plane-components

restart-control-plane-components:
	kubectl -n numaflow-system delete po -lapp.kubernetes.io/component=controller-manager,app.kubernetes.io/part-of=numaflow --ignore-not-found=true
	kubectl -n numaflow-system delete po -lapp.kubernetes.io/component=numaflow-ux,app.kubernetes.io/part-of=numaflow --ignore-not-found=true
	kubectl -n numaflow-system delete po -lapp.kubernetes.io/component=numaflow-webhook,app.kubernetes.io/part-of=numaflow --ignore-not-found=true

.PHONY: cleanup-e2e
cleanup-e2e:
	kubectl -n numaflow-system delete svc -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete sts -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete deploy -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete cm -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete secret -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete po -lnumaflow-e2e=true --ignore-not-found=true

# To run just one of the e2e tests by name (i.e. 'make TestCreateSimplePipeline'):
Test%:
	$(MAKE) cleanup-e2e
	$(MAKE) image e2eapi-image
	kubectl -n numaflow-system delete po -lapp.kubernetes.io/component=controller-manager,app.kubernetes.io/part-of=numaflow
	kubectl -n numaflow-system delete po e2e-api-pod  --ignore-not-found=true
	go generate $(shell find $(shell grep -rl $(*) ./test/*-e2e/*.go))
	cat test/manifests/e2e-api-pod.yaml |  sed 's@quay.io/numaproj/@$(IMAGE_NAMESPACE)/@' | sed 's/:latest/:$(VERSION)/' | kubectl -n numaflow-system apply -f -
	-go test -v -timeout 20m -count 1 --tags test -p 1 ./test/$(shell grep $(*) -R ./test | head -1 | awk -F\/ '{print $$3}' ) -run='.*/$*'
	$(MAKE) cleanup-e2e

.PHONY: ui-build
ui-build:
	./hack/build-ui.sh

.PHONY: ui-test
ui-test: ui-build
	./hack/test-ui.sh

.PHONY: image
image: clean ui-build dist/$(BINARY_NAME)-linux-$(HOST_ARCH)
ifdef GITHUB_ACTIONS
	# The binary will be built in a separate Github Actions job
	cp -pv numaflow-rs-linux-amd64 dist/numaflow-rs-linux-amd64
	cp -pv entrypoint-linux-amd64 dist/entrypoint-linux-amd64
else
	$(MAKE) build-rust-in-docker
endif
	DOCKER_BUILDKIT=1 $(DOCKER) build --build-arg "BASE_IMAGE=$(DEV_BASE_IMAGE)" $(DOCKER_BUILD_ARGS) -t $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION) --target $(BINARY_NAME) -f $(DOCKERFILE) .
	@if [[ "$(DOCKER_PUSH)" = "true" ]]; then $(DOCKER) push $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION); fi
ifdef IMAGE_IMPORT_CMD
	$(IMAGE_IMPORT_CMD) $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION)
endif

.PHONY: build-rust-in-docker
build-rust-in-docker:
	mkdir -p dist
	-$(DOCKER) container ls --all --filter=ancestor='$(IMAGE_NAMESPACE)/$(BINARY_NAME)-rust-builder:$(VERSION)' --format "{{.ID}}" | xargs $(DOCKER) rm
	-$(DOCKER) image rm $(IMAGE_NAMESPACE)/$(BINARY_NAME)-rust-builder:$(VERSION)
	DOCKER_BUILDKIT=1 $(DOCKER) build --build-arg "BASE_IMAGE=$(DEV_BASE_IMAGE)" $(DOCKER_BUILD_ARGS) -t $(IMAGE_NAMESPACE)/$(BINARY_NAME)-rust-builder:$(VERSION) --target rust-builder -f $(DOCKERFILE) . --load
	export CTR=$$($(DOCKER) create $(IMAGE_NAMESPACE)/$(BINARY_NAME)-rust-builder:$(VERSION)) && $(DOCKER) cp $$CTR:/root/numaflow dist/numaflow-rs-linux-$(HOST_ARCH) && $(DOCKER) cp $$CTR:/root/entrypoint dist/entrypoint-linux-$(HOST_ARCH) && $(DOCKER) rm $$CTR && $(DOCKER) image rm $(IMAGE_NAMESPACE)/$(BINARY_NAME)-rust-builder:$(VERSION)

.PHONY: build-rust-in-docker-multi
build-rust-in-docker-multi:
	mkdir -p dist
	docker run $(DOCKER_ENV_ARGS) -v ./dist/cargo:/root/.cargo -v ./rust/:/app/ -w /app --rm ubuntu:24.04 bash build.sh all
	cp -pv rust/target/aarch64-unknown-linux-gnu/release/numaflow dist/numaflow-rs-linux-arm64
	cp -pv rust/target/x86_64-unknown-linux-gnu/release/numaflow dist/numaflow-rs-linux-amd64
	cp -pv rust/target/aarch64-unknown-linux-gnu/release/entrypoint dist/entrypoint-linux-arm64
	cp -pv rust/target/x86_64-unknown-linux-gnu/release/entrypoint dist/entrypoint-linux-amd64

# Set Rust target triplet based on host architecture
RUST_TARGET_TRIPLET := x86_64-unknown-linux-gnu
ifeq ($(HOST_ARCH),arm64)
	RUST_TARGET_TRIPLET := aarch64-unknown-linux-gnu
endif


.PHONY: build-rust-docker-ghactions
build-rust-docker-ghactions:
	mkdir -p dist
	docker run $(DOCKER_ENV_ARGS) -v ./dist/cargo:/root/.cargo -v ./rust/:/app/ -w /app --rm ubuntu:24.04 bash build.sh $(HOST_ARCH)

image-multi: ui-build set-qemu dist/$(BINARY_NAME)-linux-arm64.gz dist/$(BINARY_NAME)-linux-amd64.gz
ifndef GITHUB_ACTIONS
	$(MAKE) build-rust-in-docker-multi
endif
	$(DOCKER) buildx build --sbom=false --provenance=false --build-arg "BASE_IMAGE=$(RELEASE_BASE_IMAGE)" $(DOCKER_BUILD_ARGS) -t $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION) --target $(BINARY_NAME) --platform linux/amd64,linux/arm64 --file $(DOCKERFILE) ${PUSH_OPTION} .

set-qemu:
	$(DOCKER) pull tonistiigi/binfmt:latest
	$(DOCKER) run --rm --privileged tonistiigi/binfmt:latest --install amd64,arm64


.PHONY: swagger
swagger:
	./hack/swagger-gen.sh ${VERSION}
	$(MAKE) api/json-schema/schema.json

api/json-schema/schema.json: api/openapi-spec/swagger.json hack/json-schema/main.go
	go run ./hack/json-schema

.PHONY: rustgen
rustgen:
	$(MAKE) --directory rust generate

.PHONY: codegen
codegen:
	./hack/generate-proto.sh
	./hack/update-codegen.sh
	./hack/openapi-gen.sh
	$(MAKE) swagger
	./hack/update-api-docs.sh
	$(MAKE) manifests
	rm -rf ./vendor
	go mod tidy
	$(MAKE) rustgen

clean:
	-rm -rf ${CURRENT_DIR}/dist

.PHONY: crds
crds:
	./hack/crdgen.sh

.PHONY: manifests
manifests: crds
	kubectl kustomize config/cluster-install > config/install.yaml
	kubectl kustomize config/namespace-install > config/namespace-install.yaml
	kubectl kustomize config/advanced-install/namespaced-controller > config/advanced-install/namespaced-controller-wo-crds.yaml
	kubectl kustomize config/advanced-install/namespaced-numaflow-server > config/advanced-install/namespaced-numaflow-server.yaml
	kubectl kustomize config/advanced-install/numaflow-server > config/advanced-install/numaflow-server.yaml
	kubectl kustomize config/advanced-install/minimal-crds > config/advanced-install/minimal-crds.yaml
	kubectl kustomize config/advanced-install/numaflow-dex-server > config/advanced-install/numaflow-dex-server.yaml
	kubectl kustomize config/extensions/webhook > config/validating-webhook-install.yaml

$(GOPATH)/bin/golangci-lint:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(GOPATH)/bin v1.64.8

.PHONY: lint
lint: $(GOPATH)/bin/golangci-lint
	go mod tidy
	$(GOPATH)/bin/golangci-lint run --fix --verbose --concurrency 4 --timeout 5m --enable goimports
	cd rust && cargo fmt -- --check || (echo "Run 'cd rust && cargo fmt' to fix formatting issues" && exit 1)

.PHONY: start
start: image
	kubectl apply -f test/manifests/numaflow-ns.yaml
	kubectl -n numaflow-system delete cm numaflow-cmd-params-config --ignore-not-found=true
	kubectl kustomize test/manifests | sed 's@quay.io/numaproj/@$(IMAGE_NAMESPACE)/@' | sed 's/:$(BASE_VERSION)/:$(VERSION)/' | kubectl -n numaflow-system apply -l app.kubernetes.io/part-of=numaflow --prune=false --force -f -
	kubectl -n numaflow-system wait -lapp.kubernetes.io/part-of=numaflow --for=condition=Ready --timeout 60s pod --all

.PHONY: e2eapi-image
e2eapi-image: clean dist/e2eapi
	DOCKER_BUILDKIT=1 $(DOCKER) build . --target e2eapi --tag $(IMAGE_NAMESPACE)/e2eapi:$(VERSION) --build-arg VERSION="$(VERSION)"
	@if [[ "$(DOCKER_PUSH)" = "true" ]]; then $(DOCKER) push $(IMAGE_NAMESPACE)/e2eapi:$(VERSION); fi
ifdef IMAGE_IMPORT_CMD
	$(IMAGE_IMPORT_CMD) $(IMAGE_NAMESPACE)/e2eapi:$(VERSION)
endif

/usr/local/bin/mkdocs:
	$(PYTHON) -m pip install mkdocs==1.3.0 mkdocs_material==8.3.9 mkdocs-embed-external-markdown==2.3.0

/usr/local/bin/lychee:
ifeq (, $(shell which lychee))
ifeq ($(shell uname),Darwin)
	brew install lychee
else
	curl -sSfL https://github.com/lycheeverse/lychee/releases/download/lychee-v0.19.1/lychee-$(shell uname -m)-unknown-linux-gnu.tar.gz | sudo tar xz -C /usr/local/bin/
endif
endif

# docs

.PHONY: docs
docs: /usr/local/bin/mkdocs docs-linkcheck
	$(PYTHON) -m mkdocs build

.PHONY: docs-serve
docs-serve: docs
	$(PYTHON) -m mkdocs serve

.PHONY: docs-linkcheck
docs-linkcheck: /usr/local/bin/lychee
	lychee --insecure --accept '100..=399,403,429' --exclude-path=CHANGELOG.md --exclude-path=USERS.md --exclude-path=./docs/APIs.md --exclude "https://localhost:*" --exclude "http://localhost:*" --exclude "http://127.0.0.1*" --exclude "https://kubernetes.io/" *.md $(shell find ./docs -name '*.md') $(shell find ./examples -name '*.yaml')

# pre-push checks

.git/hooks/%: hack/git/hooks/%
	@mkdir -p .git/hooks
	cp hack/git/hooks/$* .git/hooks/$*

.PHONY: githooks
githooks: .git/hooks/commit-msg

.PHONY: pre-push
pre-push: codegen lint
	# marker file, based on it's modification time, we know how long ago this target was run
	touch dist/pre-push

.PHONY: checksums
checksums:
	sha256sum ./dist/$(BINARY_NAME)-*.gz | awk -F './dist/' '{print $$1 $$2}' > ./dist/$(BINARY_NAME)-checksums.txt

# release - targets only available on release branch
ifneq ($(findstring release,$(GIT_BRANCH)),)

.PHONY: prepare-release
prepare-release: check-version-warning clean update-manifests-version codegen
	git status
	@git diff --quiet || printf "\n\nPlease run 'git diff' to confirm the file changes are correct.\n\n"

.PHONY: release
release: check-version-warning
	@echo
	@echo "1. Make sure you have run 'make prepare-release VERSION=$(VERSION)', and confirmed all the changes are expected."
	@echo
	@echo "2. Run following commands to commit the changes to the release branch, add give a tag."
	@echo
	@echo "git commit -am \"Update manifests to $(VERSION)\""
	@echo "git push {your-remote}"
	@echo
	@echo "git tag -a $(VERSION) -m $(VERSION)"
	@echo "git push {your-remote} $(VERSION)"
	@echo

endif

.PHONY: check-version-warning
check-version-warning:
	@if [[ ! "$(VERSION)" =~ ^v[0-9]+\.[0-9]+\.[0-9]+.*$  ]]; then echo -n "It looks like you're not using a version format like 'v1.2.3', or 'v1.2.3-rc2', that version format is required for our releases. Do you wish to continue anyway? [y/N]" && read ans && [[ $${ans:-N} = y ]]; fi

.PHONY: update-manifests-version
update-manifests-version:
	cat config/base/kustomization.yaml | sed 's/newTag: .*/newTag: $(VERSION)/' | sed 's@value: quay.io/numaproj/numaflow:.*@value: quay.io/numaproj/numaflow:$(VERSION)@' > /tmp/tmp_kustomization.yaml
	mv /tmp/tmp_kustomization.yaml config/base/kustomization.yaml
	cat config/advanced-install/namespaced-controller/kustomization.yaml | sed 's/newTag: .*/newTag: $(VERSION)/' | sed 's@value: quay.io/numaproj/numaflow:.*@value: quay.io/numaproj/numaflow:$(VERSION)@' > /tmp/tmp_kustomization.yaml
	mv /tmp/tmp_kustomization.yaml config/advanced-install/namespaced-controller/kustomization.yaml
	cat config/advanced-install/namespaced-numaflow-server/kustomization.yaml | sed 's/newTag: .*/newTag: $(VERSION)/' | sed 's@value: quay.io/numaproj/numaflow:.*@value: quay.io/numaproj/numaflow:$(VERSION)@' > /tmp/tmp_kustomization.yaml
	mv /tmp/tmp_kustomization.yaml config/advanced-install/namespaced-numaflow-server/kustomization.yaml
	cat config/advanced-install/numaflow-server/kustomization.yaml | sed 's/newTag: .*/newTag: $(VERSION)/' | sed 's@value: quay.io/numaproj/numaflow:.*@value: quay.io/numaproj/numaflow:$(VERSION)@' > /tmp/tmp_kustomization.yaml
	mv /tmp/tmp_kustomization.yaml config/advanced-install/numaflow-server/kustomization.yaml
	cat config/extensions/webhook/kustomization.yaml | sed 's/newTag: .*/newTag: $(VERSION)/' | sed 's@value: quay.io/numaproj/numaflow:.*@value: quay.io/numaproj/numaflow:$(VERSION)@' > /tmp/tmp_kustomization.yaml
	mv /tmp/tmp_kustomization.yaml config/extensions/webhook/kustomization.yaml
	cat Makefile | sed 's/^VERSION?=.*/VERSION?=$(VERSION)/' | sed 's/^BASE_VERSION:=.*/BASE_VERSION:=$(VERSION)/' > /tmp/ae_makefile
	mv /tmp/ae_makefile Makefile
