CURRENT_DIR=$(shell pwd)
DIST_DIR=${CURRENT_DIR}/dist
BINARY_NAME:=numaflow
DOCKERFILE:=Dockerfile

BUILD_DATE=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(shell git rev-parse HEAD)
GIT_BRANCH=$(shell git rev-parse --symbolic-full-name --verify --quiet --abbrev-ref HEAD)
GIT_TAG=$(shell if [ -z "`git status --porcelain`" ]; then git describe --exact-match --tags HEAD 2>/dev/null; fi)
GIT_TREE_STATE=$(shell if [ -z "`git status --porcelain`" ]; then echo "clean" ; else echo "dirty"; fi)

DOCKER_PUSH?=false
IMAGE_NAMESPACE?=quay.io/numaproj
VERSION?=v0.5.0
BASE_VERSION:=v0.5.0

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

K3D ?= $(shell [ "`command -v kubectl`" != '' ] && [ "`command -v k3d`" != '' ] && [[ "`kubectl config current-context`" =~ k3d-* ]] && echo true || echo false)

.PHONY: build
build: dist/$(BINARY_NAME)-linux-amd64.gz dist/$(BINARY_NAME)-linux-arm64.gz dist/$(BINARY_NAME)-linux-arm.gz dist/$(BINARY_NAME)-linux-ppc64le.gz dist/$(BINARY_NAME)-linux-s390x.gz dist/e2eapi

dist/$(BINARY_NAME)-%.gz: dist/$(BINARY_NAME)-%
	@[ -e dist/$(BINARY_NAME)-$*.gz ] || gzip -k dist/$(BINARY_NAME)-$*

dist/$(BINARY_NAME): GOARGS = GOOS= GOARCH=
dist/$(BINARY_NAME)-linux-amd64: GOARGS = GOOS=linux GOARCH=amd64
dist/$(BINARY_NAME)-linux-arm64: GOARGS = GOOS=linux GOARCH=arm64
dist/$(BINARY_NAME)-linux-arm: GOARGS = GOOS=linux GOARCH=arm
dist/$(BINARY_NAME)-linux-ppc64le: GOARGS = GOOS=linux GOARCH=ppc64le
dist/$(BINARY_NAME)-linux-s390x: GOARGS = GOOS=linux GOARCH=s390x

dist/$(BINARY_NAME):
	go build -v -ldflags '${LDFLAGS}' -o ${DIST_DIR}/$(BINARY_NAME) ./cmd

dist/e2eapi:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -ldflags '${LDFLAGS}' -o ${DIST_DIR}/e2eapi ./test/e2e-api

dist/$(BINARY_NAME)-%:
	CGO_ENABLED=0 $(GOARGS) go build -v -ldflags '${LDFLAGS}' -o ${DIST_DIR}/$(BINARY_NAME)-$* ./cmd

.PHONY: test
test:
	go test $(shell go list ./... | grep -v /vendor/ | grep -v /numaflow/test/) -race -short -v

.PHONY: test-coverage
test-coverage:
	go test -covermode=atomic -coverprofile=test/profile.cov.tmp $(shell go list ./... | grep -v /vendor/ | grep -v /numaflow/test/ | grep -v /pkg/client/ | grep -v /pkg/proto/ | grep -v /hack/)
	cat test/profile.cov.tmp | grep -v v1alpha1/zz_generated | grep -v v1alpha1/generated > test/profile.cov
	rm test/profile.cov.tmp
	go tool cover -func=test/profile.cov

.PHONY: test-redis
test-redis:
	go test -tags isb_redis -race -short -v ./pkg/isb/redis ./pkg/isbsvc

.PHONY: test-jetstream
test-jetstream:
	go test -tags isb_jetstream -race -short -v ./pkg/isb/jetstream

.PHONY: test-coverage-with-isb
test-coverage-with-isb:
	go test -covermode=atomic -coverprofile=test/profile.cov.tmp  -tags=isb_redis,isb_jetstream $(shell go list ./... | grep -v /vendor/ | grep -v /numaflow/test/ | grep -v /pkg/client/ | grep -v /pkg/proto/ | grep -v /hack/)
	cat test/profile.cov.tmp | grep -v v1alpha1/zz_generated | grep -v v1alpha1/generated > test/profile.cov
	rm test/profile.cov.tmp
	go tool cover -func=test/profile.cov

.PHONY: test-coverage-with-jetstream
test-coverage-with-jetstream:
	go test -covermode=atomic -coverprofile=test/profile.cov.tmp  -tags isb_jetstream $(shell go list ./... | grep -v /vendor/ | grep -v /numaflow/test/ | grep -v /pkg/client/ | grep -v /pkg/proto/ | grep -v /hack/)
	cat test/profile.cov.tmp | grep -v v1alpha1/zz_generated | grep -v v1alpha1/generated > test/profile.cov
	rm test/profile.cov.tmp
	go tool cover -func=test/profile.cov

.PHONY: test-code
test-code:
	go test -tags=isb_redis,isb_jetstream -race -v $(shell go list ./... | grep -v /vendor/ | grep -v /numaflow/test/)

test-e2e:
test-kafka-e2e:
test-%: 
	$(MAKE) image e2eapi-image
	kubectl -n numaflow-system delete po -lapp.kubernetes.io/component=controller-manager,app.kubernetes.io/part-of=numaflow
	kubectl -n numaflow-system delete po e2e-api-pod  --ignore-not-found=true
	cat test/manifests/e2e-api-pod.yaml |  sed 's@quay.io/numaproj/@$(IMAGE_NAMESPACE)/@' | sed 's/:$(BASE_VERSION)/:$(VERSION)/' | kubectl -n numaflow-system apply -f -
	go generate $(shell find ./test/$* -name '*.go')
	-go test -v -timeout 10m -count 1 --tags test -p 1 ./test/$*
	$(MAKE) cleanup-e2e
	

.PHONY: cleanup-e2e
cleanup-e2e:
	kubectl -n numaflow-system delete svc -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete sts -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete deploy -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete cm -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete secret -lnumaflow-e2e=true --ignore-not-found=true
	kubectl -n numaflow-system delete po -lnumaflow-e2e=true --ignore-not-found=true

.PHONY: image
image: clean dist/$(BINARY_NAME)-linux-amd64
	DOCKER_BUILDKIT=1 docker build --build-arg "ARCH=amd64" -t $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION)  --target $(BINARY_NAME) -f $(DOCKERFILE) .
	@if [ "$(DOCKER_PUSH)" = "true" ]; then docker push $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION); fi
ifeq ($(K3D),true)
	k3d image import $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION)
endif

image-linux-%: dist/$(BINARY_NAME)-linux-$*
	DOCKER_BUILDKIT=1 docker build --build-arg "ARCH=$*" -t $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION)-linux-$* --platform "linux/$*" --target $(BINARY_NAME) -f $(DOCKERFILE) .
	@if [ "$(DOCKER_PUSH)" = "true" ]; then docker push $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION)-linux-$*; fi

image-multi: set-qemu dist/$(BINARY_NAME)-linux-arm64.gz dist/$(BINARY_NAME)-linux-amd64.gz
	docker buildx build --tag $(IMAGE_NAMESPACE)/$(BINARY_NAME):$(VERSION) --target $(BINARY_NAME) --platform linux/amd64,linux/arm64 --file ./Dockerfile ${PUSH_OPTION} .

set-qemu:
	docker pull tonistiigi/binfmt:latest
	docker run --rm --privileged tonistiigi/binfmt:latest --install amd64,arm64

.PHONY: codegen
codegen:
	./hack/generate-proto.sh
	./hack/update-codegen.sh
	./hack/update-api-docs.sh
	$(MAKE) manifests
	rm -rf ./vendor
	go mod tidy

clean:
	-rm -rf ${CURRENT_DIR}/dist

.PHONY: crds
crds:
	./hack/crdgen.sh	

.PHONY: manifests
manifests: crds
	kubectl kustomize config/cluster-install > config/install.yaml

$(GOPATH)/bin/golangci-lint:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b `go env GOPATH`/bin v1.46.2

.PHONY: lint
lint: $(GOPATH)/bin/golangci-lint
	go mod tidy
	golangci-lint run --fix --verbose --concurrency 4 --timeout 5m

.PHONY: start
start: image
	kubectl apply -f test/manifests/numaflow-ns.yaml
	kubectl kustomize test/manifests | sed 's@quay.io/numaproj/@$(IMAGE_NAMESPACE)/@' | sed 's/:$(BASE_VERSION)/:$(VERSION)/' | kubectl -n numaflow-system apply -l app.kubernetes.io/part-of=numaflow --prune=false --force -f -
	kubectl -n numaflow-system wait --for=condition=Ready --timeout 60s pod --all


.PHONY: e2eapi-image
e2eapi-image: clean dist/e2eapi
	DOCKER_BUILDKIT=1 docker build . --build-arg "ARCH=amd64" --platform=linux/amd64 --target e2eapi --tag $(IMAGE_NAMESPACE)/e2eapi:$(VERSION) --build-arg VERSION="$(VERSION)"
	@if [ "$(DOCKER_PUSH)" = "true" ]; then docker push $(IMAGE_NAMESPACE)/e2eapi:$(VERSION); fi
ifeq ($(K3D),true)
	k3d image import $(IMAGE_NAMESPACE)/e2eapi:$(VERSION)
endif

.PHONY: checksums
checksums:
	for f in ./dist/$(BINARY_NAME)-*.gz; do openssl dgst -sha256 "$$f" | awk ' { print $$2 }' > "$$f".sha256 ; done

# release - targets only available on release branch
ifneq ($(findstring release,$(GIT_BRANCH)),)

.PHONY: prepare-release
prepare-release: check-version-warning clean update-manifests-version codegen
	git status
	@git diff --quiet || echo "\n\nPlease run 'git diff' to confirm the file changes are correct.\n"

.PHONY: release
release: check-version-warning
	@echo "\n1. Make sure you have run 'VERSION=$(VERSION) make prepare-release', and confirmed all the changes are expected."
	@echo "\n2. Run following commands to commit the changes to the release branch, add give a tag.\n"
	@echo "git commit -am \"Update manifests to $(VERSION)\""
	@echo "git push {your-remote}\n"
	@echo "git tag -a $(VERSION) -m $(VERSION)"
	@echo "git push {your-remote} $(VERSION)\n"

endif

.PHONY: check-version-warning
check-version-warning:
	@if [[ ! "$(VERSION)" =~ ^v[0-9]+\.[0-9]+\.[0-9]+.*$  ]]; then echo -n "It looks like you're not using a version format like 'v1.2.3', or 'v1.2.3-rc2', that version format is required for our releases. Do you wish to continue anyway? [y/N]" && read ans && [ $${ans:-N} = y ]; fi

.PHONY: update-manifests-version
update-manifests-version:
	cat config/base/kustomization.yaml | sed 's/newTag: .*/newTag: $(VERSION)/' | sed 's@value: quay.io/numaproj/numaflow:.*@value: quay.io/numaproj/numaflow:$(VERSION)@' > /tmp/base_kustomization.yaml
	mv /tmp/base_kustomization.yaml config/base/kustomization.yaml
	cat Makefile | sed 's/^VERSION?=.*/VERSION?=$(VERSION)/' | sed 's/^BASE_VERSION:=.*/BASE_VERSION:=$(VERSION)/' > /tmp/ae_makefile
	mv /tmp/ae_makefile Makefile

