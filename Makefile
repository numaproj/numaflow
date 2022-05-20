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
VERSION?=latest

override LDFLAGS += \
  -X ${PACKAGE}.version=${VERSION} \
  -X ${PACKAGE}.buildDate=${BUILD_DATE} \
  -X ${PACKAGE}.gitCommit=${GIT_COMMIT} \
  -X ${PACKAGE}.gitTreeState=${GIT_TREE_STATE}

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
	kubectl -n numaflow-system delete po -lapp=controller-manager,app.kubernetes.io/part-of=numaflow
	kubectl -n numaflow-system delete po e2e-api-pod  --ignore-not-found=true
	kubectl -n numaflow-system apply -f  test/manifests/e2e-api-pod.yaml
	go generate $(shell find ./test/$* -name '*.go')
	go test -v -timeout 10m -count 1 --tags test -p 1 ./test/$*



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
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b `go env GOPATH`/bin v1.42.1

.PHONY: lint
lint: $(GOPATH)/bin/golangci-lint
	go mod tidy
	golangci-lint run --fix --verbose --concurrency 4 --timeout 5m

.PHONY: start
start: image
	kubectl apply -f test/manifests/numaflow-ns.yaml
	kubectl kustomize test/manifests | kubectl -n numaflow-system apply -l app.kubernetes.io/part-of=numaflow --prune=false --force -f -
	kubectl -n numaflow-system wait --for=condition=Ready --timeout 60s pod --all


.PHONY: e2eapi-image
e2eapi-image: clean dist/e2eapi
	DOCKER_BUILDKIT=1 docker build . --build-arg "ARCH=amd64" --platform=linux/amd64 --target e2eapi --tag $(IMAGE_NAMESPACE)/e2eapi:$(VERSION) --build-arg VERSION="$(VERSION)"
ifeq ($(K3D),true)
	k3d image import $(IMAGE_NAMESPACE)/e2eapi:$(VERSION)
endif
