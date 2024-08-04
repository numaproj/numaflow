VERSION ?= main

SDK_VERSION := $(shell if [[ "$(VERSION)" =~ ^v[0-9]+\.[0-9]+\.[0-9]+.*  ]]; then echo ${VERSION} | cut -c2-; elif [[ "$(VERSION)" =~ ^main$$  ]]; then echo 0.0.0-pre; else echo $(VERSION); fi)

GENERATOR_VERSION := v7.7.0

DOCKER = docker run --rm -v `pwd -P`:/base --workdir /base

publish: generate
	echo TODO

generate:
	rm -Rf ./docs ./test ./numaflow/models/ ./numaflow/model/
	mkdir -p ./dist
	cat ../../api/openapi-spec/swagger.json | ../../hack/swaggerfilter.py io.numaproj.numaflow | \
		sed 's/io.k8s.api.core.//' | \
		sed 's/io.k8s.apimachinery.pkg.apis.meta.//' | \
		sed 's/io.k8s.apimachinery.pkg.api.//' | \
		sed 's/io.numaproj.numaflow.v1alpha1.//' \
		> ./dist/swagger.json
	$(DOCKER) openapitools/openapi-generator-cli:$(GENERATOR_VERSION) \
		generate \
		-i /base/dist/swagger.json \
		-g rust \
		-o /base \
		--remove-operation-id-prefix \
		--model-name-prefix '' \
		--model-name-suffix '' \
		--global-property packageName=numaflow \
		--additional-properties packageVersion=${SDK_VERSION} \
		--import-mappings V1PeriodAffinity="k8s_openapi::api::core::v1::Affinity" \
		--type-mappings V1PeriodAffinity="k8s_openapi::api::core::v1::Affinity" \
		--generate-alias-as-model \
