VERSION ?= main

SDK_VERSION := $(shell if [[ "$(VERSION)" =~ ^v[0-9]+\.[0-9]+\.[0-9]+.*  ]]; then echo ${VERSION} | cut -c2-; elif [[ "$(VERSION)" =~ ^main$$  ]]; then echo 0.0.0-pre; else echo $(VERSION); fi)

# Somehow type-mappings stopped working starting from v7.3.0
GENERATOR_VERSION := v7.3.0

DOCKER = docker run --rm -v `pwd -P`:/base --workdir /base

publish: generate
	echo TODO

generate:
	rm -Rf ./docs ./test ./numaflow/models/ ./numaflow/model/
	mkdir -p ./dist
	cat ../../api/openapi-spec/swagger.json | ../../hack/swaggerfilter.py io.numaproj.numaflow | \
		sed 's/io.k8s.api.core.v1./CoreV1/' | \
		sed 's/io.k8s.apimachinery.pkg.apis.meta.v1./MetaV1/' | \
		sed 's/io.k8s.apimachinery.pkg.api.resource.Quantity/ResourceQuantity/' | \
		sed 's/io.numaproj.numaflow.v1alpha1.//' \
		> ./dist/swagger.json
	python3 ./hack/update_swagger.py ./dist/swagger.json
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
		--type-mappings CoreV1Affinity="k8s_openapi::api::core::v1::Affinity" \
		--type-mappings CoreV1SecretKeySelector="k8s_openapi::api::core::v1::SecretKeySelector" \
		--type-mappings CoreV1EnvVar="k8s_openapi::api::core::v1::EnvVar" \
		--type-mappings CoreV1EnvFromSource="k8s_openapi::api::core::v1::EnvFromSource" \
		--type-mappings CoreV1Lifecycle="k8s_openapi::api::core::v1::Lifecycle" \
		--type-mappings CoreV1Probe="k8s_openapi::api::core::v1::Probe" \
		--type-mappings CoreV1ContainerPort="k8s_openapi::api::core::v1::ContainerPort" \
		--type-mappings CoreV1ResourceRequirements="k8s_openapi::api::core::v1::ResourceRequirements" \
		--type-mappings CoreV1SecurityContext="k8s_openapi::api::core::v1::SecurityContext" \
		--type-mappings CoreV1VolumeDevice="k8s_openapi::api::core::v1::VolumeDevice" \
		--type-mappings CoreV1VolumeMount="k8s_openapi::api::core::v1::VolumeMount" \
		--type-mappings CoreV1Toleration="k8s_openapi::api::core::v1::Toleration" \
		--type-mappings CoreV1PodSecurityContext="k8s_openapi::api::core::v1::PodSecurityContext" \
		--type-mappings CoreV1LocalObjectReference="k8s_openapi::api::core::v1::LocalObjectReference" \
		--type-mappings CoreV1PodDNSConfig="k8s_openapi::api::core::v1::PodDNSConfig" \
		--type-mappings CoreV1ContainerResizePolicy="k8s_openapi::api::core::v1::ContainerResizePolicy" \
		--type-mappings CoreV1Container="k8s_openapi::api::core::v1::Container" \
		--type-mappings CoreV1Volume="k8s_openapi::api::core::v1::Volume" \
		--type-mappings CoreV1EmptyDirVolumeSource="k8s_openapi::api::core::v1::EmptyDirVolumeSource" \
		--type-mappings MetaV1Duration="kube::core::Duration" \
		--type-mappings MetaV1ListMeta="k8s_openapi::apimachinery::pkg::apis::meta::v1::ListMeta" \
		--type-mappings MetaV1Condition="k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition" \
		--type-mappings MetaV1Time="k8s_openapi::apimachinery::pkg::apis::meta::v1::Time" \
		--type-mappings MetaV1ObjectMeta="k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta" \
		--type-mappings ResourceQuantity="k8s_openapi::apimachinery::pkg::api::resource::Quantity" \
		--generate-alias-as-model

	sed 's/\[dependencies\]/[dependencies]\nkube = "0.93.1"\nk8s-openapi = { version = "0.22.0", features = ["v1_29"] }/g' Cargo.toml  > tmp && mv tmp Cargo.toml