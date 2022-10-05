GIT_TAG               := $(shell git describe --exact-match --tags --abbrev=0 2> /dev/null || echo untagged)
ifeq ($(GIT_TAG),untagged)
# "SNAPSHOT" is "latest" for Java
VERSION               := 0.0.0-SNAPSHOT
else
# remove the "v" prefix, not allowed
VERSION               := $(GIT_TAG:v=)
endif

# work dir
WD                    := $(shell echo "`pwd`/client")

DOCKER = docker run --rm -v $(WD):/wd --workdir /wd
MVN = $(DOCKER) -v $(HOME)/.m2:/root/.m2 -e JAVA_SDK_MAVEN_PASSWORD=${JAVA_SDK_MAVEN_PASSWORD} maven:3-openjdk-8 mvn -s settings.xml
CHOWN = chown -R $(shell id -u):$(shell id -g)

publish: generate
	# https://help.github.com/en/packages/using-github-packages-with-your-projects-ecosystem/configuring-apache-maven-for-use-with-github-packages
	$(MVN) deploy -DskipTests -DaltDeploymentRepository=github::default::https://maven.pkg.github.com/argoproj/argo-workflows

generate:
	rm -Rf $(WD)
	mkdir -p $(WD)
	cp settings.xml $(WD)/settings.xml
	cat ../../api/openapi-spec/swagger.json | ./hack/swaggerfilter.py io.numaproj.numaflow | \
		sed 's/io.k8s.api.core.//' | \
		sed 's/io.k8s.apimachinery.pkg.apis.meta.//' | \
		sed 's/io.k8s.apimachinery.pkg.api.//' | \
		sed 's/io.numaproj.numaflow.v1alpha1.//' \
		> $(WD)/swagger.json
	$(DOCKER) openapitools/openapi-generator-cli:v5.2.1 \
		generate \
		-i /wd/swagger.json \
		-g java \
		-o /wd \
		-p hideGenerationTimestamp=true \
		-p serializationLibrary=jsonb \
		-p dateLibrary=java8 \
		--api-package io.numaproj.numaflowflow.apis \
		--invoker-package io.numaproj.numaflow \
		--model-package io.numaproj.numaflow.models \
		--skip-validate-spec \
		--group-id io.numaproj.numaflow \
		--artifact-id numaflow-client-java \
		--artifact-version $(VERSION) \
		--import-mappings V1Time=java.time.Instant \
		--type-mappings V1Time=java.time.Instant \
		--import-mappings Time=java.time.Instant \
		--import-mappings V1Duration=java.time.Duration \
		--type-mappings V1Duration=java.time.Duration \
		--import-mappings V1Affinity=io.kubernetes.client.openapi.models.V1Affinity \
		--import-mappings V1ConfigMapKeySelector=io.kubernetes.client.openapi.models.V1ConfigMapKeySelector \
		--import-mappings V1Container=io.kubernetes.client.openapi.models.V1Container \
		--import-mappings V1ContainerPort=io.kubernetes.client.openapi.models.V1ContainerPort \
		--import-mappings V1EnvFromSource=io.kubernetes.client.openapi.models.V1EnvFromSource \
		--import-mappings V1EnvVar=io.kubernetes.client.openapi.models.V1EnvVar \
		--import-mappings V1HostAlias=io.kubernetes.client.openapi.models.V1HostAlias \
		--import-mappings V1Lifecycle=io.kubernetes.client.openapi.models.V1Lifecycle \
		--import-mappings V1ListMeta=io.kubernetes.client.openapi.models.V1ListMeta \
		--import-mappings V1LocalObjectReference=io.kubernetes.client.openapi.models.V1LocalObjectReference \
		--import-mappings V1ObjectMeta=io.kubernetes.client.openapi.models.V1ObjectMeta \
		--import-mappings V1ObjectReference=io.kubernetes.client.openapi.models.V1ObjectReference \
		--import-mappings V1PersistentVolumeClaim=io.kubernetes.client.openapi.models.V1PersistentVolumeClaim \
		--import-mappings V1PodDisruptionBudgetSpec=io.kubernetes.client.openapi.models.V1beta1PodDisruptionBudgetSpec \
		--import-mappings V1PodDNSConfig=io.kubernetes.client.openapi.models.V1PodDNSConfig \
		--import-mappings V1PodSecurityContext=io.kubernetes.client.openapi.models.V1PodSecurityContext \
		--import-mappings V1Probe=io.kubernetes.client.openapi.models.V1Probe \
		--import-mappings V1ResourceRequirements=io.kubernetes.client.openapi.models.V1ResourceRequirements \
		--import-mappings V1SecretKeySelector=io.kubernetes.client.openapi.models.V1SecretKeySelector \
		--import-mappings V1SecurityContext=io.kubernetes.client.openapi.models.V1SecurityContext \
		--import-mappings V1Toleration=io.kubernetes.client.openapi.models.V1Toleration \
		--import-mappings V1Volume=io.kubernetes.client.openapi.models.V1Volume \
		--import-mappings V1VolumeDevice=io.kubernetes.client.openapi.models.V1VolumeDevice \
		--import-mappings V1VolumeMount=io.kubernetes.client.openapi.models.V1VolumeMount \
		--import-mappings V1Condition=io.kubernetes.client.openapi.models.V1Condition \
		--import-mappings ResourceQuantity=io.kubernetes.client.custom.Quantity \
		--type-mappings ResourceQuantity=io.kubernetes.client.custom.Quantity \
		--generate-alias-as-model
	# https://vsupalov.com/docker-shared-permissions/#set-the-docker-user-when-running-your-container
	$(CHOWN) $(WD) || sudo $(CHOWN) $(WD)
	# adding kubernetes-client
	cd client && sed 's/<dependencies>/<dependencies><dependency><groupId>io.kubernetes<\/groupId><artifactId>client-java<\/artifactId><version>16.0.0<\/version><\/dependency>/g' pom.xml > tmp && mv tmp pom.xml

	docker run -v ~/.m2:/root/.m2 -v $(WD):/wd -w /wd maven:3-openjdk-8 \
		mvn install -DskipTests -Dmaven.javadoc.skip
client/pom.xml: generate

install: client/pom.xml
	mvn -f client install
