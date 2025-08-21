# Validating Admission Webhook

This validating webhook will prevent disallowed spec changes to immutable fields of Numaflow CRDs including Pipelines and InterStepBufferServices.
It also prevents creating a CRD with a faulty spec.
The user sees an error immediately returned by the server explaining why the request was denied.

## Installation

To install the validating webhook, run the following command line:

```shell
kubectl apply -n numaflow-system -f https://raw.githubusercontent.com/numaproj/numaflow/stable/config/validating-webhook-install.yaml
```

## Examples

Currently, the validating webhook prevents updating the pvc storage size, for example.

Example spec:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream:
    version: latest
    persistence:
      volumeSize: 3Gi // Update it will cause the error below
```

```shell
Error from server (BadRequest): error when applying patch:
{"metadata":{"annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{\"apiVersion\":\"numaflow.numaproj.io/v1alpha1\",\"kind\":\"InterStepBufferService\",\"metadata\":{\"annotations\":{},\"name\":\"default\",\"namespace\":\"numaflow-system\"},\"spec\":{\"jetstream\":{\"persistence\":{\"volumeSize\":\"5Gi\"},\"version\":\"latest\"}}}\n"}},"spec":{"jetstream":{"persistence":{"volumeSize":"5Gi"}}}}
to:
Resource: "numaflow.numaproj.io/v1alpha1, Resource=interstepbufferservices", GroupVersionKind: "numaflow.numaproj.io/v1alpha1, Kind=InterStepBufferService"
Name: "default", Namespace: "numaflow-system"
for: "examples/0-isbsvc-jetstream.yaml": error when patching "examples/0-isbsvc-jetstream.yaml": admission webhook "webhook.numaflow.numaproj.io" denied the request: can not change persistence of Jetstream ISB Service
```

There is also validation that prevents the `interStepBufferServiceName` of a Pipeline from being updated.

```shell
Error from server (BadRequest): error when applying patch:
{"metadata":{"annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{\"apiVersion\":\"numaflow.numaproj.io/v1alpha1\",\"kind\":\"Pipeline\",\"metadata\":{\"annotations\":{},\"name\":\"simple-pipeline\",\"namespace\":\"numaflow-system\"},\"spec\":{\"edges\":[{\"from\":\"in\",\"to\":\"cat\"},{\"from\":\"cat\",\"to\":\"out\"}],\"interStepBufferServiceName\":\"change\",\"vertices\":[{\"name\":\"in\",\"source\":{\"generator\":{\"duration\":\"1s\",\"rpu\":5}}},{\"name\":\"cat\",\"udf\":{\"builtin\":{\"name\":\"cat\"}}},{\"name\":\"out\",\"sink\":{\"log\":{}}}]}}\n"}},"spec":{"interStepBufferServiceName":"change","vertices":[{"name":"in","source":{"generator":{"duration":"1s","rpu":5}}},{"name":"cat","udf":{"builtin":{"name":"cat"}}},{"name":"out","sink":{"log":{}}}]}}
to:
Resource: "numaflow.numaproj.io/v1alpha1, Resource=pipelines", GroupVersionKind: "numaflow.numaproj.io/v1alpha1, Kind=Pipeline"
Name: "simple-pipeline", Namespace: "numaflow-system"
for: "examples/1-simple-pipeline.yaml": error when patching "examples/1-simple-pipeline.yaml": admission webhook "webhook.numaflow.numaproj.io" denied the request: Cannot update pipeline with different interStepBufferServiceName
```

Other validations include:

Pipeline:

1. cannot change the type of an existing vertex
2. cannot change the partition count of a reduce vertex
3. cannot change the storage class of a reduce vertex
4. etc.

InterStepBufferService:

1. cannot change the persistence configuration of an ISB Service
2. etc.
