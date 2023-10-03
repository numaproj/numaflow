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

Currently, the validating webhook prevents updating the type of an InterStepBufferService from JetStream to Redis for example.

Example spec:
```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: InterStepBufferService
metadata:
  name: default
spec:
  jetstream: // change to redis and reapply will cause below error
    version: latest
```

```shell
Error from server (BadRequest): error when applying patch:
{"metadata":{"annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{\"apiVersion\":\"numaflow.numaproj.io/v1alpha1\",\"kind\":\"InterStepBufferService\",\"metadata\":{\"annotations\":{},\"name\":\"default\",\"namespace\":\"numaflow-system\"},\"spec\":{\"redis\":{\"native\":{\"version\":\"7.0.11\"}}}}\n"}},"spec":{"jetstream":null,"redis":{"native":{"version":"7.0.11"}}}}
to:
Resource: "numaflow.numaproj.io/v1alpha1, Resource=interstepbufferservices", GroupVersionKind: "numaflow.numaproj.io/v1alpha1, Kind=InterStepBufferService"
Name: "default", Namespace: "numaflow-system"
for: "redis.yaml": error when patching "redis.yaml": admission webhook "webhook.numaflow.numaproj.io" denied the request: Can not change ISB Service type from Jetstream to Redis
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