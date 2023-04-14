# Controllers

Currently in `Numaflow`, there are 3 CRDs introduced, each one has a corresponding controller.

- interstepbufferservices.numaflow.numaproj.io
- pipelines.numaflow.numaproj.io
- vertices.numaflow.numaproj.io

The source code of the controllers is located at `./pkg/reconciler/`.

![Architecture](../assets/architecture.png)

## Inter-Step Buffer Service Controller

`Inter-Step Buffer Service Controller` is used to watch `InterStepBufferService` object, depending on the spec of the object, it might install services (such as JetStream, or Redis) in the namespace, or simply provide the configuration of the `InterStepBufferService` (for example, when an `external` redis ISB Service is given).

## Pipeline Controller

Pipeline Controller is used to watch `Pipeline` objects, it does following major things when there's a pipeline object created.

- Spawn a Kubernetes Job to create [buffers](./edges-and-buffers.md) in the [Inter-Step Buffer Services](../core-concepts/inter-step-buffer-service.md).
- Create `Vertex` objects according to `.spec.vertices` defined in `Pipeline` object.
- Create some other Kubernetes objects used for the Pipeline, such as a Deployment and a Service for daemon service application.

## Vertex Controller

Vertex controller watches the `Vertex` objects, based on the replica defined in the spec, creates a number of pods to run the workloads.
