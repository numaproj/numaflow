# How To Debug

To enable debug logs in a Vertex Pod, set environment variable `NUMAFLOW_DEBUG` to `true` for the Vertex. For example:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  vertices:
    - name: in
      source:
        generator:
          rpu: 100
          duration: 1s
    - name: p1
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable # A UDF which simply cats the message
          imagePullPolicy: Always
      containerTemplate:
        env:
          - name: NUMAFLOW_DEBUG
            value: !!str "true"
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: p1
    - from: p1
      to: out
```

To enable debug logs in the daemon pod, set environment variable `NUMAFLOW_DEBUG` to `true` for the daemon pod.
For example:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  templates:
    daemon:
      containerTemplate:
        env:
          - name: NUMAFLOW_DEBUG
            value: !!str "true"
```

## Profiling

If your pipeline is running with `NUMAFLOW_DEBUG` then `pprof` is enabled in the Vertex Pod. You
can also enable just `pprof` by setting `NUMAFLOW_PPROF` to `true`.

For example, run the commands like below to profile memory usage for a Vertex Pod, a web page displaying the memory information will be automatically opened.

```sh
# Port-forward
kubectl port-forward simple-pipeline-p1-0-7jzbn 2469

go tool pprof -http localhost:8081 https+insecure://localhost:2469/debug/pprof/heap
```

`Tracing` is also available with commands below.

```sh
# Add optional "&seconds=n" to specify the duration.
curl -skq https://localhost:2469/debug/pprof/trace?debug=1 -o trace.out

go tool trace -http localhost:8082 trace.out
```

## Debug Inside the Container

When doing local [development](development.md) using command lines such as `make start`, or `make image`, the built `numaflow` docker image is based on `alpine`, which allows you to execute into the container for debugging with `kubectl exec -it {pod-name} -c {container-name} -- sh`.

This is not allowed when running pipelines with official released images, as they are based on `scratch`.
