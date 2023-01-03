# Edges And Buffers

`Edge` and `Buffer` are two logic concepts used in `Numaflow`, this page is used to describe the definition of them.

`Edge` means the connection between the vertices, specifically, `edge` is defined in the pipeline spec under `.spec.egdes`. For each `edge` definition, no matter the to vertex is a map, or a reduce with `parallelism > 1`, it is considered as one edge.

In the following pipeline example, there are 3 edges defined (`in` - `aoti`, `aoti` - `compute-sum`, `compute-sum` - `sink`).

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: even-odd-sum
spec:
  vertices:
    - name: in
      source:
        http: {}
    - name: atoi
      scale:
        min: 1
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-even-odd
    - name: compute-sum
      udf:
        container:
          image: quay.io/numaio/numaflow-go/reduce-sum
        groupBy:
          window:
            fixed:
              length: 60s
          keyed: true
    - name: sink
      scale:
        min: 1
      sink:
        log: {}
  edges:
    - from: in
      to: atoi
    - from: atoi
      to: compute-sum
      parallelism: 2
    - from: compute-sum
      to: sink
```

`Buffer` is a concept different from `edge` but has lots of connections. Usually, each `edge` has one or more corresponding buffers, depending on the to vertex type (map or reduce). If the to vertex is a reduce and the edge parallelism > 1, there will be multiple buffers defined for that edge.

`Buffers` are not only defined for `edges`, but also for `Source` and `Sink` vertices - each `Source` and `Sink` vertex has a coresponding `buffer`.

In summary, there are 3 types of `buffers` in a pipeline:

- Edge Buffer
- Source Buffer
- Sink Buffer

Each buffer has a name, the naming convertion for different type of buffers can be found in the [source code](https://github.com/numaproj/numaflow/blob/main/pkg/apis/numaflow/v1alpha1/vertex_types.go).

`Buffer` is only used internally, it's transparent to the users. Each Inter-Step Buffer implementation should have something physical to map to the `buffers`. For example, In JetStream Inter-Step ISB implementation, a K/V bucket will be created for a `Source Buffer` or a `Sink Buffer`, and a K/V bucket plus a Stream will be created for a `Edge Buffer`. These buffer management operations are done by K8s jobs spawned by the controllers during pipeline creation and deletion.
