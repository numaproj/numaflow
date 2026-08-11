# HTTP Source

HTTP Source starts an HTTP service to accept POST requests in the Vertex Pod. By default, it listens on port 8443 with TLS enabled, with request URI `/vertices/{vertexName}`.

A plain HTTP (non-TLS) server can also be enabled by explicitly setting `ports.http`. By default, only HTTPS is exposed. When `ports.http` is set, both HTTPS and HTTP endpoints are exposed.

## Spec

Minimal spec for an http source which exposes a https endpoint for access on port 8443:

```yaml
source:
  http: {}
```

## Plain HTTP (non-TLS)

By default, the HTTP source only accepts HTTPS traffic and listens on port 8443. To also accept plain HTTP requests, explicitly set `ports.http`. The HTTPS server always starts on `ports.https` (default 8443), and the HTTP server only starts when `ports.http` is set. When enabled, both servers run simultaneously.

```yaml
source:
  http:
    service: true
    ports:
      http: 8090       # enables plain HTTP
      # https: 8443    # optional, defaults to 8443
```

When `service: true` is set alongside `ports.http`, the generated ClusterIP Service will expose both ports:

| Port name | Port  | Protocol |
|-----------|-------|----------|
| `https`   | 8443  | HTTPS (TLS) |
| `http`    | 8090  | HTTP (plain) |

Sending data over plain HTTP:

```sh
curl -X POST -d "hello world" http://${http-source-host}:8090/vertices/${vertexName}
```

> **Note:** Plain HTTP should only be used in trusted network environments (e.g., you have service mesh). Prefer HTTPS whenever possible, as it encrypts data in transit.

## x-numaflow-id

When posting data to the HTTP Source, an optional HTTP header `x-numaflow-id` can be specified, which will be used to dedup. If it's not provided, the HTTP Source will generate a random UUID to do it.

```sh
curl -kq -X POST -H "x-numaflow-id: ${id}" -d "hello world" ${http-source-url}
```

## x-numaflow-event-time

By default, the time of the data coming to the HTTP source is used as the event time. It can be set by putting an HTTP header `x-numaflow-event-time` with value of the number of milliseconds elapsed since January 1, 1970 UTC.

```sh
curl -kq -X POST -H "x-numaflow-event-time: 1663006726000" -d "hello world" ${http-source-url}
```

## x-numaflow-keys

The HTTP Source supports message keys for aggregation purposes. Keys can be specified using the `x-numaflow-keys` HTTP header with a comma-separated string of key values.

```sh
curl -kq -X POST -H "x-numaflow-keys: key1,key2,key3" -d "hello world" ${http-source-url}
```

For example, to send a message with keys "user123" and "region-us-west":

```sh
curl -kq -X POST -H "x-numaflow-keys: user123,region-us-west" -d '{"data": "sample"}' ${http-source-url}
```

If the `x-numaflow-keys` header is not provided, the message will be processed without any keys.

## Auth

A `Bearer` token can be configured to prevent the HTTP Source from being accessed by unexpected clients. To do so, a Kubernetes Secret needs to be created to store the token, and valid clients must include the token in the HTTP request header.

Firstly, create a k8s secret containing your token.

```sh
echo -n 'tr3qhs321fjglwf1e2e67dfda4tr' > ./token.txt

kubectl create secret generic http-source-token --from-file=my-token=./token.txt
```

Then add `auth` to the Source spec:

```yaml
source:
  http:
    auth:
      token:
        name: http-source-token
        key: my-token
```

When the clients post data to the Source, add `Authorization: Bearer tr3qhs321fjglwf1e2e67dfda4tr` to the header, for example:

```sh
TOKEN="Bearer tr3qhs321fjglwf1e2e67dfda4tr"
curl -kq -X POST -H "Authorization: $TOKEN" -d "hello world" ${http-source-url}
```

## Health Check

The HTTP Source also has an endpoint `/health` created automatically, which is useful for LoadBalancer or Ingress configuration, where a health check endpoint is often required by the cloud provider.

## Sending Data

Data can be sent to an HTTP source through:

- ClusterIP Service (within the cluster)
- Ingress or LoadBalancer Service (outside of the cluster)
- Port-forward (for testing)

### ClusterIP Service

An HTTP Source can generate a `ClusterIP` Service if `service: true` is specified:

```yaml
source:
  http:
    service: true
```

The Service name and the endpoint URI to send data to differ between Pipeline and MonoVertex — see [Pipeline](#pipeline) and [MonoVertex](#monovertex) below.

### LoadBalancer Service or Ingress

To create a `LoadBalancer` type Service, or a `NodePort` one for Ingress, you need to do it yourself. The `selector` to use in the Service differs between Pipeline and MonoVertex — see [Pipeline](#pipeline) and [MonoVertex](#monovertex) below.

### Port-forwarding

To test an HTTP source, you can do it from your local through port-forwarding.

```sh
kubectl port-forward pod ${pod-name} 8443
curl -kq -X POST -d "hello world" https://localhost:8443/vertices/${vertexName}
```

## Pipeline

A Pipeline with HTTP Source:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: http-pipeline
spec:
  vertices:
    - name: in
      source:
        http: {}
    - name: p1
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable # A UDF which simply cats the message
          imagePullPolicy: Always
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: p1
    - from: p1
      to: out
```

The Service name is in the format `{pipelineName}-{vertexName}`, so with `service: true` the HTTP Source can be accessed at `https://{pipelineName}-{vertexName}.{namespace}.svc:8443/vertices/{vertexName}` within the cluster by default:

```sh
curl -kq -X POST -d "hello world" https://http-pipeline-in:8443/vertices/in
```

For a `LoadBalancer` Service or Ingress, use a `selector` like the following:

```yaml
numaflow.numaproj.io/pipeline-name: http-pipeline # pipeline name
numaflow.numaproj.io/vertex-name: in # vertex name
```

## MonoVertex

The HTTP Source can also be used in a MonoVertex, which is useful when you just need to read from the HTTP Source and write to a Sink (optionally with a Transformer or Map UDF), without needing the full Pipeline semantics. The `source.http` spec is identical to the one used in a Pipeline; the difference is in the Service name, endpoint URI, and selector, since a MonoVertex has only one Vertex.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: simple-mono-vertex
spec:
  source:
    http: {}
  sink:
    log: {}
```

Unlike a Pipeline, where the Service name combines the pipeline and vertex names, a MonoVertex's HTTP Source Service is simply named after the MonoVertex. With `service: true`, it can be accessed at `https://{monoVertexName}.{namespace}.svc:8443/vertices/{monoVertexName}` within the cluster by default:

```sh
curl -kq -X POST -d "hello world" https://simple-mono-vertex:8443/vertices/simple-mono-vertex
```

For a `LoadBalancer` Service or Ingress, use a `selector` like the following:

```yaml
numaflow.numaproj.io/mono-vertex-name: simple-mono-vertex # mono vertex name
```
