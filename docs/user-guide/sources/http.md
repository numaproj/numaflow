# HTTP Source

HTTP Source starts an HTTP service to accept POST requests in the Vertex Pod. By default, it listens on port 8443 with TLS enabled, with request URI `/vertices/{vertexName}`.

A plain HTTP (non-TLS) server can also be enabled by explicitly setting `http` under `ports`.

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

## Sending Data

Data could be sent to an HTTP source through:

- ClusterIP Service (within the cluster)
- Ingress or LoadBalancer Service (outside of the cluster)
- Port-forward (for testing)

### ClusterIP Service

An HTTP Source Vertex can generate a `ClusterIP` Service if `service: true` is specified. The service name is in the format `{pipelineName}-{vertexName}`, so the HTTP Source can be accessed at `https://{pipelineName}-{vertexName}.{namespace}.svc:8443/vertices/{vertexName}` within the cluster.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: http-pipeline
spec:
  vertices:
    - name: in
      source:
        http:
          service: true
```

### LoadBalancer Service or Ingress

To create a `LoadBalancer` type Service, or a `NodePort` one for Ingress, you need to do it yourself. Use `selector` like the following in the Service:

```yaml
numaflow.numaproj.io/pipeline-name: http-pipeline # pipeline name
numaflow.numaproj.io/vertex-name: in # vertex name
```

### Port-forwarding

To test an HTTP source, you can do it from your local through port-forwarding.

```sh
kubectl port-forward pod ${pod-name} 8443
curl -kq -X POST -d "hello world" https://localhost:8443/vertices/in
```

## Plain HTTP (non-TLS)

By default, the HTTP source only accepts HTTPS traffic. To also accept plain HTTP requests, explicitly set `ports.http`. The HTTPS server always starts on `ports.https` (default 8443); the HTTP server only starts when `ports.http` is set.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: http-pipeline
spec:
  vertices:
    - name: in
      source:
        http:
          service: true
          ports:
            http: 8090       # enables plain HTTP
            # https: 8443    # optional, defaults to 8443
```

When `service: true` is set alongside `httpPort`, the generated ClusterIP Service will expose both ports:

| Port name | Port  | Protocol |
|-----------|-------|----------|
| `https`   | 8443  | HTTPS (TLS) |
| `http`    | 8090  | HTTP (plain) |

Sending data over plain HTTP:

```sh
curl -X POST -d "hello world" http://http-pipeline-in:8090/vertices/in
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

Then add `auth` to the Source Vertex:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: http-pipeline
spec:
  vertices:
    - name: in
      source:
        http:
          auth:
            token:
              name: http-source-token
              key: my-token
```

When the clients post data to the Source Vertex, add `Authorization: Bearer tr3qhs321fjglwf1e2e67dfda4tr` to the header, for example:

```sh
TOKEN="Bearer tr3qhs321fjglwf1e2e67dfda4tr"
# Post data from a Pod in the same namespace of the cluster
curl -kq -X POST -H "Authorization: $TOKEN" -d "hello world" https://http-pipeline-in:8443/vertices/in
```

## Health Check

The HTTP Source also has an endpoint `/health` created automatically, which is useful for LoadBalancer or Ingress configuration, where a health check endpoint is often required by the cloud provider.
