# HTTP Source

HTTP Source starts an HTTP service with TLS enabled to accept POST request in the Vertex Pod. It listens to port 8443, with request URI `/vertices/{vertexName}`.

An Pipeline with HTTP Source:

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
        builtin:
          name: cat
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

An HTTP Source Vertex can generate a `ClusterIP` Service if `service: true` is specified, the service name is in the format of `{pipelineName}-{vertexName}`, so the HTTP Source can be accessed through `https://{pipelineName}-{vertexName}.{namespace}.svc.cluster.local:8443/vertices/{vertexName}` within the cluster.

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

To create a `LoadBalander` type Service, or a `NodePort` one for Ingress, you need to do it by you own. Just remember to use `selector` like following in the Service:

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

## x-numaflow-id

When posting data to the HTTP Source, an optional HTTP header `x-numaflow-id` can be specified, which will be used to dedup. If it's not provided, the HTTP Source will generate a random UUID to do it.

```sh
curl -kq -X POST -H "x-numaflow-id: ${id}" -d "hello world" ${http-source-url}
```

## x-numaflow-event-time

By default, the time of the date coming to the HTTP source is used as the event time, it could be set by putting an HTTP header `x-numaflow-event-time` with value of the number of milliseconds elapsed since January 1, 1970 UTC.

```sh
curl -kq -X POST -H "x-numaflow-event-time: 1663006726000" -d "hello world" ${http-source-url}
```

## Auth

A `Bearer` token can be configured to prevent the HTTP Source from being accessed by unexpected clients. To do so, a Kubernetes Secret needs to be created to store the token, and the valid clients also need to include the token in its HTTP request header.

Firstly, create a k8s secret containing your token.

```sh
echo -n 'tr3qhs321fjglwf1e2e67dfda4tr' > ./token.txt

kubectl create secret generic http-source-token --from-file=my-token=./token.txt
```

Then add `auth`to the Source Vertex:

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

The HTTP Source also has an endpoint `/health` created automatically, which is useful for for LoadBalancer or Ingress configuration, where a health check endpoint is often required by the cloud provider.
