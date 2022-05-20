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
    - name: input
      source:
        http: {}
    - name: p1
      udf:
        builtin:
          name: cat
    - name: output
      sink:
        log: {}
  edges:
    - from: input
      to: p1
    - from: p1
      to: output
```

## ClusterIP Service

An HTTP Source Vertex can generate a `ClusterIP` Service if `service: true` is specified, the service name is in the format of `{pipelineName}-{vertexName}`, so the HTTP Source can be accessed through `https://{pipelineName}-{vertexName}.{namespace}.svc.cluster.local:8443/vertices/{vertexName}`.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: http-pipeline
spec:
  vertices:
    - name: input
      source:
        http:
          service: true
```

## Your Own Service

Sometimes you don't need a `ClusterIP` Service but a `LoadBalander` or `NodePort` one, you need to create it by you own. Just remember to use `selector` like following in the Service:

```yaml
numaflow.numaproj.io/pipeline-name: http-pipeline # pipeline name
numaflow.numaproj.io/vertex-name: input # vertex name
```

## x-numaflow-id

When posting data to the HTTP Source, an optional HTTP header `x-numaflow-id` can be specified, it will be used to dedup. If it's not provided, the HTTP Source will generate a random UUID to do it.

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
    - name: input
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
curl -kq -X POST -H "Authorization: $TOKEN" -d "hello world" https://http-pipeline-input:8443/vertices/input
```

## Health Check

The HTTP Source also has an endpoint `/health` created automatically, which is useful for for LoadBalancer or Ingress configuration, where a health check endpoint is often required by the cloud provider.
