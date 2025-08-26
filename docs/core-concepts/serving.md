# ServingPipeline

The `ServingPipeline` is a specialized Numaflow resource designed to expose a standard Numaflow data processing 
[Pipeline](./pipeline.md) as an interactive HTTP service. The major design idea behind `ServingPipeline` is to bridge traditional 
request/response API patterns with the power of Numaflow's stream processing capabilities, allowing external clients to 
directly inject data, trigger processing, and retrieve results via familiar REST or Server-Sent Events (SSE) mechanisms.

A `ServingPipeline` consists of two main parts:

1.  An **HTTP Serving Layer:** Defined in `spec.serving`, this configures the external interface, including service 
exposure, request identification (via headers), timeouts, authentication, and result storage options.
2.  A **Standard Numaflow Pipeline:** Defined in `spec.pipeline`, this contains the familiar vertices and edges for data
processing. However, it *must* use a specific `serving` source as its entry point and conclude with a User Defined 
Sink (UDSink) capable of handling serving responses (using `ResponseServe`).

This structure enables features like synchronous (`/sync`), asynchronous (`/async`), and streaming (`/sse`) API endpoints,
along with request tracking via unique IDs. Unlike a standard `Pipeline` focused purely on stream processing, `ServingPipeline`
adds this interactive HTTP request/response lifecycle management layer on top.

The major benefits of `ServingPipeline` are as follows:

  * **API Exposure:** Easily expose complex stream processing logic or ML models within Numaflow pipelines via standard 
RESTful APIs.
  * **Interactive Workflows:** Supports common application patterns requiring synchronous request/response or traceable
asynchronous tasks initiated via HTTP.
  * **Flexible Interaction:** Offers synchronous, asynchronous, and streaming (Server-Sent Events) options for clients
to receive results based on their needs.
  * **Traceability:** Built-in request ID mechanism allows tracking individual requests and retrieving their specific
results or status.

## Use Cases of ServingPipeline

`ServingPipeline` is ideal for scenarios where external systems or users need to interact directly with a Numaflow pipeline via HTTP:

  * **ML Model Serving:** Deploying a machine learning model within a Numaflow pipeline (e.g., for pre/post-processing) 
and exposing it as a real-time inference API endpoint.
  * **Interactive Data Services:** Building services for data validation, enrichment, or transformation where clients 
submit data via an API call and receive the processed result.
  * **API Gateway Pattern:** Using `ServingPipeline` as a front-end to trigger event-driven backends processed by Numaflow,
potentially returning a final status or result synchronously or asynchronously.
  * **Traceable Asynchronous Jobs:** Kicking off complex, multi-step processing within Numaflow via an API call and 
allowing the client to poll for status or results later using a unique request ID.

## Anatomy of ServingPipeline

A `ServingPipeline` resource defines both the serving layer configuration (`spec.serving`) and the underlying processing
pipeline (`spec.pipeline`).

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: ServingPipeline
metadata:
  name: serving-pipeline-custom-store # Example name
spec:
  # Configures the HTTP serving aspects
  serving:
    ... serving configuration options ...

  # Defines the underlying Numaflow pipeline for processing
  pipeline:
    ... pipeline ...
```

### Configuration (`spec.serving`)

This section configures the HTTP interface:

  * `service` (boolean): If `true`, automatically creates a Kubernetes Service.
  * `msgIDHeaderKey` (string): The HTTP header key for the unique Request ID.
  * `requestTimeoutSeconds` (integer): Timeout for `/sync` and `/sse` requests (default: `120`).
  * `auth`: Optional configuration for token-based authentication using a Kubernetes `Secret`.
  * `store`: Optional configuration for a custom result storage backend. If omitted, internal storage (e.g., JetStream) is 
used. See [Custom Results Store](#custom-results-store-specservingstore) below.

### Custom Results Store (`spec.serving.store`)

You can provide a custom storage backend by specifying a container image in `spec.serving.store.container`. This 
container must implement a specific gRPC interface (defined in the Numaflow SDKs) for storing (`Put`) and retrieving
(`Get`) results associated with request IDs. This allows using preferred databases or caches.

Refer to the Numaflow SDK documentation for your language for the exact interface. The Go interface requires methods like:

```golang
type ServingStorer interface {
	Put(ctx context.Context, put PutDatum)
	Get(ctx context.Context, get GetDatum) StoredResult
}
// PutDatum, GetDatum, StoredResult provide necessary details
```

A Golang example can be found [here](https://github.com/numaproj/numaflow-go/tree/main/examples/servingstore).

### User-Defined Sink Implementation for Serving

The User Defined Sink (UDSink) in a `ServingPipeline`'s pipeline (`spec.pipeline.vertices[].sink.udsink`) must signal the
final response payload for the original HTTP request. Use the **`ResponseServe(requestID, resultBytes)`** function (or 
SDK equivalent) in your sink code.

#### Example (Go SDK)

```golang
type serveSink struct{}

func (l *serveSink) Sink(ctx context.Context, datumStreamCh <-chan sinksdk.Datum) sinksdk.Responses {
	result := sinksdk.ResponsesBuilder()
	for d := range datumStreamCh {
		id := d.ID() // Original Request ID
		val := d.Value() // Final payload from pipeline
		// Use ResponseServe to mark 'val' as the result for 'id'
		result = result.Append(sinksdk.ResponseServe(id, val))
	}
	return result
}
```
A complete example can be found [here](https://github.com/numaproj/numaflow-go/tree/main/examples/sinker/serve).

Using `ResponseServe` ensures the result is correctly stored and available via the API endpoints.

## API Endpoints

The `ServingPipeline` exposes the following endpoints:

  * `POST /v1/process/sync`: Submit data, wait for result in response body.
  * `POST /v1/process/async`: Submit data, return immediately; fetch result later.
  * `GET /v1/process/fetch?id=<request_id>`: Retrieve status/result(s) for a request ID.
  * `GET /v1/process/sse?id=<request_id>`: Stream results using Server-Sent Events.
  * `GET /v1/process/message?id=<request_id>`: Get message path info.

## Interaction Example

1.  **Submit data synchronously:**

    ```sh
    curl -k -XPOST \
      --header 'X-Numaflow-Id: job-456' \
      --header 'content-type: application/json' \
      --header 'Authorization: Bearer <your-token-if-auth-enabled>' \
      --data '{"value":123}' \
      --url https://<serving-pipeline-service-address>:8443/v1/process/sync
    ```

2.  **Fetch results later:**

    ```sh
    curl -k \
      --header 'Authorization: Bearer <your-token-if-auth-enabled>' \
      --url https://<serving-pipeline-service-address>:8443/v1/process/fetch?id=job-456
    ```

3.  **Stream results using SSE:**

    ```sh
    curl -k -N \
      --header 'X-Numaflow-Id: stream-789' \
      --header 'Authorization: Bearer <your-token-if-auth-enabled>' \
      --url https://<serving-pipeline-service-address>:8443/v1/process/sse?id=stream-789
    # Client receives events as results become available
    ```

To query `ServingPipeline` objects with `kubectl`:

```sh
kubectl get servingpipeline
```