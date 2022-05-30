# NumaFlow by Example

Welcome to the NumaFlow Community! This document provides an example-by example guide to using NumaFlow.

If you haven't already, install NumaFlow by following the [QUICK START](../docs/QUICK_START.md) instructions.

The top-level abstraction in NumaFlow is the `Pipeline`. A `Pipeline` consists of a set of `vertices` connected by `edges`. A vertex can be a `source`, `sink`, or `processing` vertex. In the example below, we have a source vertex named *input* that generates messages at a specified rate, a sink vertex named *ouput* that logs messages, and a processing vertex named *cat* that produces any input message as output. Lastly, there are two edges, one connecting the *input* to the *cat* vertex and another connecting the *cat* to the *output* vertex. The resulting pipeline simply copies internally generated messages to the log.
```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  vertices:
    - name: input
      source:
        # Generate 5 messages every second
        # xxx what does rpu stand for? how large are the messages by default, what data is contained in the message? rename duration to interval?
        generator:
          rpu: 5
          duration: 1s
    - name: cat
      udf: # A user defined function
        builtin: # Use a builtin function as the udf
          name: cat # cats the message
    - name: output
      sink:
        # Output message to the log
	# xxx which log? k8s events?
        log: {}

  # input -> cat -> output
  edges:
    - from: input
      to: cat
    - from: cat
      to: output
```

Below we have a simple variation on the above example that takes input from an http endpoint.
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
          # Whether to create a ClusterIP Service, defaults to false
	  # xxx Needd example of how to send data to the endpoint
          service: true
          # Optional bearer token auth
	  # xxx This is a k8s secret? How is this token used?
#         auth:
#           # A secret selector pointing to the secret contains token
#           token:
#             name: my-secret
#             key: my-key
    - name: cat
      udf:
        builtin:
          name: cat # A builtin UDF which simply cats the message
    - name: output
      sink:
        # A simple log printing sink
        log: {}
  edges:
    - from: input
      to: cat
    - from: cat
      to: output
```

Let's modify the UDF in the first example to passthrough only messages with an *id* less than 100
```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: filter-pipeline
spec:
  vertices:
    - name: input
      source:
        generator:
          rpu: 5
          duration: 1s
    - name: filter
      udf:
        builtin:
          name: filter
          kwargs:
            expression: int(json(payload).id) < 100
    - name: output
      sink:
        log: {}
  edges:
    - from: input
      to: filter
    - from: filter
      to: output
```

