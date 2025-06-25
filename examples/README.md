# Numaflow by Example

Welcome to the Numaflow Community! This document provides an example-by-example guide to using Numaflow.

If you haven't already, install Numaflow by following the [QUICK START](../docs/quick-start.md) instructions.

The top-level abstraction in Numaflow is the `Pipeline`. A `Pipeline` consists of a set of `vertices` connected by `edges`. A vertex can be a `source`, `sink`, or `processing` vertex. In the example below, we have a source vertex named _in_ that generates messages at a specified rate, a sink vertex named _out_ that logs messages, and a processing vertex named _cat_ that produces any input message as output. Lastly, there are two edges, one connecting the _in_ to the _cat_ vertex and another connecting the _cat_ to the _out_ vertex. The resulting pipeline simply copies internally generated messages to the log.

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: simple-pipeline
spec:
  vertices:
    - name: in
      source:
        # Generate 5 messages every second
        # xxx what does rpu stand for? how large are the messages by default, what data is contained in the message? rename duration to interval?
        generator:
          rpu: 5
          duration: 1s
    - name: cat
      udf: # A user-defined function
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable # A UDF which simply cats the message
          imagePullPolicy: Always
    - name: out
      sink:
        # Output message to the stdout log
        log: {}

  # in -> cat -> out
  edges:
    - from: in
      to: cat
    - from: cat
      to: out
```

Below we have a simple variation on the above example that takes input from an http endpoint.

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
          # Whether to create a ClusterIP Service, defaults to false
          service: true
          # Optional bearer token auth
    #         auth:
    #           # A secret selector pointing to the secret contains token
    #           token:
    #             name: my-secret
    #             key: my-key
    - name: cat
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-cat:stable # A UDF which simply cats the message
          imagePullPolicy: Always
    - name: out
      sink:
        # A simple log printing sink
        log: {}
  edges:
    - from: in
      to: cat
    - from: cat
      to: out
```

Let's modify the UDF in the first example to pass-through only messages with an _id_ less than 100

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: filter-pipeline
spec:
  vertices:
    - name: in
      source:
        generator:
          rpu: 5
          duration: 1s
    - name: filter
      udf:
        container:
          image: quay.io/numaio/numaflow-go/map-filter-id:stable # A filter which pass-through only messages with an id less than 100
          imagePullPolicy: Always
    - name: out
      sink:
        log: {}
  edges:
    - from: in
      to: filter
    - from: filter
      to: out
```
