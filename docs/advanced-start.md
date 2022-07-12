# An Advanced Example

After [Quick Start](./quick-start.md), It's time to look at an advanced pipeline example.

In this example, there are 5 vertices in a pipeline. An [HTTP](./sources/http.md) Source vertex which serves an HTTP service to receive numbers as source data, a [UDF](./user-defined-functions.md) vertex to tag the ingested numbers with the key `even` or `odd`, and there are 3 [Log](./sinks/log.md) Sinks, one to print the `even` numbers, one to print the `odd` numbers, and the other one to print both the even and odd numbers.

![Pipeline Diagram](../assets/even-odd.png)

Create an [Inter-Step Buffer Service](./inter-step-buffer-service.md) if it's not there.

```shell
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/0-isbsvc-jetstream.yaml
```

Apply the `even-odd` pipeline spec.

```shell
kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/stable/test/e2e/testdata/even-odd.yaml
```

Run a port-forward to the `HTTP` source pod to make it possible to post data from the laptop.

```shell
# Replace with the coresponding "in" vertex pod name
kubectl port-forward even-odd-in-0-xxxx 8443
```

Run following testing commands to post some numbers, and verfiy if they are displayed in coresponding pod logs.

```shell
curl -kq -X POST -d "101" https://localhost:8443/vertices/in # 101 should be display in the log of both "odd" and "number" pods.
curl -kq -X POST -d "102" https://localhost:8443/vertices/in # 102 should be display in the log of both "even" and "number" pods.
```

The source code of the `even-odd` [User Defined Function](./user-defined-functions.md) can be found [here](https://github.com/numaproj/numaflow-go/tree/main/examples/function/evenodd). You also can replace the [Log](./sinks/log.md) Sink with some other sinks like [Kafka](./sinks/kafka.md) to forward the data to Kafka topics.

In the end, run the commands below to clean up the testing pipeline and ISB service.

```shell
kubectl delete -f https://raw.githubusercontent.com/numaproj/numaflow/stable/test/e2e/testdata/even-odd.yaml
kubectl delete -f https://raw.githubusercontent.com/numaproj/numaflow/stable/examples/0-isbsvc-jetstream.yaml
```

## What's Next

After exploring how Numaflow pipeline run, you can check what data [Sources](./sources/generator.md) and [Sinks](./sinks/kafka.md) Numaflow supports out of the box, or learn how to write [User Defined Functions](./user-defined-functions.md).
