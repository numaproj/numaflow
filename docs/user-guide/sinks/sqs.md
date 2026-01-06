# SQS Sink

A `SQS` sink is used to forward messages to an AWS SQS queue.

## Configuration

```yaml
spec:
  vertices:
    - name: sqs-output
      sink:
        sqs:
          queueName: "your-queue-name"              # Required: Name of your SQS queue
          awsRegion: "us-west-2"                    # Required: AWS region
          queueOwnerAWSAccountID: "123456789012"    # Required: AWS account ID
          assumeRole:                               # Optional: For cross-account access
            roleArn: "arn:aws:iam::123456789012:role/CrossAccount-Role"
```

## Authentication

See [SQS Source - Configuring Credentials](../sources/sqs.md#configuring-credentials-to-access-aws) for authentication options including AWS credentials secrets and IAM roles.

## Message Headers

The SQS sink uses message headers to control SQS-specific behavior. Headers can originate from:
1. The source (e.g., SQS system attributes from an SQS source)
2. User metadata under the `sqs` namespace (merged into headers at the sink)

### Supported Headers

| Header | Description |
|--------|-------------|
| `MessageGroupId` | Required for FIFO queues. Messages with the same group ID are processed in order. |
| `MessageDeduplicationId` | Used for FIFO queues to prevent duplicate message delivery. |
| `DelaySeconds` | Delays message delivery by the specified number of seconds (0-900). |

### FIFO Queue Support

For FIFO queues, the sink automatically extracts `MessageGroupId` and `MessageDeduplicationId` from message headers:

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: sqs-to-sqs-fifo
spec:
  vertices:
    - name: in
      source:
        sqs:
          queueName: "source-queue.fifo"
          awsRegion: "us-west-2"
          queueOwnerAWSAccountID: "123456789012"
          attributeNames:
            - MessageGroupId
            - MessageDeduplicationId
    - name: out
      sink:
        sqs:
          queueName: "destination-queue.fifo"
          awsRegion: "us-west-2"
          queueOwnerAWSAccountID: "123456789012"
  edges:
    - from: in
      to: out
```

In this example, `MessageGroupId` and `MessageDeduplicationId` from the source queue are automatically propagated to the destination FIFO queue.

### Setting Headers via Metadata

User-defined functions can set SQS headers by writing to the `sqs` metadata namespace. These values are merged into the message headers at the sink, allowing UDFs to control FIFO ordering or set delay seconds.

## Example Pipeline

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: http-to-sqs
spec:
  vertices:
    - name: in
      source:
        http: {}
    - name: process
      udf:
        container:
          image: my-processor:latest
    - name: out
      sink:
        sqs:
          queueName: "output-queue"
          awsRegion: "us-west-2"
          queueOwnerAWSAccountID: "123456789012"
  edges:
    - from: in
      to: process
    - from: process
      to: out
```

