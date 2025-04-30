# Setting up Numaflow MonoVertex with SQS Source

This guide explains how to set up a Numaflow MonoVertex that reads from an AWS SQS queue. 

## Configuring Credentials to access AWS 

There are couple of ways you could achieve this.

### Creating AWS Credentials Secret

First, we need to create a Kubernetes secret to store AWS credentials securely and encode AWS Credentials.

```bash
# Encode your AWS credentials (replace with your actual credentials)
ACCESS_KEY_ID=$(echo -n "your-aws-access-key-id" | base64)
SECRET_ACCESS_KEY=$(echo -n "your-aws-secret-access-key" | base64)

kubectl create secret generic aws-secret \
--from-literal access-key-id=${ACCESS_KEY_ID} \
--from-literal secret-access-key=${SECRET_ACCESS_KEY}
```

### IAM ROLE
1. Use an IAM role with the necessary permissions to access the SQS queue.
2. Ensure the IAM role of the pod has access to the SQS queue, and the SQS queue policy allows the IAM role to perform actions on the queue.
3. Attach the appropriate service account with the IAM role to the pod.

For more details, refer to the AWS documentation: https://docs.aws.amazon.com/eks/latest/userguide/pod-id-how-it-works.html

## Create the numaflow pipeline 

Create a file named `sqs-pl.yaml` with either of the following content.

### MonoVertex Specification

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: MonoVertex
metadata:
  name: sqs-reader
spec:
    source:
        sqs:
          queueName: "your-queue-name"        # Required: Name of your SQS queue
          awsRegion: "your-aws-region"        # Required: AWS region where queue is located
          queueOwnerAWSAccountID: "123456789012" # Required: AWS account ID of the queue owner
          # Optional configurations
          maxNumberOfMessages: 10             # Max messages per poll (1-10)
          visibilityTimeout: 30              # Visibility timeout in seconds
          waitTimeSeconds: 20                # Long polling wait time
          attributeNames:                    # SQS attributes to retrieve
            - All
          messageAttributeNames:             # Message attributes to retrieve
            - All
    sink:
        log: {}                             # Simple log sink for testing
    limits:
        readBatchSize: 10
        bufferSize: 100
    scale:
        min: 1
        max: 5
```

### Pipeline Specification

```yaml
apiVersion: numaflow.numaproj.io/v1alpha1
kind: Pipeline
metadata:
  name: sqs-source-pl
spec:
  vertices:
    - name: in
      scale:
        min: 1
      source:
        sqs:
          queueName: "queue-name"
          awsRegion: "us-west-2"
          queueOwnerAWSAccountID: "123456789012" # Required: AWS account ID of the queue owner
          # Optional configurations
          maxNumberOfMessages: 10            # Max messages per poll (1-10)
          visibilityTimeout: 30              # Visibility timeout in seconds
          waitTimeSeconds: 20                # Long polling wait time
          attributeNames:                    # SQS attributes to retrieve
            - All
          messageAttributeNames:             # Message attributes to retrieve
            - All
    - name: out
      scale:
        min: 1
      sink:
        log: {}
  edges:
    - from: in
      to: out
```

## Apply the Configuration

Apply the pipeline specification:

```bash
kubectl apply -f sqs-pl.yaml
```

## Verify the Setup

Check that the MonoVertex is running:

```bash
kubectl get monovertex sqs-reader
kubectl get pods -l numaflow.numaproj.io/vertex-name=sqs-reader
```


## Miscellaneous

### Troubleshooting

- Check the pods logs for any errors:
```bash
kubectl logs -l numaflow.numaproj.io/vertex-name=sqs-reader
```
- Verify the secret exists:
```bash
kubectl get secret aws-secret
```
- Ensure the SQS queue exists and is accessible with the provided credentials
