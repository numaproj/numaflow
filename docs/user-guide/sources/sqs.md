# Setting up Numaflow MonoVertex with SQS Source

This guide explains how to set up a Numaflow MonoVertex that reads from an AWS SQS queue. We'll cover creating the necessary Kubernetes secrets for AWS credentials and configuring the MonoVertex specification.

## 1. Creating AWS Credentials Secret

First, we need to create a Kubernetes secret to store AWS credentials securely.

### Encode AWS Credentials

```bash
# Encode your AWS credentials (replace with your actual credentials)
ACCESS_KEY_ID=$(echo -n "your-aws-access-key-id" | base64)
SECRET_ACCESS_KEY=$(echo -n "your-aws-secret-access-key" | base64)

kubectl create secret generic aws-secret \
--from-literal access-key-id=${ACCESS_KEY_ID} \
--from-literal secret-access-key=${SECRET_ACCESS_KEY}
```

## 2. Create the numaflow pipeline 

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

## 3. Apply the Configuration

Apply the pipeline specification:

```bash
kubectl apply -f sqs-pl.yaml
```

## 4. Verify the Setup

Check that the MonoVertex is running:

```bash
kubectl get monovertex sqs-reader
kubectl get pods -l numaflow.numaproj.io/vertex-name=sqs-reader
```

## Important Security Considerations

1. Never commit AWS credentials to version control
2. Consider using IAM roles instead of access keys when possible
3. Ensure proper RBAC controls are in place
4. Regularly rotate AWS credentials
5. Use appropriate queue permissions in AWS
6. Consider using VPC endpoints for SQS access

## Troubleshooting

- Check the pods logs for any errors:
```bash
kubectl logs -l numaflow.numaproj.io/vertex-name=sqs-reader
```
- Verify the secret exists:
```bash
kubectl get secret aws-secret
```
- Ensure the SQS queue exists and is accessible with the provided credentials
