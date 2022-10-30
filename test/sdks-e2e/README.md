# SDKs Test

Each SDK is supposed to implement:

1. A `flatmap` UDF example, which splits the input message with `,`, and return a list. Build a docker image `quay.io/numaio/flatmap-example:${language}` and push to `quay.io`;
   
2. A `simplesink` UDSink example, which prints out original message in pod logs. Build a docker image `quay.io/numaio/simplesink-example:${language}` and push to `quay.io`.
