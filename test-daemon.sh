#!/bin/bash

# Minimal test script for daemon gRPC (h2) and HTTP/1.1
# Usage: ./test-daemon.sh
# Prerequisites: kubectl port-forward -n default pod/<daemon-pod> 4327:4327

ADDR="localhost:4327"
PROTO_PATH="pkg/apis/proto"
PROTO_FILE="mvtxdaemon/mvtxdaemon.proto"

echo "Testing HTTP/1.1 REST:"
curl -k -s https://$ADDR/api/v1/metrics | head -c 200
echo -e "\n"

echo "Testing gRPC (h2):"
grpcurl -insecure -import-path $PROTO_PATH -proto $PROTO_FILE -d '{}' $ADDR mvtxdaemon.MonoVertexDaemonService/GetMonoVertexMetrics 2>&1 | head -c 300
