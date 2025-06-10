#!/bin/bash

# Number of pipelines to create
NUM_PIPELINES=${1:-20}

# Source file
SOURCE_FILE="examples/1-simple-pipeline.yaml"

echo "Creating $NUM_PIPELINES pipelines..."

for i in $(seq 1 $NUM_PIPELINES); do
  # Create a temporary file with modified pipeline name
  TMP_FILE=$(mktemp)
  
  # Replace the pipeline name with indexed version
  sed "s/name: simple-pipeline/name: simple-pipeline-$i/" "$SOURCE_FILE" > "$TMP_FILE"
  
  # Apply the modified pipeline
  kubectl apply -f "$TMP_FILE"
  
  # Clean up temp file
  rm "$TMP_FILE"
  
  echo "Created pipeline simple-pipeline-$i"
done

echo "All $NUM_PIPELINES pipelines created successfully!"