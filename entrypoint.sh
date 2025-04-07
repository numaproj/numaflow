#!/bin/sh
set -e

if [ "$NUMAFLOW_RUNTIME" = "rust" ]; then
  exec /bin/numaflow-rs "$@"
else
  exec /bin/numaflow "$@"
fi