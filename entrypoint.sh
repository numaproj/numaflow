#!/bin/sh
set -e

## This is an intermediary entrypoint script that allows us to switch between starting the Rust binary
## and the Go binary based on the NUMAFLOW_RUNTIME environment variable.
## Eventually the entire data-plane will be in Rust and this script will be removed.

if [ "$NUMAFLOW_RUNTIME" = "rust" ]; then
  exec /bin/numaflow-rs "$@"
else
  exec /bin/numaflow "$@"
fi