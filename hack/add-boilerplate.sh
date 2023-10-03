#!/bin/bash

USAGE=$(cat <<EOF
Add boilerplate.<ext>.txt to all .<ext> files missing it in a directory.

Usage: (from repo root)
       ./hack/add-boilerplate.sh <ext> <DIR>

Example: (from repo root)
         ./hack/add-boilerplate.sh go cmd
EOF
)

set -e

if [[ -z $1 || -z $2 ]]; then
  echo "${USAGE}"
  exit 1
fi

grep -r -L -E "Copyright \d+ The Numaproj Authors" $2  \
  | grep -E "\.$1\$" \
  | xargs -I {} sh -c \
  "cat hack/boilerplate/boilerplate.$1.txt {} > /tmp/boilerplate && mv /tmp/boilerplate {}"
