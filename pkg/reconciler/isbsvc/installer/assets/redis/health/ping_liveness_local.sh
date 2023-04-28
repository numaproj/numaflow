#!/bin/bash

[[ -f $REDIS_PASSWORD_FILE ]] && export REDIS_PASSWORD="$(< "${REDIS_PASSWORD_FILE}")"
[[ -n "$REDIS_PASSWORD" ]] && export REDISCLI_AUTH="$REDIS_PASSWORD"
response=$(
  timeout -s 15 $1 \
  redis-cli \
    -h localhost \
    -p $REDIS_PORT \
    ping
)
if [ "$response" != "PONG" ] && [ "$response" != "LOADING Redis is loading the dataset in memory" ]; then
  echo "$response"
  exit 1
fi