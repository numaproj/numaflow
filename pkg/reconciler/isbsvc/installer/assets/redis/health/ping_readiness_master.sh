#!/bin/bash
[[ -f $REDIS_MASTER_PASSWORD_FILE ]] && export REDIS_MASTER_PASSWORD="$(<
"${REDIS_MASTER_PASSWORD_FILE}")"
[[ -n "$REDIS_MASTER_PASSWORD" ]] && export
REDISCLI_AUTH="$REDIS_MASTER_PASSWORD"
response=$(
  timeout -s 15 $1 \
  redis-cli \
    -h $REDIS_MASTER_HOST \
    -p $REDIS_MASTER_PORT_NUMBER \
    ping
)
if [ "$?" -eq "124" ]; then
  echo "Timed out"
  exit 1
fi

if [ "$response" != "PONG" ]; then
  echo "$response"
  exit 1
fi