#!/bin/bash

. /opt/bitnami/scripts/libvalidations.sh
. /opt/bitnami/scripts/libos.sh

run_redis_command() {
    if is_boolean_yes "$REDIS_TLS_ENABLED"; then
        redis-cli -h 127.0.0.1 -p "$REDIS_TLS_PORT" --tls --cert "$REDIS_TLS_CERT_FILE" --key "$REDIS_TLS_KEY_FILE" --cacert "$REDIS_TLS_CA_FILE" "$@"
    else
        redis-cli -h 127.0.0.1 -p ${REDIS_PORT} "$@"
    fi
}
failover_finished() {
    REDIS_ROLE=$(run_redis_command role | head -1)
    [[ "$REDIS_ROLE" != "master" ]]
}

# redis-cli automatically consumes credentials from the REDISCLI_AUTH variable
[[ -n "$REDIS_PASSWORD" ]] && export REDISCLI_AUTH="$REDIS_PASSWORD"
[[ -f "$REDIS_PASSWORD_FILE" ]] && export REDISCLI_AUTH="$(< "${REDIS_PASSWORD_FILE}")"

if ! failover_finished; then
    echo "Waiting for sentinel to run failover for up to 20s"
    retry_while "failover_finished" "20" 1
else
    exit 0
fi
