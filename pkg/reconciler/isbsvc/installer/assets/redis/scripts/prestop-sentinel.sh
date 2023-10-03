#!/bin/bash

. /opt/bitnami/scripts/libvalidations.sh
. /opt/bitnami/scripts/libos.sh

HEADLESS_SERVICE="{{.HeadlessServiceName}}.{{.Namespace}}.svc"
SENTINEL_SERVICE_ENV_NAME=REDIS_SENTINEL_SERVICE_PORT_TCP_SENTINEL
SENTINEL_SERVICE_PORT=${!SENTINEL_SERVICE_ENV_NAME}
REDIS_SERVICE="{{.ServiceName}}.{{.Namespace}}.svc"

get_full_hostname() {
    hostname="$1"
    full_hostname="${hostname}.${HEADLESS_SERVICE}"
    echo "${full_hostname}"
}

run_sentinel_command() {
    if is_boolean_yes "$REDIS_SENTINEL_TLS_ENABLED"; then
        redis-cli -h "$REDIS_SERVICE" -p "$SENTINEL_SERVICE_PORT" --tls --cert "$REDIS_SENTINEL_TLS_CERT_FILE" --key "$REDIS_SENTINEL_TLS_KEY_FILE" --cacert "$REDIS_SENTINEL_TLS_CA_FILE" sentinel "$@"
    else
        redis-cli -h "$REDIS_SERVICE" -p "$SENTINEL_SERVICE_PORT" sentinel "$@"
    fi
}
sentinel_failover_finished() {
    REDIS_SENTINEL_INFO=($(run_sentinel_command get-master-addr-by-name "mymaster"))
    REDIS_MASTER_HOST="${REDIS_SENTINEL_INFO[0]}"
    [[ "$REDIS_MASTER_HOST" != "$(get_full_hostname $HOSTNAME)" ]]
}


# redis-cli automatically consumes credentials from the REDISCLI_AUTH variable
[[ -n "$REDIS_PASSWORD" ]] && export REDISCLI_AUTH="$REDIS_PASSWORD"
[[ -f "$REDIS_PASSWORD_FILE" ]] && export REDISCLI_AUTH="$(< "${REDIS_PASSWORD_FILE}")"

if ! sentinel_failover_finished; then
    echo "I am the master pod and you are stopping me. Starting sentinel failover"
    if retry_while "sentinel_failover_finished" "20" 1; then
        echo "Master has been successfuly failed over to a different pod."
        exit 0
    else
        echo "Master failover failed"
        exit 1
    fi
else
    exit 0
fi