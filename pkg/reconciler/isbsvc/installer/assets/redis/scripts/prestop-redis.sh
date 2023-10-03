#!/bin/bash

. /opt/bitnami/scripts/libvalidations.sh
. /opt/bitnami/scripts/libos.sh

run_redis_command() {
    if is_boolean_yes "$REDIS_TLS_ENABLED"; then
        redis-cli -h 127.0.0.1 -p "$REDIS_TLS_PORT" --tls --cert "$REDIS_TLS_CERT_FILE" --key "$REDIS_TLS_KEY_FILE" --cacert "$REDIS_TLS_CA_FILE" "$@"
    else
        redis-cli -h 127.0.0.1 -p "$REDIS_PORT" "$@"
    fi
}
is_master() {
    REDIS_ROLE=$(run_redis_command role | head -1)
    [[ "$REDIS_ROLE" == "master" ]]
}

HEADLESS_SERVICE="{{.HeadlessServiceName}}.{{.Namespace}}.svc"
SENTINEL_SERVICE_ENV_NAME=REDIS_SENTINEL_SERVICE_PORT_TCP_SENTINEL
SENTINEL_SERVICE_PORT=$(get_port "{{.ServiceName}}" "TCP_SENTINEL")
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


if is_master && ! sentinel_failover_finished; then
    echo "I am the master pod and you are stopping me. Pausing client connections."
    # Pausing client write connections to avoid data loss
    run_redis_command CLIENT PAUSE "22000" WRITE

    echo "Issuing failover"
    # if I am the master, issue a command to failover once
    run_sentinel_command failover "mymaster"
    echo "Waiting for sentinel to complete failover for up to 20s"
    retry_while "sentinel_failover_finished" "20" 1
else
    exit 0
fi