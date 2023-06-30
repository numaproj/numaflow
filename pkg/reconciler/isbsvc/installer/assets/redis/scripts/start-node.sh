#!/bin/bash

. /opt/bitnami/scripts/libos.sh
. /opt/bitnami/scripts/liblog.sh
. /opt/bitnami/scripts/libvalidations.sh

get_port() {
    hostname="$1"
    type="$2"

    port_var=$(echo "${hostname^^}_SERVICE_PORT_$type" | sed "s/-/_/g")
    port=${!port_var}

    if [ -z "$port" ]; then
        case $type in
            "SENTINEL")
                echo 26379
                ;;
            "REDIS")
                echo 6379
                ;;
        esac
    else
        echo $port
    fi
}

get_full_hostname() {
    hostname="$1"
    full_hostname="${hostname}.${HEADLESS_SERVICE}"
    echo "${full_hostname}"
}

REDISPORT=$(get_port "$HOSTNAME" "REDIS")

HEADLESS_SERVICE="{{.HeadlessServiceName}}.{{.Namespace}}.svc"
REDIS_SERVICE="{{.ServiceName}}.{{.Namespace}}.svc"
SENTINEL_SERVICE_PORT=$(get_port "{{.ServiceName}}" "TCP_SENTINEL")


validate_quorum() {
    if is_boolean_yes "$REDIS_TLS_ENABLED"; then
        quorum_info_command="REDISCLI_AUTH="\$REDIS_PASSWORD" redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT --tls --cert ${REDIS_TLS_CERT_FILE} --key ${REDIS_TLS_KEY_FILE} --cacert ${REDIS_TLS_CA_FILE} sentinel master mymaster"
    else
        quorum_info_command="REDISCLI_AUTH="\$REDIS_PASSWORD" redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT sentinel master mymaster"
    fi
    info "about to run the command: $quorum_info_command"
    eval $quorum_info_command | grep -Fq "s_down"
}

trigger_manual_failover() {
    if is_boolean_yes "$REDIS_TLS_ENABLED"; then
        failover_command="REDISCLI_AUTH="\$REDIS_PASSWORD" redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT --tls --cert ${REDIS_TLS_CERT_FILE} --key ${REDIS_TLS_KEY_FILE} --cacert ${REDIS_TLS_CA_FILE} sentinel failover mymaster"
    else
        failover_command="REDISCLI_AUTH="\$REDIS_PASSWORD" redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT sentinel failover mymaster"
    fi

    info "about to run the command: $failover_command"
    eval $failover_command
}

get_sentinel_master_info() {
    if is_boolean_yes "$REDIS_TLS_ENABLED"; then
        sentinel_info_command="REDISCLI_AUTH="\$REDIS_PASSWORD" timeout 220 redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT --tls --cert ${REDIS_TLS_CERT_FILE} --key ${REDIS_TLS_KEY_FILE} --cacert ${REDIS_TLS_CA_FILE} sentinel get-master-addr-by-name mymaster"
    else
        sentinel_info_command="REDISCLI_AUTH="\$REDIS_PASSWORD" timeout 220 redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT sentinel get-master-addr-by-name mymaster"
    fi

    info "about to run the command: $sentinel_info_command"
    eval $sentinel_info_command
}

[[ -f $REDIS_PASSWORD_FILE ]] && export REDIS_PASSWORD="$(< "${REDIS_PASSWORD_FILE}")"
[[ -f $REDIS_MASTER_PASSWORD_FILE ]] && export REDIS_MASTER_PASSWORD="$(< "${REDIS_MASTER_PASSWORD_FILE}")"

# check if there is a master
master_in_persisted_conf="$(get_full_hostname "$HOSTNAME")"
master_port_in_persisted_conf="$REDIS_MASTER_PORT_NUMBER"
master_in_sentinel="$(get_sentinel_master_info)"
redisRetVal=$?

if [[ $redisRetVal -ne 0 ]]; then
    if [[ "$master_in_persisted_conf" == "$(get_full_hostname "$HOSTNAME")" ]]; then
        # Case 1: No active sentinel and in previous sentinel.conf we were the master --> MASTER
        info "Configuring the node as master"
        export REDIS_REPLICATION_MODE="master"
    else
        # Case 2: No active sentinel and in previous sentinel.conf we were not master --> REPLICA
        info "Configuring the node as replica"
        export REDIS_REPLICATION_MODE="replica"
        REDIS_MASTER_HOST=${master_in_persisted_conf}
        REDIS_MASTER_PORT_NUMBER=${master_port_in_persisted_conf}
    fi
else
    # Fetches current master's host and port
    REDIS_SENTINEL_INFO=($(get_sentinel_master_info))
    info "Current master: REDIS_SENTINEL_INFO=(${REDIS_SENTINEL_INFO[0]},${REDIS_SENTINEL_INFO[1]})"
    REDIS_MASTER_HOST=${REDIS_SENTINEL_INFO[0]}
    REDIS_MASTER_PORT_NUMBER=${REDIS_SENTINEL_INFO[1]}

    if [[ "$REDIS_MASTER_HOST" == "$(get_full_hostname "$HOSTNAME")" ]]; then
        # Case 3: Active sentinel and master it is this node --> MASTER
        info "Configuring the node as master"
        export REDIS_REPLICATION_MODE="master"
    else
        # Case 4: Active sentinel and master is not this node --> REPLICA
        info "Configuring the node as replica"
        export REDIS_REPLICATION_MODE="replica"
    fi
fi

if [[ -n "$REDIS_EXTERNAL_MASTER_HOST" ]]; then
    REDIS_MASTER_HOST="$REDIS_EXTERNAL_MASTER_HOST"
    REDIS_MASTER_PORT_NUMBER="${REDIS_EXTERNAL_MASTER_PORT}"
fi

if [[ -f /opt/bitnami/redis/mounted-etc/replica.conf ]];then
    cp /opt/bitnami/redis/mounted-etc/replica.conf /opt/bitnami/redis/etc/replica.conf
fi

if [[ -f /opt/bitnami/redis/mounted-etc/redis.conf ]];then
    cp /opt/bitnami/redis/mounted-etc/redis.conf /opt/bitnami/redis/etc/redis.conf
fi

echo "" >> /opt/bitnami/redis/etc/replica.conf
echo "replica-announce-port $REDISPORT" >> /opt/bitnami/redis/etc/replica.conf
echo "replica-announce-ip $(get_full_hostname "$HOSTNAME")" >> /opt/bitnami/redis/etc/replica.conf
ARGS=("--port" "${REDIS_PORT}")

if [[ "$REDIS_REPLICATION_MODE" = "slave" ]] || [[ "$REDIS_REPLICATION_MODE" = "replica" ]]; then
    ARGS+=("--replicaof" "${REDIS_MASTER_HOST}" "${REDIS_MASTER_PORT_NUMBER}")
fi
ARGS+=("--requirepass" "${REDIS_PASSWORD}")
ARGS+=("--masterauth" "${REDIS_MASTER_PASSWORD}")
ARGS+=("--include" "/opt/bitnami/redis/etc/replica.conf")
ARGS+=("--include" "/opt/bitnami/redis/etc/redis.conf")
exec redis-server "${ARGS[@]}"