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
    echo "${hostname}.${HEADLESS_SERVICE}"
}

REDISPORT=$(get_port "$HOSTNAME" "REDIS")

myip=$(hostname -i)

# If there are more than one IP, use the first IPv4 address
if [[ "$myip" = *" "* ]]; then
    myip=$(echo $myip | awk '{if ( match($0,/([0-9]+\.)([0-9]+\.)([0-9]+\.)[0-9]+/) ) { print substr($0,RSTART,RLENGTH); } }')
fi

HEADLESS_SERVICE="{{.HeadlessServiceName}}.{{.Namespace}}.svc.cluster.local"
REDIS_SERVICE="{{.ServiceName}}.{{.Namespace}}.svc.cluster.local"
SENTINEL_SERVICE_PORT=$(get_port "{{.ServiceName}}" "TCP_SENTINEL")

not_exists_dns_entry() {
    if [[ -z "$(getent ahosts "$HEADLESS_SERVICE" | grep "^${myip}" )" ]]; then
        warn "$HEADLESS_SERVICE does not contain the IP of this pod: ${myip}"
        return 1
    fi
    debug "$HEADLESS_SERVICE has my IP: ${myip}"
    return 0
}

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
        sentinel_info_command="REDISCLI_AUTH="\$REDIS_PASSWORD" redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT --tls --cert ${REDIS_TLS_CERT_FILE} --key ${REDIS_TLS_KEY_FILE} --cacert ${REDIS_TLS_CA_FILE} sentinel get-master-addr-by-name mymaster"
    else
        sentinel_info_command="REDISCLI_AUTH="\$REDIS_PASSWORD" redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT sentinel get-master-addr-by-name mymaster"
    fi

    info "about to run the command: $sentinel_info_command"
    eval $sentinel_info_command
}

[[ -f $REDIS_PASSWORD_FILE ]] && export REDIS_PASSWORD="$(< "${REDIS_PASSWORD_FILE}")"
[[ -f $REDIS_MASTER_PASSWORD_FILE ]] && export REDIS_MASTER_PASSWORD="$(< "${REDIS_MASTER_PASSWORD_FILE}")"

# Waits for DNS to add this ip to the service DNS entry
retry_while not_exists_dns_entry

if [[ -z "$(getent ahosts "$HEADLESS_SERVICE" | grep -v "^${myip}")" ]]; then
    # Only node available on the network, master by default
    export REDIS_REPLICATION_MODE="master"
else
    export REDIS_REPLICATION_MODE="slave"
    
    # Fetches current master's host and port
    REDIS_SENTINEL_INFO=($(get_sentinel_master_info))
    info "printing REDIS_SENTINEL_INFO=(${REDIS_SENTINEL_INFO[0]},${REDIS_SENTINEL_INFO[1]})"
    REDIS_MASTER_HOST=${REDIS_SENTINEL_INFO[0]}
    REDIS_MASTER_PORT_NUMBER=${REDIS_SENTINEL_INFO[1]}
fi

if [[ "$REDIS_REPLICATION_MODE" = "master" ]]; then
    debug "Starting as master node"
    if [[ ! -f /opt/bitnami/redis/etc/master.conf ]]; then
        cp /opt/bitnami/redis/mounted-etc/master.conf /opt/bitnami/redis/etc/master.conf
    fi
else
    debug "Starting as replica node"
    if [[ ! -f /opt/bitnami/redis/etc/replica.conf ]];then
        cp /opt/bitnami/redis/mounted-etc/replica.conf /opt/bitnami/redis/etc/replica.conf
    fi
fi

if [[ ! -f /opt/bitnami/redis/etc/redis.conf ]];then
    cp /opt/bitnami/redis/mounted-etc/redis.conf /opt/bitnami/redis/etc/redis.conf
fi

echo "" >> /opt/bitnami/redis/etc/replica.conf
echo "replica-announce-port $REDISPORT" >> /opt/bitnami/redis/etc/replica.conf
echo "replica-announce-ip $(get_full_hostname "$HOSTNAME")" >> /opt/bitnami/redis/etc/replica.conf
ARGS=("--port" "${REDIS_PORT}")

if [[ "$REDIS_REPLICATION_MODE" = "slave" ]]; then
    ARGS+=("--slaveof" "${REDIS_MASTER_HOST}" "${REDIS_MASTER_PORT_NUMBER}")
fi
ARGS+=("--requirepass" "${REDIS_PASSWORD}")
ARGS+=("--masterauth" "${REDIS_MASTER_PASSWORD}")
if [[ "$REDIS_REPLICATION_MODE" = "master" ]]; then
    ARGS+=("--include" "/opt/bitnami/redis/etc/master.conf")
else
    ARGS+=("--include" "/opt/bitnami/redis/etc/replica.conf")
fi
ARGS+=("--include" "/opt/bitnami/redis/etc/redis.conf")
exec redis-server "${ARGS[@]}"
