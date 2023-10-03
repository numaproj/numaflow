#!/bin/bash

. /opt/bitnami/scripts/libos.sh
. /opt/bitnami/scripts/libvalidations.sh
. /opt/bitnami/scripts/libfile.sh

HEADLESS_SERVICE="{{.HeadlessServiceName}}.{{.Namespace}}.svc"
REDIS_SERVICE="{{.ServiceName}}.{{.Namespace}}.svc"

get_port() {
    hostname="$1"
    type="$2"

    port_var=$(echo "${hostname^^}_SERVICE_PORT_$type" | sed "s/-/_/g")
    port=${!port_var}

    if [ -z "$port" ]; then
        case $type in
            "SENTINEL")
                echo {{.SentinelPort}}
                ;;
            "REDIS")
                echo {{.RedisPort}}
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

SERVPORT=$(get_port "$HOSTNAME" "SENTINEL")
REDISPORT=$(get_port "$HOSTNAME" "REDIS")
SENTINEL_SERVICE_PORT=$(get_port "{{.ServiceName}}" "TCP_SENTINEL")

sentinel_conf_set() {
    local -r key="${1:?missing key}"
    local value="${2:-}"

    # Sanitize inputs
    value="${value//\\/\\\\}"
    value="${value//&/\\&}"
    value="${value//\?/\\?}"
    [[ "$value" = "" ]] && value="\"$value\""

    replace_in_file "/opt/bitnami/redis-sentinel/etc/sentinel.conf" "^#*\s*${key} .*" "${key} ${value}" false
}
sentinel_conf_add() {
    echo $'\n'"$@" >> "/opt/bitnami/redis-sentinel/etc/sentinel.conf"
}
host_id() {
    echo "$1" | openssl sha1 | awk '{print $2}'
}
get_sentinel_master_info() {
    if is_boolean_yes "$REDIS_SENTINEL_TLS_ENABLED"; then
        sentinel_info_command="REDISCLI_AUTH="\$REDIS_PASSWORD" timeout 220 redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT --tls --cert ${REDIS_SENTINEL_TLS_CERT_FILE} --key ${REDIS_SENTINEL_TLS_KEY_FILE} --cacert ${REDIS_SENTINEL_TLS_CA_FILE} sentinel get-master-addr-by-name mymaster"
    else
        sentinel_info_command="REDISCLI_AUTH="\$REDIS_PASSWORD" timeout 220 redis-cli -h $REDIS_SERVICE -p $SENTINEL_SERVICE_PORT sentinel get-master-addr-by-name mymaster"
    fi
    info "about to run the command: $sentinel_info_command"
    eval $sentinel_info_command
}

[[ -f $REDIS_PASSWORD_FILE ]] && export REDIS_PASSWORD="$(< "${REDIS_PASSWORD_FILE}")"

master_in_persisted_conf="$(get_full_hostname "$HOSTNAME")"
if ! get_sentinel_master_info && [[ "$master_in_persisted_conf" == "$(get_full_hostname "$HOSTNAME")" ]]; then
    # No master found, lets create a master node
    export REDIS_REPLICATION_MODE="master"

    REDIS_MASTER_HOST=$(get_full_hostname "$HOSTNAME")
    REDIS_MASTER_PORT_NUMBER="$REDISPORT"
else
    export REDIS_REPLICATION_MODE="replica"

    # Fetches current master's host and port
    REDIS_SENTINEL_INFO=($(get_sentinel_master_info))
    info "printing REDIS_SENTINEL_INFO=(${REDIS_SENTINEL_INFO[0]},${REDIS_SENTINEL_INFO[1]})"
    REDIS_MASTER_HOST=${REDIS_SENTINEL_INFO[0]}
    REDIS_MASTER_PORT_NUMBER=${REDIS_SENTINEL_INFO[1]}
fi

if [[ -n "$REDIS_EXTERNAL_MASTER_HOST" ]]; then
    REDIS_MASTER_HOST="$REDIS_EXTERNAL_MASTER_HOST"
    REDIS_MASTER_PORT_NUMBER="${REDIS_EXTERNAL_MASTER_PORT}"
fi

cp /opt/bitnami/redis-sentinel/mounted-etc/sentinel.conf /opt/bitnami/redis-sentinel/etc/sentinel.conf
printf "\nsentinel auth-pass %s %s" "mymaster" "$REDIS_PASSWORD" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
printf "\nrequirepass %s" "$REDIS_PASSWORD" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
printf "\nsentinel myid %s" "$(host_id "$HOSTNAME")" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf

if [[ -z "$REDIS_MASTER_HOST" ]] || [[ -z "$REDIS_MASTER_PORT_NUMBER" ]]
then
    # Prevent incorrect configuration to be written to sentinel.conf
    error "Redis master host is configured incorrectly (host: $REDIS_MASTER_HOST, port: $REDIS_MASTER_PORT_NUMBER)"
    exit 1
fi

sentinel_conf_set "sentinel monitor" "mymaster "$REDIS_MASTER_HOST" "$REDIS_MASTER_PORT_NUMBER" 2"

add_known_sentinel() {
    hostname="$1"
    ip="$2"

    if [[ -n "$hostname" && -n "$ip" && "$hostname" != "$HOSTNAME" ]]; then
        sentinel_conf_add "sentinel known-sentinel mymaster $(get_full_hostname "$hostname") $(get_port "$hostname" "SENTINEL") $(host_id "$hostname")"
    fi
}
add_known_replica() {
    hostname="$1"
    ip="$2"

    if [[ -n "$ip" && "$(get_full_hostname "$hostname")" != "$REDIS_MASTER_HOST" ]]; then
        sentinel_conf_add "sentinel known-replica mymaster $(get_full_hostname "$hostname") $(get_port "$hostname" "REDIS")"
    fi
}

# Add available hosts on the network as known replicas & sentinels
for node in $(seq 0 $(({{.Replicas}}-1))); do
    hostname="{{.StatefulSetName}}-$node"
    ip="$(getent hosts "$hostname.$HEADLESS_SERVICE" | awk '{ print $1 }')"
    add_known_sentinel "$hostname" "$ip"
    add_known_replica "$hostname" "$ip"
done

echo "" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
echo "sentinel announce-hostnames yes" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
echo "sentinel resolve-hostnames yes" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
echo "sentinel announce-port $SERVPORT" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
echo "sentinel announce-ip $(get_full_hostname "$HOSTNAME")" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
exec redis-server /opt/bitnami/redis-sentinel/etc/sentinel.conf --sentinel