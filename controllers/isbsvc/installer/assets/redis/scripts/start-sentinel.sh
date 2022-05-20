#!/bin/bash

. /opt/bitnami/scripts/libos.sh
. /opt/bitnami/scripts/libvalidations.sh
. /opt/bitnami/scripts/libfile.sh

HEADLESS_SERVICE="{{.HeadlessServiceName}}.{{.Namespace}}.svc.cluster.local"
REDIS_SERVICE="{{.ServiceName}}.{{.Namespace}}.svc.cluster.local"

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
    echo "${hostname}.${HEADLESS_SERVICE}"
}

SERVPORT=$(get_port "$HOSTNAME" "SENTINEL")
REDISPORT=$(get_port "$HOSTNAME" "REDIS")
SENTINEL_SERVICE_PORT=$(get_port "{{.ServiceName}}" "TCP_SENTINEL")

myip=$(hostname -i)

# If there are more than one IP, use the first IPv4 address
if [[ "$myip" = *" "* ]]; then
    myip=$(echo $myip | awk '{if ( match($0,/([0-9]+\.)([0-9]+\.)([0-9]+\.)[0-9]+/) ) { print substr($0,RSTART,RLENGTH); } }')
fi


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
not_exists_dns_entry() {
    if [[ -z "$(getent ahosts "$HEADLESS_SERVICE" | grep "^${myip}" )" ]]; then
        warn "$HEADLESS_SERVICE does not contain the IP of this pod: ${myip}"
        return 1
    fi
    debug "$HEADLESS_SERVICE has my IP: ${myip}"
    return 0
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

# Waits for DNS to add this ip to the service DNS entry
retry_while not_exists_dns_entry

[[ -f $REDIS_PASSWORD_FILE ]] && export REDIS_PASSWORD="$(< "${REDIS_PASSWORD_FILE}")"

cp /opt/bitnami/redis-sentinel/mounted-etc/sentinel.conf /opt/bitnami/redis-sentinel/etc/sentinel.conf
printf "\nsentinel auth-pass %s %s" "mymaster" "$REDIS_PASSWORD" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
printf "\nrequirepass %s" "$REDIS_PASSWORD" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf
printf "\nsentinel myid %s" "$(host_id "$HOSTNAME")" >> /opt/bitnami/redis-sentinel/etc/sentinel.conf

if [[ -z "$(getent ahosts "$HEADLESS_SERVICE" | grep -v "^${myip}")" ]]; then
    # Only node available on the network, master by default
    export REDIS_REPLICATION_MODE="master"

    REDIS_MASTER_HOST=$(get_full_hostname "$HOSTNAME")
    REDIS_MASTER_PORT_NUMBER="$REDISPORT"
else
    export REDIS_REPLICATION_MODE="slave"

    # Fetches current master's host and port
    REDIS_SENTINEL_INFO=($(get_sentinel_master_info))
    info "printing REDIS_SENTINEL_INFO=(${REDIS_SENTINEL_INFO[0]},${REDIS_SENTINEL_INFO[1]})" 
    REDIS_MASTER_HOST=${REDIS_SENTINEL_INFO[0]}
    REDIS_MASTER_PORT_NUMBER=${REDIS_SENTINEL_INFO[1]}
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
