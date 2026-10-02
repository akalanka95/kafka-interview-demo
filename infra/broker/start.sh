#!/bin/bash
# Broker entrypoint: format storage once (with the admin SCRAM credential), then start Kafka.
# The stock apache/kafka entrypoint formats storage itself but cannot add SCRAM users,
# and inter-broker SASL/SCRAM needs the admin credential to exist before the first start.
set -euo pipefail

: "${NODE_ID:?NODE_ID is required}"
: "${CLUSTER_ID:?CLUSTER_ID is required}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD is required}"

CONF="/mnt/broker/server-${NODE_ID}.properties"

# --ignore-formatted makes restarts a no-op; a fresh volume gets formatted.
/opt/kafka/bin/kafka-storage.sh format \
  --ignore-formatted \
  -t "${CLUSTER_ID}" \
  -c "${CONF}" \
  --add-scram "SCRAM-SHA-512=[name=admin,password=${ADMIN_PASSWORD}]"

exec /opt/kafka/bin/kafka-server-start.sh "${CONF}"
