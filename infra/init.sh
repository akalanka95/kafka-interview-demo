#!/bin/bash
# One-shot cluster bootstrap: SCRAM users, topics, ACLs. Idempotent — safe to re-run:
#   docker compose run --rm kafka-init
# See docs/infra.md §6–7.
set -euo pipefail

BS=kafka-1:9092,kafka-2:9092,kafka-3:9092
CFG=/mnt/client-configs/admin.properties
BIN=/opt/kafka/bin

log() { echo "[init] $*"; }

# ---------------------------------------------------------------------------
# 1) SCRAM users (upsert). admin already exists from `kafka-storage format`.
# ---------------------------------------------------------------------------
for u in web-app py-producer py-consumer py-ops intruder; do
  log "user $u"
  $BIN/kafka-configs.sh --bootstrap-server "$BS" --command-config "$CFG" --alter \
    --add-config "SCRAM-SHA-512=[password=${u}-secret]" \
    --entity-type users --entity-name "$u"
done

# ---------------------------------------------------------------------------
# 2) Topics — RF 3, min ISR 2
# ---------------------------------------------------------------------------
mk() {
  local topic=$1 partitions=$2; shift 2
  log "topic $topic (p=$partitions)"
  $BIN/kafka-topics.sh --bootstrap-server "$BS" --command-config "$CFG" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 3 \
    --config min.insync.replicas=2 "$@" 2>&1 | grep -v "collide\|^$" || true
}
mk web.messages 3
mk orders       3
mk orders.retry 3
mk orders.dlq   1 --config retention.ms=1209600000   # 14 days

# ---------------------------------------------------------------------------
# 3) ACLs — least privilege (adding an existing ACL is a no-op)
# ---------------------------------------------------------------------------
acl() { $BIN/kafka-acls.sh --bootstrap-server "$BS" --command-config "$CFG" --add "$@" > /dev/null; }

log "acl web-app"
acl --allow-principal User:web-app --operation Read --operation Write --operation Describe --topic web.messages
acl --allow-principal User:web-app --operation Read --group web-consumer- --resource-pattern-type prefixed
acl --allow-principal User:web-app --operation Write --operation Describe --transactional-id web-tx- --resource-pattern-type prefixed
acl --allow-principal User:web-app --operation IdempotentWrite --operation Describe --cluster

log "acl py-producer"
acl --allow-principal User:py-producer --operation Write --operation Describe --topic orders
acl --allow-principal User:py-producer --operation IdempotentWrite --cluster

log "acl py-consumer"
acl --allow-principal User:py-consumer --operation Read --operation Describe --topic orders
acl --allow-principal User:py-consumer --operation Read --operation Write --operation Describe --topic orders.retry
acl --allow-principal User:py-consumer --operation Write --operation Describe --topic orders.dlq
acl --allow-principal User:py-consumer --operation Read --group orders-processor --group orders-retry-processor
acl --allow-principal User:py-consumer --operation IdempotentWrite --cluster

log "acl py-ops"
acl --allow-principal User:py-ops --operation Read --operation Describe --topic orders.dlq
acl --allow-principal User:py-ops --operation Write --operation Describe --topic orders
acl --allow-principal User:py-ops --operation Read --group dlq-inspector
acl --allow-principal User:py-ops --operation IdempotentWrite --cluster

# intruder: intentionally no ACLs.

log "topics:"
$BIN/kafka-topics.sh --bootstrap-server "$BS" --command-config "$CFG" --describe \
  --topic 'web.messages|orders|orders.retry|orders.dlq' 2>/dev/null | grep -v '^\s*Topic:.*Partition:' || true
log "ACL count: $($BIN/kafka-acls.sh --bootstrap-server "$BS" --command-config "$CFG" --list | grep -c 'principal=User:')"
log "init complete"
