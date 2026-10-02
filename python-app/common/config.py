"""Client config per Kafka user, topic names, retry policy, file locations."""
import os
from pathlib import Path

APP_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = APP_DIR / "data"
DB_PATH = Path(os.getenv("ORDERS_DB", str(DATA_DIR / "orders.db")))
LAST_SENT = DATA_DIR / "last_sent.jsonl"
FAIL_FLAG = APP_DIR / ".fail_downstream"   # present => downstream "down"

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092,localhost:29092,localhost:39092")

TOPIC, RETRY_TOPIC, DLQ_TOPIC = "orders", "orders.retry", "orders.dlq"
GROUP, RETRY_GROUP, DLQ_GROUP = "orders-processor", "orders-retry-processor", "dlq-inspector"

MAX_INPLACE_ATTEMPTS = 3
INPLACE_BACKOFF = [0.5, 1.0]               # seconds slept between attempts (2 sleeps for 3 attempts)
RETRY_DELAY_S = [5, 10, 20]                # not-before = now + RETRY_DELAY_S[hop - 1]
MAX_RETRY_HOPS = len(RETRY_DELAY_S)


def client_conf(user: str) -> dict:
    """librdkafka names: sasl.mechanisms is plural (Java uses sasl.mechanism)."""
    return {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": user,
        "sasl.password": os.getenv("KAFKA_PASSWORD", f"{user}-secret"),  # demo-only passwords
    }


def producer_conf(user: str) -> dict:
    return {**client_conf(user), "enable.idempotence": True, "acks": "all",
            "linger.ms": 5, "compression.type": "lz4"}


def consumer_conf(user: str, group: str) -> dict:
    return {**client_conf(user), "group.id": group, "enable.auto.commit": False,
            "auto.offset.reset": "earliest", "isolation.level": "read_committed",
            "max.poll.interval.ms": 300000}
