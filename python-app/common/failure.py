"""Error classes and routing to orders.retry / orders.dlq.

Routing always waits for the broker ack (flush) before returning, so the caller can
commit the source offset safely. A publish that is not acked raises RoutingError and
the caller must NOT commit.
"""
import time

from . import config
from .headers import copy_original_headers, now_iso, to_kafka


class TransientError(Exception):
    """Worth retrying: downstream down, database locked."""


class PermanentError(Exception):
    """Retrying cannot help: bad JSON, schema violation, missing message-id, conflicting order."""


class RoutingError(Exception):
    """The retry/DLQ publish was not acknowledged."""


def _publish(producer, topic: str, msg, headers: dict) -> None:
    errors = []
    producer.produce(topic, key=msg.key(), value=msg.value(), headers=to_kafka(headers),
                     on_delivery=lambda err, _m: err and errors.append(err))
    remaining = producer.flush(10)
    if errors or remaining:
        raise RoutingError(f"publish to {topic} failed: {errors[0] if errors else 'flush timed out'}")


def route_to_retry(producer, msg, hop: int) -> int:
    """Re-publish with retry-count=hop and a not-before delay. Returns not-before (epoch ms)."""
    not_before = int(time.time() * 1000) + config.RETRY_DELAY_S[hop - 1] * 1000
    headers = copy_original_headers(msg)
    headers.update({"retry-count": hop, "not-before": not_before})
    _publish(producer, config.RETRY_TOPIC, msg, headers)
    return not_before


def route_to_dlq(producer, msg, err: BaseException, retry_count: int) -> None:
    headers = copy_original_headers(msg)
    headers.update({"error-class": type(err).__name__, "error-message": str(err)[:500],
                    "failed-at": now_iso(), "retry-count": retry_count})
    _publish(producer, config.DLQ_TOPIC, msg, headers)
