"""Business logic shared by consumer.py and retry_consumer.py.

process()       : dedup -> validate -> downstream + DB txn with in-place retries.
handle_record() : process() + routing of failures to orders.retry / orders.dlq.
After handle_record() returns, the caller commits the offset. If it raises RoutingError,
the caller must not commit (the record is redelivered and dedup absorbs any repeat).
"""
import json
import time

from . import config, db
from .failure import PermanentError, TransientError, route_to_dlq, route_to_retry
from .headers import get_header
from .log import key_of, short_id

REQUIRED = {"orderId": str, "customerId": str, "currency": str, "createdAt": str}


def validate(raw) -> dict:
    if raw is None:
        raise PermanentError("empty value")
    try:
        order = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as e:
        raise PermanentError(f"invalid JSON: {e}") from e
    if not isinstance(order, dict):
        raise PermanentError("value is not a JSON object")
    for field, typ in REQUIRED.items():
        if not isinstance(order.get(field), typ) or not order[field]:
            raise PermanentError(f"missing or invalid '{field}'")
    amount = order.get("amount")
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise PermanentError("missing or invalid 'amount'")
    if amount <= 0:
        raise PermanentError(f"amount must be > 0, got {amount}")
    return order


def call_downstream(order: dict) -> None:
    """Simulated external call. In production it must accept message-id as an idempotency
    key: it runs outside the SQLite transaction, so a crash after it would repeat it."""
    if config.FAIL_FLAG.exists():
        raise TransientError("downstream unavailable")


def process(msg, conn, log, sleep=time.sleep) -> str:
    """Returns "PROCESSED" or "DUPLICATE". Raises PermanentError, or TransientError once
    the in-place attempts are used up."""
    key = key_of(msg)
    message_id = get_header(msg, "message-id")
    if not message_id:
        raise PermanentError("missing message-id header")
    if db.is_processed(conn, message_id):
        return "DUPLICATE"
    order = validate(msg.value())

    for attempt in range(1, config.MAX_INPLACE_ATTEMPTS + 1):
        try:
            call_downstream(order)
            db.apply_order_txn(conn, order, message_id, msg.topic(), msg.partition(), msg.offset())
            return "PROCESSED"
        except db.DuplicateMessage:
            return "DUPLICATE"
        except TransientError as e:
            if attempt == config.MAX_INPLACE_ATTEMPTS:
                raise
            delay = config.INPLACE_BACKOFF[attempt - 1]
            log(msg, key, f"RETRY {attempt}/{config.MAX_INPLACE_ATTEMPTS} in {delay}s", f"TransientError: {e}")
            sleep(delay)
    raise AssertionError("unreachable")


def handle_record(msg, conn, producer, log, hops_so_far: int = 0, sleep=time.sleep) -> str:
    """Process one record and route failures. Returns the outcome for the caller's log/tests."""
    key = key_of(msg)
    mid = short_id(get_header(msg, "message-id"))
    try:
        outcome = process(msg, conn, log, sleep)
    except TransientError as e:
        hop = hops_so_far + 1
        if hop <= config.MAX_RETRY_HOPS:
            not_before = route_to_retry(producer, msg, hop)
            log(msg, key, f"→ {config.RETRY_TOPIC} hop {hop}",
                f"not-before={time.strftime('%H:%M:%S', time.localtime(not_before / 1000))}  TransientError: {e}")
            return "RETRY"
        route_to_dlq(producer, msg, TransientError(f"retries exhausted: {e}"), hops_so_far)
        log(msg, key, f"→ {config.DLQ_TOPIC}", f"TransientError: retries exhausted after {hops_so_far} hops")
        return "DLQ"
    except PermanentError as e:
        route_to_dlq(producer, msg, e, hops_so_far)
        log(msg, key, f"→ {config.DLQ_TOPIC}", f"PermanentError: {e}")
        return "DLQ"
    except Exception as e:  # a bug must not stop the pipeline
        route_to_dlq(producer, msg, e, hops_so_far)
        log(msg, key, f"→ {config.DLQ_TOPIC}", f"{type(e).__name__}: {e}")
        return "DLQ"

    hop_note = f"(hop {hops_so_far})" if hops_so_far else ""
    if outcome == "DUPLICATE":
        log(msg, key, "DUPLICATE SKIPPED", f"{mid} {hop_note}".strip())
    else:
        log(msg, key, "PROCESSED", f"{mid} {hop_note}".strip())
    return outcome
