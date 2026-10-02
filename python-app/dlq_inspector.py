"""DLQ inspector (user py-ops). Read-only on orders.dlq: assign + seek, never commits.

  python dlq_inspector.py --list [--limit 20]
  python dlq_inspector.py --show <offset>
  python dlq_inspector.py --replay <offset>

Replay re-produces key, value and the ORIGINAL message-id to orders, so replaying the same
record twice is processed once (the consumer's dedup table skips the second one).
"""
import argparse
import json

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from common import config, log
from common.headers import as_dict, now_iso, to_kafka

APP = "dlq"
PARTITION = 0   # orders.dlq has a single partition


def make_consumer() -> Consumer:
    # log_level 2: hide the harmless coordinator-disconnect line on a quick close (never joins the group).
    return Consumer({**config.consumer_conf("py-ops", config.DLQ_GROUP), "enable.partition.eof": True, "log_level": 2})


def watermarks(consumer):
    return consumer.get_watermark_offsets(TopicPartition(config.DLQ_TOPIC, PARTITION), timeout=10)


def read_range(consumer, start: int, end: int) -> list:
    """Records with start <= offset < end."""
    if start >= end:
        return []
    consumer.assign([TopicPartition(config.DLQ_TOPIC, PARTITION, start)])
    out = []
    while True:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            raise SystemExit(f"read failed: {msg.error()}")
        out.append(msg)
        if msg.offset() >= end - 1:
            break
    return out


def read_one(consumer, offset: int):
    low, high = watermarks(consumer)
    if not low <= offset < high:
        raise SystemExit(f"offset {offset} not in {config.DLQ_TOPIC} (available {low}..{high - 1})")
    msgs = read_range(consumer, offset, offset + 1)
    if not msgs:
        raise SystemExit(f"offset {offset} could not be read")
    return msgs[0]


def text(b) -> str:
    return b.decode("utf-8", "replace") if isinstance(b, bytes) else str(b)


def cmd_list(consumer, limit: int) -> None:
    low, high = watermarks(consumer)
    msgs = read_range(consumer, max(low, high - limit), high)
    print(f"{config.DLQ_TOPIC}: offsets {low}..{high - 1} ({high - low} records), showing {len(msgs)}")
    print(f"{'offset':>6}  {'key':<10} {'error-class':<16} {'origin':<14} {'hops':>4}  {'failed-at':<24} error-message")
    for m in msgs:
        h = as_dict(m)
        origin = f"{h.get('original-topic', '?')}/{h.get('original-partition', '?')}@{h.get('original-offset', '?')}"
        print(f"{m.offset():>6}  {text(m.key()):<10} {h.get('error-class', ''):<16} {origin:<14} "
              f"{h.get('retry-count', ''):>4}  {h.get('failed-at', ''):<24} {h.get('error-message', '')[:70]}")


def cmd_show(consumer, offset: int) -> None:
    m = read_one(consumer, offset)
    print(f"offset   {m.offset()}\nkey      {text(m.key())}\nheaders")
    for k, v in as_dict(m).items():
        print(f"  {k:<20} {v}")
    value = text(m.value()) if m.value() is not None else "<null>"
    try:
        value = json.dumps(json.loads(value), indent=2)
    except ValueError:
        pass
    print(f"value\n{value}")


def cmd_replay(consumer, offset: int) -> int:
    m = read_one(consumer, offset)
    h = as_dict(m)
    headers = {"message-id": h.get("message-id"), "produced-at": h.get("produced-at"),
               "replayed-from": f"{config.DLQ_TOPIC}@{offset}", "replayed-at": now_iso()}
    errors = []
    producer = Producer(config.producer_conf("py-ops"))
    producer.produce(config.TOPIC, key=m.key(), value=m.value(), headers=to_kafka(headers),
                     on_delivery=lambda err, _m: err and errors.append(err))
    if producer.flush(10) or errors:
        log.error(APP, f"replay failed: {errors[0] if errors else 'flush timed out'}")
        return 1
    log.info(APP, f"replayed {config.DLQ_TOPIC}@{offset} key={text(m.key())} "
                  f"message-id={h.get('message-id')} -> {config.TOPIC}")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Inspect and replay orders.dlq")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--list", action="store_true", help="table of the most recent DLQ records")
    g.add_argument("--show", type=int, metavar="OFFSET", help="full value and headers of one record")
    g.add_argument("--replay", type=int, metavar="OFFSET", help="re-produce one record to orders")
    p.add_argument("--limit", type=int, default=20, help="rows for --list (default 20)")
    args = p.parse_args(argv)

    consumer = make_consumer()
    try:
        if args.list:
            cmd_list(consumer, args.limit)
        elif args.show is not None:
            cmd_show(consumer, args.show)
        else:
            return cmd_replay(consumer, args.replay)
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
