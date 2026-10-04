"""Orders producer (user py-producer). Idempotent: enable.idempotence=true, acks=all.

  python producer.py --count 20                    20 new valid orders
  python producer.py --duplicate 5                 re-send the last 5 records with the SAME message-id
  python producer.py --poison 2                    malformed records (straight to the DLQ)
  python producer.py --count 200 --rate 20         throttle to 20 msg/s (time to kill the consumer)
  python producer.py --user intruder --count 1     no ACLs => authorization error

The idempotent producer only removes duplicates caused by its OWN retries. An app-level
re-send (--duplicate) looks like a new message to Kafka; the consumer's dedup table catches it.
"""
import argparse
import json
import random
import re
import time
import uuid

from confluent_kafka import KafkaException, Producer

from common import config, log
from common.headers import now_iso, to_kafka

APP = "producer"


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Orders producer")
    p.add_argument("--count", type=int, default=0, help="N new valid orders")
    p.add_argument("--duplicate", type=int, default=0,
                   help="re-send the last N records from data/last_sent.jsonl (same key, value, message-id)")
    p.add_argument("--poison", type=int, default=0, help="N malformed records (invalid JSON / schema violation)")
    p.add_argument("--rate", type=float, default=0, help="messages per second (default: unlimited)")
    p.add_argument("--user", default="py-producer", help="Kafka user (intruder for the ACL demo)")
    args = p.parse_args(argv)
    if min(args.count, args.duplicate, args.poison) < 0 or args.rate < 0:
        p.error("values must be >= 0")
    if not (args.count or args.duplicate or args.poison):
        p.error("give at least one of --count, --duplicate, --poison")
    return args


def read_last_sent() -> list:
    if not config.LAST_SENT.exists():
        return []
    with config.LAST_SENT.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def next_order_number(sent: list) -> int:
    nums = [int(m.group(1)) for r in sent if (m := re.fullmatch(r"ORD-(\d+)", r["key"]))]
    return max(nums, default=1000) + 1


def new_order(n: int) -> dict:
    return {"orderId": f"ORD-{n}", "customerId": f"C-{random.randint(1, 99)}",
            "amount": round(random.uniform(5, 500), 2), "currency": "SGD", "createdAt": now_iso()}


def poison_value(i: int) -> str:
    variants = ["{not json",
                json.dumps({"orderId": f"BAD-{i}", "customerId": "C-0", "currency": "SGD", "createdAt": now_iso()}),
                json.dumps({"orderId": f"BAD-{i}", "customerId": "C-0", "amount": -5, "currency": "SGD",
                            "createdAt": now_iso()})]
    return variants[i % len(variants)]


class Sender:
    def __init__(self, user: str, rate: float):
        self.producer = Producer({**config.producer_conf(user),
                                  "error_cb": lambda err: log.error(APP, f"client error: {err}")})
        self.delay = 1 / rate if rate else 0
        self.failed = 0
        self.ok = 0

    def send(self, key: str, value: str, message_id: str, kind: str) -> None:
        def on_delivery(err, msg):
            if err:
                self.failed += 1
                log.error(APP, f"{key} {kind} delivery failed: {err}")
                return
            self.ok += 1
            print(f"{time.strftime('%H:%M:%S')} {APP:<9} {f'p{msg.partition()}@{msg.offset()}':<8} "
                  f"{key:<10} SENT {kind:<17} msg={message_id[:8]}…", flush=True)
            if kind == "new":   # only valid new orders are candidates for --duplicate
                config.DATA_DIR.mkdir(parents=True, exist_ok=True)
                with config.LAST_SENT.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"key": key, "value": value, "message_id": message_id}) + "\n")

        headers = {"message-id": message_id, "produced-at": now_iso()}
        while True:
            try:
                self.producer.produce(config.TOPIC, key=key, value=value, headers=to_kafka(headers),
                                      on_delivery=on_delivery)
                break
            except BufferError:          # local queue full: serve callbacks and try again
                self.producer.poll(0.5)
        self.producer.poll(0)
        if self.delay:
            time.sleep(self.delay)

    def flush(self) -> int:
        remaining = self.producer.flush(30)
        if remaining:
            log.error(APP, f"{remaining} message(s) not delivered before timeout")
        return remaining


def main(argv=None) -> int:
    args = parse_args(argv)
    sender = Sender(args.user, args.rate)
    log.info(APP, f"user={args.user} topic={config.TOPIC}")
    try:
        if args.count:
            n = next_order_number(read_last_sent())
            for i in range(args.count):
                order = new_order(n + i)
                sender.send(order["orderId"], json.dumps(order), str(uuid.uuid4()), "new")
            sender.flush()   # so --duplicate in the same run can re-send these

        if args.duplicate:
            tail = read_last_sent()[-args.duplicate:]
            if len(tail) < args.duplicate:
                log.info(APP, f"only {len(tail)} record(s) in {config.LAST_SENT.name} to duplicate")
            for r in tail:
                sender.send(r["key"], r["value"], r["message_id"], "duplicate")

        for i in range(args.poison):
            sender.send(f"BAD-{i}", poison_value(i), str(uuid.uuid4()), "poison")
    except KafkaException as e:
        log.error(APP, str(e))
        return 1
    remaining = sender.flush()
    log.info(APP, f"delivered={sender.ok} failed={sender.failed + remaining}")
    return 1 if sender.failed or remaining else 0


if __name__ == "__main__":
    raise SystemExit(main())
