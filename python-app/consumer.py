"""Main consumer: topic orders, group orders-processor, user py-consumer.

Per record: dedup -> validate -> downstream + DB txn (in-place retries) -> route failures
-> commit the offset ONLY after the DB commit or the acked retry/DLQ publish.
"""
import os
import time

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from common import config, db, log
from common.failure import RoutingError
from common.lifecycle import Shutdown
from common.processing import handle_record

APP = "consumer"


def on_assign(_consumer, partitions):
    log.info(APP, f"assigned {sorted(p.partition for p in partitions)}")


def on_revoke(_consumer, partitions):
    # Offsets are committed synchronously per record, so nothing is pending here.
    log.info(APP, f"revoked {sorted(p.partition for p in partitions)}")


def commit(consumer, msg) -> None:
    try:
        consumer.commit(message=msg, asynchronous=False)
    except KafkaException as e:
        # e.g. partition revoked by a rebalance: the record is redelivered and dedup skips it.
        log.error(APP, f"commit p{msg.partition()}@{msg.offset()} failed: {e}")


def main() -> int:
    if os.getenv("FAIL_DOWNSTREAM", "").lower() == "true":
        config.FAIL_FLAG.touch()
    stop = Shutdown(APP)
    conn = db.connect()
    consumer = Consumer(config.consumer_conf("py-consumer", config.GROUP))
    side = Producer(config.producer_conf("py-consumer"))   # publishes to orders.retry / orders.dlq
    emit = log.make_logger(APP)
    consumer.subscribe([config.TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    try:
        while stop.running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error(APP, str(msg.error()))
                if msg.error().fatal():
                    return 1
                continue
            try:
                handle_record(msg, conn, side, emit, hops_so_far=0)
            except RoutingError as e:
                # Not acked => do not commit. Rewind so the record is tried again.
                log.error(APP, f"{e}; rewinding to p{msg.partition()}@{msg.offset()}")
                consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                time.sleep(1)
                continue
            commit(consumer, msg)
    finally:
        side.flush(10)
        consumer.close()   # leaves the group: the rebalance is immediate
        conn.close()
        log.info(APP, "closed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
