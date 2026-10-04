"""Retry consumer: topic orders.retry, group orders-retry-processor, user py-consumer.

A record waits until its not-before header. Never sleep() for that: it would exceed
max.poll.interval.ms and trigger a rebalance. Instead pause + seek the partition and
keep polling (heartbeats continue), then resume when the time is due.
"""
import time

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from common import config, db, log
from common.failure import RoutingError
from common.headers import get_header
from common.lifecycle import Shutdown
from common.processing import handle_record

APP = "retry"


def now_ms() -> int:
    return int(time.time() * 1000)


class RetryConsumer:
    def __init__(self):
        self.consumer = Consumer(config.consumer_conf("py-consumer", config.RETRY_GROUP))
        self.side = Producer(config.producer_conf("py-consumer"))
        self.conn = db.connect()
        self.emit = log.make_logger(APP)
        self.paused = {}   # (topic, partition) -> not-before epoch ms

    def on_assign(self, _consumer, partitions):
        for p in partitions:
            self.paused.pop((p.topic, p.partition), None)
        log.info(APP, f"assigned {sorted(p.partition for p in partitions)}")

    def on_revoke(self, _consumer, partitions):
        for p in partitions:
            self.paused.pop((p.topic, p.partition), None)
        log.info(APP, f"revoked {sorted(p.partition for p in partitions)}")

    def resume_due(self) -> None:
        due = [tp for tp, until in self.paused.items() if until <= now_ms()]
        if due:
            self.consumer.resume([TopicPartition(t, p) for t, p in due])
            for tp in due:
                del self.paused[tp]

    def wait(self, msg, not_before: int) -> None:
        tp = TopicPartition(msg.topic(), msg.partition(), msg.offset())
        self.consumer.pause([tp])
        self.consumer.seek(tp)   # required: without it the record is skipped on resume
        self.paused[(msg.topic(), msg.partition())] = not_before
        self.emit(msg, log.key_of(msg), "WAITING",
                  f"until {time.strftime('%H:%M:%S', time.localtime(not_before / 1000))}")

    def commit(self, msg) -> None:
        try:
            self.consumer.commit(message=msg, asynchronous=False)
        except KafkaException as e:
            log.error(APP, f"commit p{msg.partition()}@{msg.offset()} failed: {e}")

    def run(self, stop: Shutdown) -> int:
        self.consumer.subscribe([config.RETRY_TOPIC], on_assign=self.on_assign, on_revoke=self.on_revoke)
        try:
            while stop.running:
                self.resume_due()
                msg = self.consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    log.error(APP, str(msg.error()))
                    if msg.error().fatal():
                        return 1
                    continue
                if (msg.topic(), msg.partition()) in self.paused:
                    continue   # prefetched before the pause; the seek will deliver it again

                not_before = int(get_header(msg, "not-before") or 0)
                if now_ms() < not_before:
                    self.wait(msg, not_before)
                    continue

                hops = int(get_header(msg, "retry-count") or 0)
                try:
                    handle_record(msg, self.conn, self.side, self.emit, hops_so_far=hops)
                except RoutingError as e:
                    log.error(APP, f"{e}; rewinding to p{msg.partition()}@{msg.offset()}")
                    self.consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    time.sleep(1)
                    continue
                self.commit(msg)
        finally:
            self.side.flush(10)
            self.consumer.close()
            self.conn.close()
            log.info(APP, "closed")
        return 0


def main() -> int:
    stop = Shutdown(APP)
    return RetryConsumer().run(stop)


if __name__ == "__main__":
    raise SystemExit(main())
