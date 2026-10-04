"""Row counts and retry/DLQ end offsets. Run before and after each demo step."""
from confluent_kafka import Consumer, KafkaException, TopicPartition

from common import config, db

PARTITIONS = {config.RETRY_TOPIC: 3, config.DLQ_TOPIC: 1}


def main() -> int:
    conn = db.connect()
    for table, n in db.counts(conn).items():
        print(f"{table + ' rows':<26} {n}")
    conn.close()

    # No subscribe(): reading watermarks does not join the group or commit anything.
    consumer = Consumer({**config.consumer_conf("py-consumer", config.GROUP), "log_level": 2})
    try:
        for topic, parts in PARTITIONS.items():
            ends = [consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)[1] for p in range(parts)]
            print(f"{topic + ' end offsets':<26} {sum(ends)}  {ends}")
    except KafkaException as e:
        print(f"kafka unavailable: {e}")
    finally:
        consumer.close()
    print(f"{'downstream':<26} {'DOWN (.fail_downstream present)' if config.FAIL_FLAG.exists() else 'up'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
