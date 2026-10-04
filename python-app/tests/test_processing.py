"""Unit tests for dedup, validation, retries and routing. No Kafka needed:
python -m unittest discover -s tests   (from python-app/)"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from common import config, db
from common.failure import PermanentError, RoutingError, TransientError
from common.headers import as_dict
from common.processing import handle_record, validate


class FakeMsg:
    def __init__(self, value, key="ORD-1", headers=None, topic="orders", partition=0, offset=0):
        self._v, self._k, self._h = value, key.encode(), headers
        self._t, self._p, self._o = topic, partition, offset

    def value(self): return self._v.encode() if isinstance(self._v, str) else self._v
    def key(self): return self._k
    def headers(self): return [(k, str(v).encode()) for k, v in (self._h or {}).items()] or None
    def topic(self): return self._t
    def partition(self): return self._p
    def offset(self): return self._o


class FakeProducer:
    """Records produce() calls; delivery succeeds unless fail=True."""
    def __init__(self, fail=False):
        self.sent, self.fail, self._cb = [], fail, []

    def produce(self, topic, key, value, headers, on_delivery):
        self.sent.append((topic, key, value, dict((k, v.decode()) for k, v in headers)))
        self._cb.append(on_delivery)

    def flush(self, _timeout=None):
        for cb in self._cb:
            cb("broker down" if self.fail else None, None)
        self._cb = []
        return 0


def order(order_id="ORD-1", amount=10.5):
    return json.dumps({"orderId": order_id, "customerId": "C-1", "amount": amount,
                       "currency": "SGD", "createdAt": "2026-10-02T00:00:00Z"})


def msg(order_id="ORD-1", message_id="m-1", value=None, **kw):
    return FakeMsg(value if value is not None else order(order_id), key=order_id,
                   headers={"message-id": message_id, "produced-at": "t0", **kw.pop("headers", {})}, **kw)


class Base(unittest.TestCase):
    def setUp(self):
        self.conn = db.connect(":memory:")
        self.producer = FakeProducer()
        self.logs = []
        self.log = lambda m, k, event, detail="": self.logs.append(event)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        flag = mock.patch.object(config, "FAIL_FLAG", Path(tmp.name) / ".fail_downstream")
        flag.start()
        self.addCleanup(flag.stop)

    def handle(self, m, hops=0):
        return handle_record(m, self.conn, self.producer, self.log, hops_so_far=hops, sleep=lambda _s: None)

    def downstream_down(self):
        config.FAIL_FLAG.touch()


class ValidateTest(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(validate(order())["orderId"], "ORD-1")

    def test_rejects(self):
        for bad in ["{not json", "[]", json.dumps({"orderId": "X"}), order(amount=-5), order(amount=0),
                    order().replace("10.5", "true"), None]:
            with self.subTest(bad=bad), self.assertRaises(PermanentError):
                validate(bad)


class DedupTest(Base):
    def test_processes_once(self):
        self.assertEqual(self.handle(msg()), "PROCESSED")
        self.assertEqual(self.handle(msg(offset=5)), "DUPLICATE")   # redelivery / re-send
        self.assertEqual(db.counts(self.conn), {"orders": 1, "processed_messages": 1})

    def test_race_duplicate_detected_by_primary_key(self):
        with mock.patch.object(db, "is_processed", return_value=False):
            self.handle(msg())
            self.assertEqual(self.handle(msg()), "DUPLICATE")
        self.assertEqual(db.counts(self.conn)["orders"], 1)

    def test_same_order_new_message_id_goes_to_dlq(self):
        self.handle(msg(message_id="m-1"))
        self.assertEqual(self.handle(msg(message_id="m-2")), "DLQ")
        self.assertEqual(self.producer.sent[0][3]["error-class"], "PermanentError")
        self.assertEqual(db.counts(self.conn)["orders"], 1)

    def test_txn_rolls_back_both_rows(self):
        bad = json.loads(order())
        bad["customerId"] = None   # NOT NULL fails after the processed_messages insert
        with self.assertRaises(PermanentError):
            db.apply_order_txn(self.conn, bad, "m-x", "orders", 0, 0)
        self.assertFalse(db.is_processed(self.conn, "m-x"))


class FailureRoutingTest(Base):
    def test_poison_goes_to_dlq_without_retries(self):
        self.assertEqual(self.handle(msg(value="{not json", offset=7)), "DLQ")
        topic, _k, value, h = self.producer.sent[0]
        self.assertEqual(topic, "orders.dlq")
        self.assertEqual(value, b"{not json")
        self.assertEqual(h["error-class"], "PermanentError")
        self.assertEqual((h["original-topic"], h["original-offset"], h["message-id"]), ("orders", "7", "m-1"))
        self.assertFalse(any(e.startswith("RETRY") for e in self.logs))

    def test_missing_message_id_goes_to_dlq(self):
        self.assertEqual(self.handle(FakeMsg(order(), headers={})), "DLQ")

    def test_transient_retries_in_place_then_retry_topic(self):
        self.downstream_down()
        self.assertEqual(self.handle(msg()), "RETRY")
        self.assertEqual([e for e in self.logs if e.startswith("RETRY")], ["RETRY 1/3 in 0.5s", "RETRY 2/3 in 1.0s"])
        topic, _k, _v, h = self.producer.sent[0]
        self.assertEqual((topic, h["retry-count"]), ("orders.retry", "1"))
        self.assertIn("not-before", h)

    def test_recovers_in_place(self):
        with mock.patch("common.processing.call_downstream", side_effect=[TransientError("down"), None]):
            self.assertEqual(self.handle(msg()), "PROCESSED")
        self.assertEqual(self.logs, ["RETRY 1/3 in 0.5s", "PROCESSED"])

    def test_hops_then_dlq(self):
        self.downstream_down()
        retry_msg = msg(topic="orders.retry", partition=2, offset=3,
                        headers={"retry-count": 3, "original-topic": "orders",
                                 "original-partition": 1, "original-offset": 42})
        self.assertEqual(self.handle(retry_msg, hops=3), "DLQ")
        topic, _k, _v, h = self.producer.sent[0]
        self.assertEqual(topic, "orders.dlq")
        self.assertEqual((h["error-class"], h["retry-count"]), ("TransientError", "3"))
        self.assertEqual((h["original-partition"], h["original-offset"]), ("1", "42"))   # first failure wins

    def test_hop_increments(self):
        self.downstream_down()
        self.assertEqual(self.handle(msg(topic="orders.retry"), hops=1), "RETRY")
        self.assertEqual(self.producer.sent[0][3]["retry-count"], "2")

    def test_unacked_publish_raises_so_offset_is_not_committed(self):
        self.producer = FakeProducer(fail=True)
        with self.assertRaises(RoutingError):
            self.handle(msg(value="{not json"))

    def test_unexpected_error_goes_to_dlq(self):
        with mock.patch("common.processing.validate", side_effect=KeyError("bug")):
            self.assertEqual(self.handle(msg()), "DLQ")
        self.assertEqual(self.producer.sent[0][3]["error-class"], "KeyError")


class HeadersTest(unittest.TestCase):
    def test_as_dict(self):
        self.assertEqual(as_dict(FakeMsg("x", headers={"a": 1})), {"a": "1"})


if __name__ == "__main__":
    unittest.main()
