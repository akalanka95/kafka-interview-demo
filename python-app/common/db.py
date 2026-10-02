"""SQLite storage. The processed_messages table is the dedup key for exactly-once processing:
it is written in the SAME transaction as the business row, so both happen or neither does."""
import sqlite3

from . import config
from .failure import PermanentError, TransientError
from .headers import now_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
  order_id     TEXT PRIMARY KEY,
  customer_id  TEXT NOT NULL,
  amount       REAL NOT NULL CHECK (amount > 0),
  currency     TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  message_id   TEXT NOT NULL UNIQUE,
  processed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS processed_messages (
  message_id   TEXT PRIMARY KEY,
  topic        TEXT NOT NULL,
  partition    INTEGER NOT NULL,
  "offset"     INTEGER NOT NULL,
  processed_at TEXT NOT NULL
);
"""


class DuplicateMessage(Exception):
    """Another process committed this message-id first (race between consumers)."""


def connect(path=None) -> sqlite3.Connection:
    path = path or config.DB_PATH
    if str(path) != ":memory:":
        config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    # isolation_level=None: we issue BEGIN/COMMIT ourselves.
    conn = sqlite3.connect(str(path), timeout=5, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")      # concurrent readers + 1 writer across processes
    conn.execute("PRAGMA busy_timeout=5000")     # wait instead of failing on "database is locked"
    conn.executescript(SCHEMA)
    return conn


def is_processed(conn, message_id: str) -> bool:
    return conn.execute("SELECT 1 FROM processed_messages WHERE message_id = ?", (message_id,)).fetchone() is not None


def apply_order_txn(conn, order: dict, message_id: str, topic: str, partition: int, offset: int) -> None:
    now = now_iso()
    try:
        conn.execute("BEGIN IMMEDIATE")          # take the write lock up front: no upgrade deadlocks
    except sqlite3.OperationalError as e:        # locked past busy_timeout
        raise TransientError(f"database busy: {e}") from e
    try:
        conn.execute('INSERT INTO processed_messages (message_id, topic, partition, "offset", processed_at) '
                     "VALUES (?, ?, ?, ?, ?)", (message_id, topic, partition, offset, now))
        conn.execute("INSERT INTO orders (order_id, customer_id, amount, currency, created_at, message_id, processed_at) "
                     "VALUES (?, ?, ?, ?, ?, ?, ?)",
                     (order["orderId"], order["customerId"], order["amount"], order["currency"],
                      order["createdAt"], message_id, now))
        conn.execute("COMMIT")
    except sqlite3.IntegrityError as e:
        conn.execute("ROLLBACK")
        if "processed_messages" in str(e):
            raise DuplicateMessage(message_id) from e
        raise PermanentError(f"conflicting order {order['orderId']}: {e}") from e
    except sqlite3.OperationalError as e:
        conn.execute("ROLLBACK")
        raise TransientError(f"database error: {e}") from e
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def counts(conn) -> dict:
    return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in ("orders", "processed_messages")}
