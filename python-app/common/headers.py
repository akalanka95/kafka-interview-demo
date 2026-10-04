"""Kafka headers are (str, bytes) pairs; these helpers keep the rest of the code on str."""
from datetime import datetime, timezone

# Headers that identify the business message and survive every hop.
CARRIED = ("message-id", "produced-at")
ORIGIN = ("original-topic", "original-partition", "original-offset")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def get_header(msg, name: str):
    for k, v in msg.headers() or []:
        if k == name:
            return v.decode("utf-8", "replace") if isinstance(v, bytes) else v
    return None


def as_dict(msg) -> dict:
    return {k: (v.decode("utf-8", "replace") if isinstance(v, bytes) else v) for k, v in msg.headers() or []}


def to_kafka(headers: dict) -> list:
    return [(k, str(v).encode("utf-8")) for k, v in headers.items() if v is not None]


def copy_original_headers(msg) -> dict:
    """Carried headers + origin trace. The first failure wins, so the trace survives retry hops."""
    src = as_dict(msg)
    out = {k: src[k] for k in CARRIED if k in src}
    if "original-topic" in src:
        out.update({k: src[k] for k in ORIGIN if k in src})
    else:
        out.update({"original-topic": msg.topic(), "original-partition": msg.partition(),
                    "original-offset": msg.offset()})
    return out
