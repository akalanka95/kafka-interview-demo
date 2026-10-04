"""One aligned line per event, readable on a shared screen."""
import sys
import time


def position(msg) -> str:
    return f"p{msg.partition()}@{msg.offset()}" if msg is not None else "-"


def short_id(message_id) -> str:
    return f"msg={message_id[:8]}…" if message_id else ""


def make_logger(app: str):
    def log(msg, key, event: str, detail: str = "") -> None:
        line = f"{time.strftime('%H:%M:%S')} {app:<9} {position(msg):<8} {str(key or '-'):<10} {event:<22} {detail}"
        print(line.rstrip(), flush=True)
    return log


def key_of(msg) -> str:
    k = msg.key()
    return k.decode("utf-8", "replace") if isinstance(k, bytes) else (k or "-")


def info(app: str, text: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {app:<9} {text}", flush=True)


def error(app: str, text: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {app:<9} ERROR {text}", file=sys.stderr, flush=True)
