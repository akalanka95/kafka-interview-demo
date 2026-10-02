"""Graceful shutdown: Ctrl+C (SIGINT), SIGTERM and, on Windows, Ctrl+Break (SIGBREAK)."""
import os
import signal

from . import log


class Shutdown:
    def __init__(self, app: str):
        self.app = app
        self.running = True
        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            if hasattr(signal, name):
                signal.signal(getattr(signal, name), self._stop)
        log.info(app, f"started pid={os.getpid()} (Ctrl+C to stop)")

    def _stop(self, signum, _frame):
        if self.running:
            log.info(self.app, f"signal {signum}: finishing the in-flight record, then closing")
        self.running = False
