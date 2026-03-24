"""DomoLogger — Logging đơn giản ra console."""

import sys
from datetime import datetime


class DomoLogger:
    """Logger đơn giản — in ra console với timestamp."""

    def __init__(self, name: str = "domo"):
        self.name = name

    def _log(self, level: str, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] [{level}] {msg}", flush=True)

    def info(self, msg: str):
        self._log("INFO", msg)

    def error(self, msg: str):
        self._log("ERROR", msg)

    def warn(self, msg: str):
        self._log("WARN", msg)

    def debug(self, msg: str):
        self._log("DEBUG", msg)
