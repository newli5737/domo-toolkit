"""DomoLogger — Logging chi tiết ra console với colors."""

import sys
import traceback
from datetime import datetime


class DomoLogger:
    """Logger chi tiết — in ra console với timestamp, module name, và colors."""

    COLORS = {
        "INFO": "\033[36m",      # Cyan
        "ERROR": "\033[31m",     # Red
        "WARN": "\033[33m",      # Yellow
        "DEBUG": "\033[90m",     # Gray
        "SUCCESS": "\033[32m",   # Green
        "STEP": "\033[35m",      # Magenta
        "PROGRESS": "\033[34m",  # Blue
    }
    RESET = "\033[0m"

    def __init__(self, name: str = "domo"):
        self.name = name

    def _log(self, level: str, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        color = self.COLORS.get(level, "")
        reset = self.RESET
        print(f"{color}[{timestamp}] [{self.name}] [{level}] {msg}{reset}", flush=True)

    def info(self, msg: str):
        self._log("INFO", msg)

    def error(self, msg: str):
        self._log("ERROR", msg)

    def warn(self, msg: str):
        self._log("WARN", msg)

    def debug(self, msg: str):
        self._log("DEBUG", msg)

    def success(self, msg: str):
        self._log("SUCCESS", msg)

    def step(self, step_num: int, total_steps: int, msg: str):
        self._log("STEP", f"[Step {step_num}/{total_steps}] {msg}")

    def progress(self, current: int, total: int, label: str = ""):
        if total > 0:
            pct = (current / total) * 100
            bar_len = 30
            filled = int(bar_len * current / total)
            bar = "█" * filled + "░" * (bar_len - filled)
            self._log("PROGRESS", f"{label} [{bar}] {current}/{total} ({pct:.1f}%)")
        else:
            self._log("PROGRESS", f"{label} {current}/??")

    def exception(self, msg: str, exc: Exception = None):
        self._log("ERROR", f"{msg}")
        if exc:
            tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
            for line in tb:
                for sub_line in line.rstrip().split('\n'):
                    self._log("ERROR", f"  {sub_line}")
