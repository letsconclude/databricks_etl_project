"""
Structured JSON logging so pipeline logs can be routed to a log analytics
workspace / cluster driver logs and queried reliably (instead of grepping
free-text print statements).
"""
from __future__ import annotations

import json
import logging
import sys
import time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)  # type: ignore[attr-defined]
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class StageTimer:
    """Context manager that logs elapsed time and row counts for a pipeline stage."""

    def __init__(self, logger: logging.Logger, stage: str):
        self.logger = logger
        self.stage = stage
        self.start = 0.0

    def __enter__(self) -> "StageTimer":
        self.start = time.time()
        self.logger.info(f"stage started: {self.stage}", extra={"extra_fields": {"stage": self.stage, "event": "start"}})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        elapsed = round(time.time() - self.start, 2)
        if exc_type is None:
            self.logger.info(
                f"stage completed: {self.stage}",
                extra={"extra_fields": {"stage": self.stage, "event": "success", "elapsed_seconds": elapsed}},
            )
        else:
            self.logger.error(
                f"stage failed: {self.stage}",
                extra={"extra_fields": {"stage": self.stage, "event": "failure", "elapsed_seconds": elapsed}},
                exc_info=True,
            )
