"""Structured JSON logging shared by all services.

Every log line is one JSON object, for example:

    {"timestamp": "2026-10-07T10:15:02.123+00:00", "level": "INFO",
     "service": "telemetry-producer", "stage": "ingestion",
     "event": "batch_sent", "message": "Sent 50 events", "record_count": 50}

Why JSON? Machines (Grafana/Loki, ELK, `jq`) can filter and count log
lines by field (e.g. all WARNINGs with event="idle_alert") instead of
parsing free text.

Usage:
    log = get_logger("telemetry-producer", stage="ingestion")
    log.info("Sent events", event="batch_sent", record_count=50)
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, MutableMapping

# The four pipeline stages we tag every log line with.
STAGES = ("ingestion", "processing", "storage", "serving")

# Attributes that every logging.LogRecord already has. Anything else on the
# record was added by us (stage, event, vehicle_id, ...) and goes into the JSON.
_STANDARD_ATTRS = set(vars(logging.makeLogRecord({})).keys()) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Turn a LogRecord into a single-line JSON string."""

    def __init__(self, service: str) -> None:
        """Remember the service name so it is added to every line."""
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        """Build the JSON payload: fixed fields first, then any context fields."""
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": self.service,
            "stage": getattr(record, "stage", "processing"),
            "event": getattr(record, "event", "log"),
            "message": record.getMessage(),
        }
        # Context fields such as vehicle_id, sim_date, batch_id, record_count.
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS and key not in payload:
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # default=str makes dates/Decimals serialisable instead of crashing.
        return json.dumps(payload, default=str)


class StructuredLogger(logging.LoggerAdapter):
    """Logger that accepts context as keyword arguments.

    Lets us write `log.info("msg", event="x", vehicle_id="V001")` instead of
    the more verbose `log.info("msg", extra={"event": "x", ...})`.
    """

    _RESERVED = {"exc_info", "stack_info", "stacklevel", "extra"}

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, MutableMapping[str, Any]]:
        """Move our custom keyword arguments into `extra` so logging accepts them."""
        extra = dict(self.extra or {})
        extra.update(kwargs.pop("extra", None) or {})
        for key in list(kwargs):
            if key not in self._RESERVED:
                extra[key] = kwargs.pop(key)
        kwargs["extra"] = extra
        return msg, kwargs


def get_logger(service: str, stage: str = "processing", level: str | None = None) -> StructuredLogger:
    """Return a JSON logger for a service.

    Args:
        service: name shown in every log line, e.g. "spark-streaming".
        stage: default pipeline stage (ingestion/processing/storage/serving);
            can be overridden per call with `stage=...`.
        level: log level; defaults to the LOG_LEVEL env var (INFO).
    """
    logger = logging.getLogger(service)
    if not logger.handlers:  # avoid duplicate handlers if called twice
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service))
        logger.addHandler(handler)
        logger.propagate = False  # don't also print through the root logger
    logger.setLevel(level or os.getenv("LOG_LEVEL", "INFO"))
    return StructuredLogger(logger, {"stage": stage})
