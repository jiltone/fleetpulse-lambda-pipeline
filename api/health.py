"""Health checks behind GET /health.

Overall status:
    unhealthy  - Postgres is down: the API cannot answer anything useful
    degraded   - the API works, but something upstream is wrong (Kafka down,
                 no fresh streaming data, or the daily batch is late)
    healthy    - everything is fine
"""
from __future__ import annotations

import socket
import time
from datetime import datetime, timedelta, timezone

from api import db
from api.models import CheckResult, HealthResponse
from common import sim_clock
from common.config import get_int, get_str


def check_postgres() -> CheckResult:
    """Can we run a query?"""
    try:
        db.ping()
        return CheckResult(status="ok", detail="query succeeded")
    except Exception as exc:
        return CheckResult(status="fail", detail=f"cannot query Postgres: {exc}")


def check_kafka() -> CheckResult:
    """Is the Kafka broker accepting TCP connections?

    A plain socket connect is enough to know the broker is reachable, without
    pulling a Kafka client library into the API.
    """
    host, _, port = get_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")[0].partition(":")
    start = time.perf_counter()
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=2):
            pass
        return CheckResult(status="ok", detail=f"{host}:{port} reachable in {(time.perf_counter() - start) * 1000:.0f} ms")
    except OSError as exc:
        return CheckResult(status="fail", detail=f"{host}:{port} unreachable: {exc}")


def check_stream_freshness() -> CheckResult:
    """Has the speed layer stored an event recently?"""
    max_age = get_int("FRESHNESS_MAX_SECONDS", 120)
    last = db.last_event_time()
    if last is None:
        return CheckResult(status="warn", detail="no streaming data stored yet")
    age = (datetime.now(timezone.utc) - last).total_seconds()
    if age > max_age:
        return CheckResult(status="warn", detail=f"last event {age:.0f}s ago (limit {max_age}s)")
    return CheckResult(status="ok", detail=f"last event {age:.0f}s ago")


def check_batch_freshness() -> CheckResult:
    """Has the batch layer finished yesterday's (or at worst the day before's) report?"""
    today = sim_clock.current_sim_date()
    latest = db.latest_profitability_date()
    if latest is None:
        if sim_clock.sim_day_index(today) < 2:
            return CheckResult(status="ok", detail="no report yet (simulation still in its first day)")
        return CheckResult(status="warn", detail="no profitability report has been produced")
    if latest < today - timedelta(days=2):
        return CheckResult(status="warn", detail=f"latest report is for {latest}, sim date is {today}")
    return CheckResult(status="ok", detail=f"latest report is for {latest}")


def run_health_checks() -> HealthResponse:
    """Run every check and combine them into one status."""
    checks = {"postgres": check_postgres(), "kafka": check_kafka()}
    if checks["postgres"].status == "fail":
        skipped = CheckResult(status="warn", detail="skipped: database unavailable")
        checks.update(stream_freshness=skipped, batch_freshness=skipped)
        status = "unhealthy"
    else:
        checks.update(stream_freshness=check_stream_freshness(), batch_freshness=check_batch_freshness())
        status = "healthy" if all(c.status == "ok" for c in checks.values()) else "degraded"
    return HealthResponse(status=status, timestamp=datetime.now(timezone.utc), checks=checks)
