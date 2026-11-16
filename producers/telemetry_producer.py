"""Streaming source: sends simulated GPS/telemetry events to Kafka.

Each vehicle emits one event every EMIT_INTERVAL_MIN_S..EMIT_INTERVAL_MAX_S
real seconds. Messages are keyed by vehicle_id, so all events of one vehicle
go to the same partition of `fleet.telemetry` and keep their order.

Robustness features (each one is easy to point at in the code):
  * waits for Kafka with exponential backoff before starting   -> wait_for_kafka()
  * acks=all + enable.idempotence=true (no lost / duplicated writes on retry)
  * delivery callback counts successes and failures            -> on_delivery()
  * graceful shutdown on Ctrl+C / docker stop (flushes buffer) -> handle_signal()
  * rates and error injection configurable via env variables
"""
from __future__ import annotations

import json
import random
import signal
import time

from confluent_kafka import KafkaError, Message, Producer
from confluent_kafka.admin import AdminClient
from prometheus_client import Counter, Gauge, start_http_server

from common import sim_clock
from common.config import get_float, get_int, get_str
from common.fleet import build_fleet
from common.logging_config import get_logger
from producers.fleet_simulator import make_bad_record, new_vehicle_state, step

log = get_logger("telemetry-producer", stage="ingestion")

# ---------------- Prometheus metrics (scraped at :PRODUCER_METRICS_PORT/metrics) ----------------
EVENTS_PRODUCED = Counter("events_produced_total", "Events acknowledged by Kafka")
EVENTS_FAILED = Counter("events_failed_total", "Events Kafka could not deliver")
INVALID_INJECTED = Counter(
    "invalid_events_injected_total", "Deliberately corrupted events sent to test validation", ["reason"]
)
DUPLICATES_INJECTED = Counter("duplicate_events_injected_total", "Events deliberately sent twice")
LAST_SENT = Gauge("producer_last_sent_timestamp", "Unix time of the last event handed to Kafka")

_running = True  # set to False by the signal handler


def handle_signal(signum: int, _frame: object) -> None:
    """Ask the main loop to stop (Ctrl+C = SIGINT, `docker stop` = SIGTERM)."""
    global _running
    log.info("Shutdown signal received", event="shutdown_requested", signal=signum)
    _running = False


def wait_for_kafka(bootstrap: str, topic: str, max_retries: int) -> None:
    """Block until Kafka is reachable and the topic exists, retrying with backoff.

    Backoff doubles each attempt (1, 2, 4, 8, ... capped at 30 s) so we don't
    hammer a broker that is still starting.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for attempt in range(1, max_retries + 1):
        try:
            metadata = admin.list_topics(timeout=5)
            if topic in metadata.topics:
                partitions = len(metadata.topics[topic].partitions)
                log.info("Connected to Kafka", event="kafka_connected", topic=topic, partitions=partitions)
                return
            reason = f"topic {topic} not found yet"
        except Exception as exc:  # broker not up yet
            reason = str(exc)
        delay = min(2 ** (attempt - 1), 30)
        log.warning(
            "Kafka not ready, retrying", event="kafka_retry", attempt=attempt, retry_in_s=delay, reason=reason
        )
        time.sleep(delay)
    raise SystemExit(f"Kafka not reachable after {max_retries} attempts")


def on_delivery(err: KafkaError | None, msg: Message) -> None:
    """Called by the Kafka client once the broker accepted (or rejected) a message."""
    if err is not None:
        EVENTS_FAILED.inc()
        log.error("Delivery failed", event="delivery_failed", error=str(err), topic=msg.topic())
    else:
        EVENTS_PRODUCED.inc()


def send(producer: Producer, topic: str, event: dict) -> None:
    """Queue one event for sending. Key = vehicle_id (None for corrupted records)."""
    key = event.get("vehicle_id")
    payload = json.dumps(event).encode("utf-8")
    while True:
        try:
            producer.produce(topic, key=key.encode() if key else None, value=payload, on_delivery=on_delivery)
            break
        except BufferError:
            # Local send queue is full: let the client deliver some messages, then retry.
            producer.poll(1)
    producer.poll(0)  # trigger delivery callbacks for finished sends
    LAST_SENT.set(time.time())


def main() -> None:
    """Run the simulation loop until a shutdown signal arrives."""
    bootstrap = get_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = get_str("KAFKA_TELEMETRY_TOPIC", "fleet.telemetry")
    interval_min = get_float("EMIT_INTERVAL_MIN_S", 1.0)
    interval_max = get_float("EMIT_INTERVAL_MAX_S", 3.0)
    bad_rate = get_float("BAD_RECORD_RATE", 0.02)
    dup_rate = get_float("DUPLICATE_RATE", 0.01)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    start_http_server(get_int("PRODUCER_METRICS_PORT", 8001))
    wait_for_kafka(bootstrap, topic, get_int("KAFKA_CONNECT_MAX_RETRIES", 10))

    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",                 # wait until the broker has stored the message
            "enable.idempotence": True,    # retries never create duplicates in Kafka
            "linger.ms": 50,               # batch messages for up to 50 ms (efficiency)
            "client.id": "telemetry-producer",
        }
    )

    rng = random.Random()
    now = time.time()
    vehicles = [new_vehicle_state(p, now, rng) for p in build_fleet()]
    next_emit = {v.profile.vehicle_id: now + rng.uniform(0, interval_max) for v in vehicles}
    log.info(
        "Telemetry producer started",
        event="producer_started",
        vehicles=len(vehicles),
        topic=topic,
        sim_date=sim_clock.current_sim_date(),
        bad_record_rate=bad_rate,
    )

    sent, last_summary = 0, now
    while _running:
        now = time.time()
        for vehicle in vehicles:
            vid = vehicle.profile.vehicle_id
            if now < next_emit[vid]:
                continue
            event = step(vehicle, now, rng)
            send(producer, topic, event)
            sent += 1
            log.debug("Event sent", event="event_sent", vehicle_id=vid, status=event["status"])

            if rng.random() < dup_rate:  # same event_id twice -> the speed layer must dedupe
                send(producer, topic, event)
                DUPLICATES_INJECTED.inc()
            if rng.random() < bad_rate:  # corrupted extra record -> must end up in the DLQ
                bad, reason = make_bad_record(event, rng)
                send(producer, topic, bad)
                INVALID_INJECTED.labels(reason=reason).inc()

            next_emit[vid] = now + rng.uniform(interval_min, interval_max)

        if now - last_summary >= 10:  # one summary line every 10 s instead of one per event
            statuses = [v.status for v in vehicles]
            log.info(
                "Producer progress",
                event="producer_progress",
                record_count=sent,
                sim_date=sim_clock.current_sim_date(),
                sim_time=sim_clock.sim_now().strftime("%H:%M"),
                idle=statuses.count("idle"),
                enroute=statuses.count("enroute"),
                on_trip=statuses.count("on_trip"),
            )
            sent, last_summary = 0, now
        time.sleep(0.1)

    remaining = producer.flush(10)  # deliver anything still buffered before exiting
    log.info("Telemetry producer stopped", event="producer_stopped", undelivered=remaining)


if __name__ == "__main__":
    main()
