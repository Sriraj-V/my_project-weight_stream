"""
producer.py — Scale-device simulator.

Emits weigh readings to Kafka at a configurable rate.
Intentionally re-emits a fraction of events (dupe_rate) to simulate
at-least-once delivery / device retries — exercises the Flink dedup logic.
Also injects a small fraction of corrupt events to populate the DLQ.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from collections import deque

from confluent_kafka import Producer
from schema import DEVICES, generate_event, corrupt_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weigh.readings.raw")
EMIT_RATE_HZ = float(os.getenv("EMIT_RATE_HZ", "10"))
DUPE_RATE = float(os.getenv("DUPE_RATE", "0.05"))   # fraction re-emitted
CORRUPT_RATE = float(os.getenv("CORRUPT_RATE", "0.02"))
DUPE_BUFFER_SIZE = 50   # pool of recent events eligible for re-emit

SLEEP_INTERVAL = 1.0 / EMIT_RATE_HZ

# ── Kafka producer ────────────────────────────────────────────────────────────
_producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
    "linger.ms": 10,
    "compression.type": "lz4",
})

recent_events: deque = deque(maxlen=DUPE_BUFFER_SIZE)

# ── Graceful shutdown ─────────────────────────────────────────────────────────
_running = True

def _shutdown(signum, frame):
    global _running
    log.info("Shutdown signal received — flushing and exiting.")
    _running = False

signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT, _shutdown)

# ── Helpers ───────────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err is not None:
        log.warning("Delivery failed for %s: %s", msg.key(), err)


def emit(event_dict: dict, key: str):
    _producer.produce(
        topic=TOPIC,
        key=key.encode(),
        value=json.dumps(event_dict).encode(),
        callback=delivery_report,
    )
    _producer.poll(0)  # trigger any waiting callbacks


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    log.info(
        "Producer started | topic=%s rate=%.1f Hz dupe=%.0f%% corrupt=%.0f%%",
        TOPIC, EMIT_RATE_HZ, DUPE_RATE * 100, CORRUPT_RATE * 100,
    )
    sent = dupes = corrupts = 0

    while _running:
        tick = time.monotonic()

        # 1) decide: fresh event, dupe, or corrupt
        roll = random.random()

        if roll < CORRUPT_RATE:
            # generate and corrupt a fresh event
            ev = corrupt_event(generate_event())
            emit(ev.to_dict(), ev.kafka_key)
            corrupts += 1
        elif roll < CORRUPT_RATE + DUPE_RATE and recent_events:
            # re-emit a recent event (duplicate)
            ev_dict = random.choice(list(recent_events))
            emit(ev_dict, ev_dict["device_id"])
            dupes += 1
        else:
            # normal fresh event
            ev = generate_event()
            ev_dict = ev.to_dict()
            emit(ev_dict, ev.kafka_key)
            recent_events.append(ev_dict)
            sent += 1

        if (sent + dupes + corrupts) % (int(EMIT_RATE_HZ) * 10) == 0:
            log.info("total=%d  fresh=%d  dupes=%d  corrupt=%d",
                     sent + dupes + corrupts, sent, dupes, corrupts)

        elapsed = time.monotonic() - tick
        time.sleep(max(0, SLEEP_INTERVAL - elapsed))

    _producer.flush(timeout=10)
    log.info("Producer stopped. total=%d fresh=%d dupes=%d corrupt=%d",
             sent + dupes + corrupts, sent, dupes, corrupts)


if __name__ == "__main__":
    main()
