"""
schema.py — domain constants and event schema for the scale-device simulator.
"""

import random
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Literal
import uuid

# ── Domain constants ──────────────────────────────────────────────────────────

SITES = ["SITE_A", "SITE_B", "SITE_C", "SITE_D"]

DEVICES = [
    {"device_id": f"SCL-{site}-{i:02d}", "site_id": site, "max_capacity_kg": cap}
    for site in SITES
    for i, cap in enumerate([5000, 10000, 2000], start=1)
]

MATERIALS = ["STEEL_COIL", "ALUMINUM_BILLET", "COPPER_WIRE", "PLASTIC_PELLET", "RUBBER_BLOCK"]

DEVICE_STATUSES = ["OK", "OK", "OK", "OK", "CALIB_WARN", "FAULT"]  # weighted toward OK

UNIT_OF_MEASURE = "kg"


# ── Event dataclass ───────────────────────────────────────────────────────────

@dataclass
class WeighReading:
    event_id: str
    device_id: str
    site_id: str
    material_code: str
    gross_weight: float
    tare_weight: float
    net_weight: float
    unit_of_measure: str
    device_status: str
    operator_id: str
    shift: Literal["DAY", "EVENING", "NIGHT"]
    event_ts: str        # ISO-8601 UTC
    kafka_key: str = field(init=False)

    def __post_init__(self):
        self.kafka_key = self.device_id

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("kafka_key")
        return d


def _shift_from_hour(hour: int) -> str:
    if 6 <= hour < 14:
        return "DAY"
    elif 14 <= hour < 22:
        return "EVENING"
    return "NIGHT"


def generate_event(device: dict | None = None) -> WeighReading:
    """Generate a single realistic weigh-reading event."""
    if device is None:
        device = random.choice(DEVICES)

    max_cap = device["max_capacity_kg"]
    tare = round(random.uniform(50, max_cap * 0.1), 2)
    gross = round(tare + random.uniform(100, max_cap * 0.9), 2)
    net = round(gross - tare, 2)
    now = datetime.now(timezone.utc)

    return WeighReading(
        event_id=str(uuid.uuid4()),
        device_id=device["device_id"],
        site_id=device["site_id"],
        material_code=random.choice(MATERIALS),
        gross_weight=gross,
        tare_weight=tare,
        net_weight=net,
        unit_of_measure=UNIT_OF_MEASURE,
        device_status=random.choice(DEVICE_STATUSES),
        operator_id=f"OP-{random.randint(100, 199)}",
        shift=_shift_from_hour(now.hour),
        event_ts=now.isoformat(),
    )


def corrupt_event(event: WeighReading) -> WeighReading:
    """Randomly corrupt an event to simulate bad data (triggers reject path)."""
    corruption = random.choice(["negative_weight", "unknown_status", "bad_gross"])
    if corruption == "negative_weight":
        event.gross_weight = -abs(event.gross_weight)
        event.net_weight = event.gross_weight - event.tare_weight
    elif corruption == "unknown_status":
        event.device_status = "UNKNOWN_CODE"
    elif corruption == "bad_gross":
        event.gross_weight = event.tare_weight - 10  # gross < tare
        event.net_weight = event.gross_weight - event.tare_weight
    return event
