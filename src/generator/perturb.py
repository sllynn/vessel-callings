"""Perturber — apply lateness / GPS jitter / dropout to clean emissions.

Deterministic from a seed. Each perturbation runs independently so they
compose cleanly. The result is a list of `(record, ingest_offset_seconds)`
tuples — the stepper produces clean event_ts; perturb decides when each
record is *written* by adding an `ingest_offset_seconds` to the wall clock.
"""
from __future__ import annotations
from dataclasses import dataclass
from math import log
from typing import Sequence
import random

from .stepper import PositionRecord


@dataclass
class Perturbed:
    record: PositionRecord
    ingest_offset_s: float   # how long after the stepper's wall-clock to actually write the record


@dataclass
class Perturber:
    rng: random.Random
    lateness_pct: float = 5.0
    lateness_max_hours: float = 6.0
    gps_jitter_m: float = 10.0
    dropout_pct: float = 0.5
    mmsi_swap_pct: float = 0.0

    def apply(self, batch: Sequence[PositionRecord]) -> list[Perturbed]:
        out: list[Perturbed] = []
        # Convert GPS metres -> degrees of latitude (constant); longitude varies
        # by cos(lat) and is applied per-record.
        sigma_lat_deg = self.gps_jitter_m / 111_320.0
        max_late_s = self.lateness_max_hours * 3600.0
        for rec in batch:
            # 1. Dropout
            if self.rng.random() * 100 < self.dropout_pct:
                continue
            # 2. GPS jitter (Gaussian, latitude-corrected for longitude)
            jitter_lat = self.rng.gauss(0.0, sigma_lat_deg)
            cos_lat = max(0.01, abs(__import__("math").cos(__import__("math").radians(rec.lat))))
            jitter_lon = self.rng.gauss(0.0, sigma_lat_deg / cos_lat)
            rec2 = PositionRecord(
                mmsi=rec.mmsi, vessel_id=rec.vessel_id, event_ts=rec.event_ts,
                lon=rec.lon + jitter_lon, lat=rec.lat + jitter_lat,
                sog=rec.sog, cog=rec.cog, heading=rec.heading,
                nav_status=rec.nav_status, vessel_type=rec.vessel_type,
                vessel_name=rec.vessel_name,
            )
            # 3. Lateness (truncated lognormal up to lateness_max_hours)
            if self.rng.random() * 100 < self.lateness_pct:
                # lognormal(mu=0, sigma=1) has median 1; scale into hours then cap.
                sample = self.rng.lognormvariate(0.0, 1.0) * 600.0  # ~10 min median
                offset = min(sample, max_late_s)
            else:
                offset = 0.0
            # 4. MMSI swap (off by default)
            if self.mmsi_swap_pct > 0 and self.rng.random() * 100 < self.mmsi_swap_pct:
                # Flip a single bit in the MMSI to simulate transponder confusion.
                rec2 = PositionRecord(**{**rec2.__dict__, "mmsi": rec2.mmsi ^ 1})
            out.append(Perturbed(record=rec2, ingest_offset_s=offset))
        return out
