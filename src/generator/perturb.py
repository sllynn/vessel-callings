"""Perturber — apply lateness / GPS jitter / per-vessel lane offset /
dropout to clean emissions.

Deterministic from a seed. Each perturbation runs independently so they
compose cleanly. The result is a list of `(record, ingest_offset_seconds)`
tuples — the stepper produces clean event_ts; perturb decides when each
record is *written* by adding an `ingest_offset_seconds` to the wall clock.
"""
from __future__ import annotations
from dataclasses import dataclass, field
import math
import random
from typing import Sequence

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
    # Per-vessel cross-track offset σ. Each vessel is drawn once and keeps
    # its lane for the duration of the run, so vessels sharing a route fan
    # out into parallel lanes instead of overlapping. Set to 0 to disable.
    lane_sigma_m: float = 800.0
    dropout_pct: float = 0.5
    mmsi_swap_pct: float = 0.0

    # Cache of per-vessel starboard offsets (metres). Populated lazily.
    _lane_offsets: dict[str, float] = field(default_factory=dict, repr=False)

    def _lane_offset(self, vessel_id: str) -> float:
        if self.lane_sigma_m <= 0:
            return 0.0
        cached = self._lane_offsets.get(vessel_id)
        if cached is None:
            cached = self.rng.gauss(0.0, self.lane_sigma_m)
            self._lane_offsets[vessel_id] = cached
        return cached

    def apply(self, batch: Sequence[PositionRecord]) -> list[Perturbed]:
        out: list[Perturbed] = []
        sigma_lat_deg = self.gps_jitter_m / 111_320.0
        max_late_s = self.lateness_max_hours * 3600.0
        for rec in batch:
            # 1. Dropout
            if self.rng.random() * 100 < self.dropout_pct:
                continue

            # 2. GPS jitter (Gaussian, latitude-corrected for longitude)
            cos_lat = max(0.01, abs(math.cos(math.radians(rec.lat))))
            jitter_lat = self.rng.gauss(0.0, sigma_lat_deg)
            jitter_lon = self.rng.gauss(0.0, sigma_lat_deg / cos_lat)

            # 3. Per-vessel lane offset, perpendicular to current cog.
            #    cog is clockwise-from-north; starboard bearing = cog + 90°.
            #    Unit vector for a bearing β with x=east, y=north is
            #    (sin β, cos β); convert metres to degrees as for jitter.
            lane_m = self._lane_offset(rec.vessel_id)
            if lane_m:
                perp_rad = math.radians(rec.cog + 90.0)
                lane_lat_deg = lane_m * math.cos(perp_rad) / 111_320.0
                lane_lon_deg = lane_m * math.sin(perp_rad) / (111_320.0 * cos_lat)
            else:
                lane_lat_deg = lane_lon_deg = 0.0

            rec2 = PositionRecord(
                mmsi=rec.mmsi, vessel_id=rec.vessel_id, event_ts=rec.event_ts,
                lon=rec.lon + jitter_lon + lane_lon_deg,
                lat=rec.lat + jitter_lat + lane_lat_deg,
                sog=rec.sog, cog=rec.cog, heading=rec.heading,
                nav_status=rec.nav_status, vessel_type=rec.vessel_type,
                vessel_name=rec.vessel_name,
            )

            # 4. Lateness (truncated lognormal up to lateness_max_hours)
            if self.rng.random() * 100 < self.lateness_pct:
                sample = self.rng.lognormvariate(0.0, 1.0) * 600.0  # ~10 min median
                offset = min(sample, max_late_s)
            else:
                offset = 0.0

            # 5. MMSI swap (off by default)
            if self.mmsi_swap_pct > 0 and self.rng.random() * 100 < self.mmsi_swap_pct:
                rec2 = PositionRecord(**{**rec2.__dict__, "mmsi": rec2.mmsi ^ 1})

            out.append(Perturbed(record=rec2, ingest_offset_s=offset))
        return out
