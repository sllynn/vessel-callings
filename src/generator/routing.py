"""RouteCache — wrapper around searoute with arc-length-aware leg metadata.

We call searoute once per (origin, dest) pair we encounter, cache the result,
and pre-compute per-waypoint cumulative arc lengths so the stepper can
interpolate position by sim-clock × speed in O(log N) bisect.
"""
from __future__ import annotations
from bisect import bisect_right
from dataclasses import dataclass, field
from typing import Any
import searoute as sr

from .world import haversine_km, bearing_deg


@dataclass
class Leg:
    """One materialised route: waypoints plus per-vertex cumulative arc length."""
    waypoints: list[tuple[float, float]]  # (lon, lat)
    cum_km: list[float]                   # cum_km[i] = arc length from start to waypoints[i]

    @property
    def length_km(self) -> float:
        return self.cum_km[-1]

    def position_at(self, arc_km: float) -> tuple[float, float, float]:
        """Interpolate (lon, lat, bearing_deg) at the given arc length along the leg.

        Bearing is the great-circle initial bearing from the previous waypoint to
        the next one — fine for the demo's vessel-heading visual.
        """
        arc_km = max(0.0, min(arc_km, self.length_km))
        i = bisect_right(self.cum_km, arc_km) - 1
        i = max(0, min(i, len(self.waypoints) - 2))
        seg_start, seg_end = self.waypoints[i], self.waypoints[i + 1]
        seg_len = self.cum_km[i + 1] - self.cum_km[i]
        if seg_len <= 0:
            return seg_end[0], seg_end[1], bearing_deg(seg_start, seg_end)
        t = (arc_km - self.cum_km[i]) / seg_len
        lon = seg_start[0] + t * (seg_end[0] - seg_start[0])
        lat = seg_start[1] + t * (seg_end[1] - seg_start[1])
        return lon, lat, bearing_deg(seg_start, seg_end)


@dataclass
class RouteCache:
    _cache: dict[tuple[tuple[float, float], tuple[float, float]], Leg] = field(default_factory=dict)

    def get(self, origin: tuple[float, float], destination: tuple[float, float]) -> Leg:
        key = (origin, destination)
        leg = self._cache.get(key)
        if leg is not None:
            return leg
        gj: dict[str, Any] = sr.searoute(origin, destination, units="km")
        waypoints = [(c[0], c[1]) for c in gj["geometry"]["coordinates"]]
        # Build cumulative arc-length so we can interpolate by km.
        cum_km = [0.0]
        for i in range(1, len(waypoints)):
            cum_km.append(cum_km[-1] + haversine_km(waypoints[i - 1], waypoints[i]))
        leg = Leg(waypoints=waypoints, cum_km=cum_km)
        self._cache[key] = leg
        return leg
