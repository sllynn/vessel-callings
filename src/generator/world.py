"""World — named ports manifest and small geospatial helpers."""
from __future__ import annotations
from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import Mapping
import yaml


EARTH_R_KM = 6371.0088


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Great-circle distance in km between two (lon, lat) points."""
    lon1, lat1 = radians(a[0]), radians(a[1])
    lon2, lat2 = radians(b[0]), radians(b[1])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    h = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return 2 * EARTH_R_KM * asin(sqrt(h))


def bearing_deg(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Initial bearing in degrees (0-360) from a to b, both (lon, lat)."""
    from math import atan2, degrees
    lon1, lat1 = radians(a[0]), radians(a[1])
    lon2, lat2 = radians(b[0]), radians(b[1])
    dlon = lon2 - lon1
    y = sin(dlon) * cos(lat2)
    x = cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)
    return (degrees(atan2(y, x)) + 360) % 360


@dataclass(frozen=True)
class Port:
    name: str
    lon: float
    lat: float
    region: str


@dataclass
class World:
    ports: Mapping[str, Port]

    @classmethod
    def load(cls, ports_yaml: str | Path) -> "World":
        raw = yaml.safe_load(Path(ports_yaml).read_text())
        ports = {
            p["name"]: Port(name=p["name"], lon=p["lon"], lat=p["lat"], region=p["region"])
            for p in raw["ports"]
        }
        return cls(ports=ports)

    @property
    def port_names(self) -> list[str]:
        return list(self.ports.keys())

    def pick_pair(self, rng) -> tuple[Port, Port]:
        """Pick two distinct ports uniformly at random."""
        a, b = rng.sample(self.port_names, 2)
        return self.ports[a], self.ports[b]
