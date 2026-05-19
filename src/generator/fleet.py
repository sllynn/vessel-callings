"""Fleet — vessel profiles and per-vessel behaviour assignment.

For the MVP, all behaviours share one route primitive: searoute-routed legs
between two ports. The differences:

  liner   — ordered cycle through 3-5 ports, light dwell at each.
  tramp   — pick next port stochastically.
  ferry   — shuttle between a fixed pair of nearby ports.
  fishing — bounded wander inside an EEZ polygon. Out of MVP scope —
            for now fishing vessels behave like tramps with slower speeds.

All vessels are deterministic from a single seed.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Literal
import random

from .world import Port, World


Behaviour = Literal["liner", "tramp", "ferry", "fishing"]
VesselType = Literal["container", "bulk", "tanker", "fishing", "ferry"]


_NAME_PREFIXES = [
    "Aurora", "Bremen", "Cassiopeia", "Delphi", "Erebus", "Faro", "Galatea",
    "Halcyon", "Iberia", "Juno", "Kestrel", "Lyra", "Mistral", "Nereid",
    "Orion", "Pallas", "Quaestor", "Rigel", "Sirocco", "Tethys", "Umbra",
    "Vega", "Westerly", "Xanthe", "Ymir", "Zephyr",
]


@dataclass
class Vessel:
    mmsi: int
    vessel_id: str
    name: str
    type: VesselType
    behaviour: Behaviour
    speed_min_kn: float
    speed_max_kn: float
    course_change_rate_max: float  # deg per sim-minute (currently informational)
    draft_m: float
    # behaviour state — set at construction
    route_plan: list[str] = field(default_factory=list)  # ordered port names


def _draw_behaviour(rng: random.Random) -> tuple[Behaviour, VesselType, float, float]:
    """Return (behaviour, type, speed_min, speed_max). Mix follows fleet.yaml."""
    r = rng.random()
    if r < 0.30:
        return "liner", "container", 16.0, 24.0
    if r < 0.80:
        return "tramp", rng.choice(["bulk", "tanker"]), 10.0, 15.0
    if r < 0.85:
        return "ferry", "ferry", 18.0, 24.0
    return "fishing", "fishing", 3.0, 10.0


def _assign_route_plan(rng: random.Random, behaviour: Behaviour, world: World) -> list[str]:
    """Pick the ordered ports this vessel will cycle through."""
    if behaviour == "ferry":
        a, b = world.pick_pair(rng)
        return [a.name, b.name]
    if behaviour == "liner":
        n = rng.randint(3, 5)
        plan = rng.sample(world.port_names, n)
        return plan
    # tramp + fishing — start with a random pair; stepper picks next port on completion
    a, b = world.pick_pair(rng)
    return [a.name, b.name]


def build_fleet(seed: int, n: int, world: World) -> list[Vessel]:
    """Construct a deterministic fleet of n vessels."""
    rng = random.Random(seed)
    fleet: list[Vessel] = []
    for i in range(n):
        behaviour, vtype, vmin, vmax = _draw_behaviour(rng)
        prefix = rng.choice(_NAME_PREFIXES)
        suffix = rng.randint(100, 999)
        fleet.append(Vessel(
            mmsi=200_000_000 + i,            # avoids collision with real MIDs (which start 200-775)
            vessel_id=f"v-{i:06d}",
            name=f"MV {prefix}-{suffix}",
            type=vtype,
            behaviour=behaviour,
            speed_min_kn=vmin,
            speed_max_kn=vmax,
            course_change_rate_max=8.0,
            draft_m=round(rng.uniform(4.0, 18.0), 1),
            route_plan=_assign_route_plan(rng, behaviour, world),
        ))
    return fleet
