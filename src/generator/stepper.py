"""Stepper — the simulation tick loop.

Each tick:
  - advances every vessel along its current leg by speed_kn × dt
  - on leg completion, transitions to MOORED for the configured dwell
  - on dwell completion, picks the next destination per behaviour and
    fetches a fresh searoute leg
  - emits one PositionRecord per vessel per tick

The MVP intentionally keeps cadence flat at one emission per tick.
Behaviour-varying cadence (moored vs underway) can be layered on later.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Iterator, Literal
import random

from .fleet import Vessel
from .routing import Leg, RouteCache
from .world import Port, World


NavStatus = Literal["underway", "moored"]
PORT_DWELL_MIN_DEFAULT = 30.0  # sim-minutes


@dataclass
class PositionRecord:
    mmsi: int
    vessel_id: str
    event_ts: float        # epoch seconds (sim-time)
    lon: float
    lat: float
    sog: float             # knots
    cog: float             # degrees
    heading: float
    nav_status: NavStatus
    vessel_type: str
    vessel_name: str


@dataclass
class _VesselState:
    vessel: Vessel
    speed_kn: float
    leg: Leg
    plan_idx: int          # index into vessel.route_plan for the leg's *origin*
    arc_km: float
    nav_status: NavStatus
    dwell_remaining_min: float


@dataclass
class Stepper:
    world: World
    routes: RouteCache
    fleet: list[Vessel]
    rng: random.Random
    sim_start_epoch: float
    sim_speedup: float = 60.0
    tick_sim_minutes: float = 1.0
    port_dwell_min: float = PORT_DWELL_MIN_DEFAULT

    _state: list[_VesselState] = field(default_factory=list)
    _sim_time: float = 0.0    # epoch seconds (sim-time)

    def __post_init__(self) -> None:
        self._sim_time = self.sim_start_epoch
        for v in self.fleet:
            speed = self.rng.uniform(v.speed_min_kn, v.speed_max_kn)
            origin = self.world.ports[v.route_plan[0]]
            dest = self.world.ports[v.route_plan[1]]
            leg = self.routes.get((origin.lon, origin.lat), (dest.lon, dest.lat))
            # Stagger start positions across each vessel's first leg so the
            # fleet doesn't all leave the same port at t=0.
            arc_km = self.rng.uniform(0.0, leg.length_km)
            self._state.append(_VesselState(
                vessel=v, speed_kn=speed, leg=leg, plan_idx=0,
                arc_km=arc_km, nav_status="underway", dwell_remaining_min=0.0,
            ))

    def step(self) -> list[PositionRecord]:
        """Advance the simulation by one tick and return that tick's emissions."""
        records: list[PositionRecord] = []
        self._sim_time += self.tick_sim_minutes * 60.0
        dt_hr = self.tick_sim_minutes / 60.0

        for s in self._state:
            if s.nav_status == "moored":
                s.dwell_remaining_min -= self.tick_sim_minutes
                if s.dwell_remaining_min <= 0:
                    self._depart(s)
            else:
                s.arc_km += s.speed_kn * 1.852 * dt_hr  # knots × km/nm × hr
                if s.arc_km >= s.leg.length_km:
                    self._arrive(s)
            records.append(self._emit(s))
        return records

    def run(self, n_ticks: int) -> Iterator[list[PositionRecord]]:
        for _ in range(n_ticks):
            yield self.step()

    # --- behaviour transitions ----

    def _arrive(self, s: _VesselState) -> None:
        s.arc_km = s.leg.length_km
        s.nav_status = "moored"
        # Light variability in dwell so vessels don't all depart in lock-step.
        s.dwell_remaining_min = self.port_dwell_min * self.rng.uniform(0.5, 1.5)

    def _depart(self, s: _VesselState) -> None:
        next_origin = self.world.ports[s.vessel.route_plan[s.plan_idx + 1]]
        next_dest = self._pick_next_destination(s, next_origin)
        leg = self.routes.get((next_origin.lon, next_origin.lat), (next_dest.lon, next_dest.lat))
        s.leg = leg
        s.arc_km = 0.0
        s.plan_idx += 1
        s.nav_status = "underway"
        s.speed_kn = self.rng.uniform(s.vessel.speed_min_kn, s.vessel.speed_max_kn)
        # If route_plan exhausted, append the new destination so the cycle continues.
        if s.plan_idx + 1 >= len(s.vessel.route_plan):
            s.vessel.route_plan.append(next_dest.name)

    def _pick_next_destination(self, s: _VesselState, origin: Port) -> Port:
        """Behaviour-specific next-port choice."""
        b = s.vessel.behaviour
        if b == "liner":
            # Cycle: head to the next port on the plan, wrap to start when exhausted.
            nxt_idx = (s.plan_idx + 2) % len(s.vessel.route_plan)
            return self.world.ports[s.vessel.route_plan[nxt_idx]]
        if b == "ferry":
            # Shuttle: swap between the two ports.
            other = [n for n in s.vessel.route_plan if n != origin.name][0]
            return self.world.ports[other]
        # tramp + fishing — pick any port other than the current one
        candidates = [n for n in self.world.port_names if n != origin.name]
        return self.world.ports[self.rng.choice(candidates)]

    # --- emission ---

    def _emit(self, s: _VesselState) -> PositionRecord:
        if s.nav_status == "moored":
            origin = self.world.ports[s.vessel.route_plan[s.plan_idx + 1]]
            lon, lat, brg = origin.lon, origin.lat, 0.0
            sog = 0.0
        else:
            lon, lat, brg = s.leg.position_at(s.arc_km)
            sog = s.speed_kn
        return PositionRecord(
            mmsi=s.vessel.mmsi,
            vessel_id=s.vessel.vessel_id,
            event_ts=self._sim_time,
            lon=lon, lat=lat,
            sog=sog, cog=brg, heading=brg,
            nav_status=s.nav_status,
            vessel_type=s.vessel.type,
            vessel_name=s.vessel.name,
        )
