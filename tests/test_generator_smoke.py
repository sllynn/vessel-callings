"""Smoke test for the generator — runs 10 ticks with 20 vessels and prints a few records.

Run from project root: .venv/bin/python -m tests.test_generator_smoke
"""
import random
import time

from src.generator.fleet import build_fleet
from src.generator.perturb import Perturber
from src.generator.routing import RouteCache
from src.generator.stepper import Stepper
from src.generator.world import World


def main() -> None:
    seed = 20260519
    world = World.load("src/generator/ports.yaml")
    routes = RouteCache()
    fleet = build_fleet(seed=seed, n=20, world=world)
    rng = random.Random(seed + 1)
    perturb = Perturber(rng=rng, lateness_pct=5, gps_jitter_m=10, dropout_pct=0.5)
    stepper = Stepper(
        world=world, routes=routes, fleet=fleet, rng=random.Random(seed + 2),
        sim_start_epoch=time.time(), sim_speedup=60.0,
    )

    print(f"world: {len(world.ports)} ports")
    print(f"fleet: {len(fleet)} vessels — behaviour breakdown:")
    by_b = {}
    for v in fleet:
        by_b[v.behaviour] = by_b.get(v.behaviour, 0) + 1
    for b, n in sorted(by_b.items()):
        print(f"  {b:10s} {n}")

    print("\n--- first 5 records from tick 0 ---")
    for tick_idx in range(10):
        records = stepper.step()
        perturbed = perturb.apply(records)
        if tick_idx == 0:
            for p in perturbed[:5]:
                r = p.record
                print(f"  {r.vessel_id} {r.vessel_type:9s} {r.nav_status:8s} "
                      f"({r.lon:7.2f},{r.lat:7.2f}) sog={r.sog:5.1f} cog={r.cog:5.1f} "
                      f"late_s={p.ingest_offset_s:6.1f}")
        if tick_idx == 9:
            print(f"\n--- tick {tick_idx}: {len(records)} clean, {len(perturbed)} post-perturb ---")
    print(f"\nroute cache populated with {len(routes._cache)} legs")


if __name__ == "__main__":
    main()
