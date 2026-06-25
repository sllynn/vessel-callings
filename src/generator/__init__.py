"""Synthetic AIS generator for the MarineIntel vessel-callings demo.

Layers (per BRIEF.md §4.1):
- world.py    — ports manifest + world geography (EEZs for fishing bounds)
- routing.py  — searoute wrapper with per-leg cache
- fleet.py    — vessel profiles + behaviour archetypes
- stepper.py  — tick loop, state machine, position emission
- perturb.py  — lateness / GPS jitter / dropout / MMSI swap
"""
