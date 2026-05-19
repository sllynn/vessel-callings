# Clarksons Vessel-Callings Demo — Project Brief

**Customer:** Clarksons Research (subsidiary of Clarksons Plc)
**ASQ:** AR-000117956 — Unblock New Workloads
**Owner (SSA):** Stuart Lynn
**Cloud:** Azure Databricks
**Status:** Drafting demo scope · 2026-05-19
**Source notes:** `~/Documents/obsidian/Clarksons Pl - AR-000117956 - Unblock New Workloads.md`

---

## 1. Purpose

Produce a runnable, parameterised, asset-bundle-packaged demo of the vessel-callings
architecture sketched in the AR-000117956 research brief and validated on the
13 May discovery call. The artefact has two audiences:

1. **Clarksons Research engineers** (Chris, Luke, Leander) — a working reference
   they can lift, adapt, and grow into the production implementation. They start
   from a low Databricks maturity baseline, so the demo doubles as enablement.
2. **The 02 June QBR audience** (sponsored by David Whicker) — a credibility
   exhibit that the recommended pattern (`h3_tessellateaswkb` + per-shape
   nominal resolution + virtual ancestor expansion + chip-refined
   `ST_Contains`) actually scales and produces correct callings under
   late-arrival and shape-mutation perturbations.

The demo is *not* a production pipeline. It is a scaffold against which
design decisions can be exercised cheaply and visibly.

## 2. Scope of the MVP

In-scope:

- End-to-end flow from synthetic AIS Bronze → indexed Silver → vessel-callings Gold.
- A `shape_cells` table built by `h3_tessellateaswkb` against three real
  shape sets covering a useful range of scales: 10 ocean basins / named seas,
  285 EEZs, and 157 UKHO IMO routeing measures (TSSs, ATBAs, precautionary
  areas, deep-water routes — UK-waters focused).
- The bitemporal callings MERGE (Option A in the research brief) on a 10-minute
  trigger, with a deliberate late-arrival window and a deliberate shape-mutation
  replay path.
- A **synthetic AIS generator** that produces realistic vessel tracks routed
  through navigable water, with knobs for sim-clock speed, lateness injection,
  GPS jitter, dropouts, and shape-mutation events.
- A Lakeview or Apps surface showing live callings on a map, so the demo is
  visually legible in the QBR.

Out of scope (for the MVP — flagged for follow-on):

- The full 150 K shapes / 200 K vessels / 10 M positions per hour load test.
  We prove the pattern on tens of shapes and thousands of vessels; we *cite*
  the scaling argument from the brief.
- Production identity-resolution / SCD-1 vessel-MMSI mapping (Sam's prior
  engagement). We assume clean `vessel_id` on the incoming feed.
- The declarative-pipeline / retraction variant (Option B). We keep it
  architecturally addressable but do not implement it in the MVP — adding
  it is a follow-on once the MERGE pattern is understood.
- Mosaic. The pattern is deliberately Mosaic-free (EoS Aug 2026).

## 3. Delivery shape

A single Databricks Asset Bundle, deployed to Azure Databricks. Notional layout:

```
clarksons-demo/
├── databricks.yml                       # bundle definition + targets (dev / qbr)
├── requirements.txt                     # top-level deps, pinned to latest
├── requirements.lock                    # uv pip compile output; cluster installs from this
├── resources/
│   ├── jobs/
│   │   ├── 00_bootstrap.job.yml         # catalogs/schemas/volume, shape ingest
│   │   ├── 10_shape_index.job.yml       # h3_tessellateaswkb → shape_cells
│   │   ├── 20_position_generate.job.yml # synthetic AIS generator
│   │   ├── 30_position_index.job.yml    # streaming Silver positions w/ h3_cell
│   │   ├── 40_callings_merge.job.yml    # 10-min Gold MERGE
│   │   └── 50_shape_mutation_replay.job.yml
│   ├── pipelines/                       # (placeholder for Option B follow-on)
│   ├── dashboards/
│   │   └── callings_lakeview.yml
│   └── apps/
│       └── callings_map.app.yml         # optional: deck.gl live map
├── notebooks/
│   ├── 00_setup/
│   ├── 10_shapes/
│   │   ├── load_shapes.py               # GEOMETRY(4326) ingest from parquet (WKB)
│   │   ├── pick_target_res.sql          # category- or area-driven
│   │   └── build_shape_cells.sql        # h3_tessellateaswkb, Liquid Cluster
│   ├── 20_positions/
│   │   ├── generate_ais.py              # widget-driven synthetic generator
│   │   └── index_positions.sql          # h3_longlatash3 @ finest target_res
│   ├── 30_callings/
│   │   ├── compute_callings.sql         # ancestor expansion + chip ST_Contains
│   │   └── merge_callings.sql           # idempotent MERGE, bitemporal cols
│   ├── 40_mutations/
│   │   └── replay_shape_change.py       # CDF-driven recompute window
│   └── 90_validation/
│       ├── correctness_oracle.sql       # full ST_Contains brute-force check
│       └── perf_microbench.sql
├── src/
│   ├── generator/                       # python: world, fleet, stepper
│   │   ├── world.py                     # ports manifest + searoute wrapper
│   │   ├── fleet.py                     # vessel profiles + behaviours
│   │   ├── stepper.py                   # tick loop, state machine
│   │   ├── perturb.py                   # lateness / jitter / dropout / mmsi-swap
│   │   ├── routing.py                   # searoute calls + leg cache
│   │   ├── ports.yaml                   # named ports (name, lon, lat)
│   │   └── fleet.yaml                   # vessel mix + behaviour assignment
│   └── shapes/                          # downloaded shapes (data/ symlink or copy)
└── tests/                               # pytest where it earns its keep
```

Every notebook takes its inputs via `dbutils.widgets`, so the same notebook
runs in dev, in CI, and from a job in the bundle. The asset bundle exposes
those widgets as job parameters in `databricks.yml`.

### Parameters (suggested widget set, top-level)

| Widget | Default (dev) | Used by |
|---|---|---|
| `catalog` | `clarksons_demo` | all |
| `schema` | `vessel_callings` | all |
| `volume` | `/Volumes/clarksons_demo/vessel_callings/landing` | replay, shapes |
| `target_res_strategy` | `category` (alt: `area`) | `pick_target_res.sql` |
| `position_res` | `10` | `index_positions.sql` (finest target_res in demo) |
| `seed` | `20260519` | `generate_ais.py` (deterministic output) |
| `n_vessels` | `2000` | `generate_ais.py` |
| `sim_speedup` | `60` (1 min real = 1 h sim) | `generate_ais.py` |
| `lateness_pct` | `5` | `generate_ais.py` |
| `lateness_max_hours` | `6` | `generate_ais.py`, MERGE |
| `gps_jitter_m` | `10` | `generate_ais.py` |
| `dropout_pct` | `0.5` | `generate_ais.py` |
| `merge_trigger_minutes` | `10` | `40_callings_merge.job.yml` |
| `watermark_hours` | `6` | streaming jobs |
| `bitemporal` | `true` | MERGE |

## 4. Data sources

### 4.1 AIS positions — synthetic generator

The Zenodo AIS dataset (`zenodo.org/records/8112336`) was considered and
rejected: it is large, awkward to download selectively, and confined to
Finnish waters — joining it against a global shape catalogue would tell
us very little of interest. Instead we build a synthetic generator that
authors realistic AIS-shaped traffic over the same geography we are
already indexing. This has the further pleasing effect of letting us
**guarantee** vessels call at the berths and ports we have drawn, rather
than leaving it to coincidence.

The generator has three layers:

**A. World** (built once, cached):

- **Named ports.** A small YAML manifest at `src/generator/ports.yaml`
  with ~5–10 named coastal points (Rotterdam, Felixstowe, Aberdeen,
  Hamburg, Lisbon, Gibraltar, New York, Suez-Med-side, Singapore) —
  each a name plus approximate `(lon, lat)`. This is the only manual
  authoring step in the build.
- **Routing via `searoute`.** Vessel-to-vessel routing uses the
  [`searoute`](https://github.com/eurostat/searoute) Python package
  (Eurostat, MIT-licensed, pure Python). Given any `(lon_a, lat_a)` →
  `(lon_b, lat_b)`, it returns a GeoJSON `LineString` of waypoints
  through its global maritime network — correctly handling choke
  points (Suez, Gibraltar, Panama, Dover). The generator calls
  `searoute.searoute(a, b)` whenever a vessel needs a new voyage and
  interpolates along the returned waypoints.
- **No land mask needed.** Searoute paths are by construction in
  navigable water. The Natural Earth land-veto polygon previously
  proposed becomes unnecessary — one whole subsystem of the generator
  falls away.
- **Anchored geography reused from §4.2.** The EEZ polygons do double
  duty: they are the shapes we index *and* they bound the random-walk
  regions for fishing vessels (Dogger Bank, North Sea, etc.) — the
  one behaviour archetype that doesn't use searoute, because fishing
  vessels meander rather than route between fixed ports.

**B. Fleet** (parameterised, deterministic):

- N vessels (default 2 000), each with `(mmsi, vessel_id, name, type,
  speed_min, speed_max, course_change_rate_max, draft)`. Mix of types —
  container, bulk, tanker, fishing, ferry — drawn deterministically from
  the `seed` widget.
- Each vessel is assigned a **behaviour**:
    - *Liner* — cyclic schedule across an ordered list of named ports;
      each leg routed by searoute when first reached, then cached for
      the duration of the run.
    - *Tramp* — picks next port stochastically, weighted by region;
      searoute generates the leg dynamically.
    - *Ferry* — shuttle between two near-coast ports (Dover–Calais,
      Felixstowe–Hoek-van-Holland); a single searoute leg used both
      ways.
    - *Fishing* — bounded random walk inside an EEZ polygon, with
      occasional returns to a home port (the home-port leg uses
      searoute; the meander does not). The one behaviour that
      doesn't lean on the routing graph.

**C. Stepper** (the loop):

- One simulated tick per sim-minute. Per vessel, a state machine governs
  behaviour:
    - `moored` — stationary with small GPS jitter; `nav_status = "moored"`;
      emits every 3 min.
    - `port_approach` — low speed, biased random walk toward the next
      berth or out-of-port waypoint; emits every 30 s.
    - `underway` — follow the active corridor at cruise speed; heading
      changes capped at `course_change_rate_max`; emits every 10–30 s
      depending on speed.
    - `fishing` — bounded random walk inside the assigned sea-area; emits
      every 60 s.
- **No land-mask veto needed** — searoute waypoints are in navigable
  water by construction. The stepper interpolates linearly between
  waypoints (great-circle for long legs), adds a small GPS jitter
  outside the kinematic loop, and never produces a land-piercing track.
- **Output schema** (AIS-shaped):

    | Column | Type | Notes |
    |---|---|---|
    | `mmsi` | BIGINT | 9-digit, deterministic per vessel |
    | `vessel_id` | STRING | Synthetic stable key — gives the Silver layer a clean PK without standing up Sam's identity-resolution work |
    | `event_ts` | TIMESTAMP | Sim-time at emission |
    | `ingest_ts` | TIMESTAMP | Wall-time at write (lateness applied here) |
    | `lon`, `lat` | DOUBLE | Post-jitter |
    | `sog`, `cog`, `heading` | DOUBLE | Speed/course over ground, heading |
    | `nav_status` | STRING | underway / moored / at_anchor / fishing |
    | `vessel_type`, `vessel_name` | STRING | For dashboards |

**Perturbations** are applied at write-time, outside the kinematics, so
they compose cleanly:

- `lateness_pct` × `lateness_max_hours` — fraction of records held back by
  a delay drawn from a truncated lognormal. The MERGE's late-window must
  catch them.
- `gps_jitter_m` — Gaussian noise added to lon/lat after the land-mask
  check (so jitter cannot push a moored vessel onto a pier in the wrong way).
- `dropout_pct` — fraction of records silently dropped, simulating the
  reality that AIS is patchy.
- Optional `mmsi_swap_pct` — a vessel briefly emits a neighbour's MMSI.
  Not exercised by default; available if a future demo wants to motivate
  the identity-resolution layer.
- **Shape-mutation events** are emitted by a separate small job
  (`50_shape_mutation_replay.job.yml`) on a scheduled sim-time — e.g.
  expanding a port boundary at T+45 min — to exercise the CDF-driven
  recompute path.

**Determinism.** A single `seed` widget threads through every random
draw (`numpy.random.default_rng(seed)`). Same seed in → byte-identical
output. This matters for the QBR: the demo must be runnable a second
time at the podium and produce the same map.

**Landing pattern.** The generator writes newline-delimited JSON to a UC
Volume; Auto Loader picks it up into Bronze. No in-memory shortcut from
generator to query — the pipeline must earn its passage from raw bytes.

### 4.2 Shapes — chosen sources (all already in `data/`)

After surveying the landscape we settled on three datasets that together
give a useful spread of scales without any hand-drawing. Ports and berths
are explicitly deferred — OSM `harbour=yes` polygons returned too many
false positives, the World Port Index is point-only, and the customer
already has their own port/berth shapes that they can drop in when we
are ready to integrate. The three demo datasets:

| Band | Source | File | Features | Licence | Target res |
|---|---|---|---|---|---|
| Ocean basins / named seas | Marine Regions Global Oceans & Seas v01 (simplified) | `data/marine_regions_global_oceans_seas_v01_simple.parquet` | **10** (S. Pacific, Indian, N. Pacific, S. Atlantic, N. Atlantic, Arctic, S. China & Easter Archipelagic, Southern, Mediterranean, Baltic) | CC-BY 4.0 | 2 |
| EEZs (global) | Marine Regions EEZ (simplified) | `data/marine_regions_global_eezs_simple.parquet` | **285** (median area ~138 000 km²) | CC-BY 4.0 | 5 |
| UKHO IMO routeing measures | UK Hydrographic Office, IMO-adopted measures in UK & adjacent waters | `data/UKHO_IMO_Routeing_Measures_Areas.parquet` (converted from the source shapefile) | **157** — 60 TSS lanes, 32 TSS zones, 21 inshore traffic zones, 20 ATBAs, 11 precautionary areas, 9 deep-water routes, 4 two-way routes | UKHO open data | 7–8, varied by feature type |

This gives 452 shapes total — comfortably small to author, comfortably
varied in scale, and rich in shape *complexity* (the IMO routeing
measures include long thin TSS lanes and large coast-aligned ATBAs that
exercise the boundary-chip refinement properly).

**Geographic gravity.** The ocean basins and EEZs are global, but the
IMO routeing data is UK-centric — Strait of Dover, Casquets, Isles of
Scilly, Shetland, the North Sea approaches. The QBR storyline therefore
naturally settles around UK and European waters. Generator route
corridors (§4.1) should follow: North Sea, English Channel, North
Atlantic transits, and Mediterranean approach via Gibraltar are the
obvious set. This is also a nice tonal fit — the customer is a London
firm with deep North-Sea / European-shipping context.

**Customer port/berth data — slot left open.** When Clarksons want to see
their own berth or port shapes flow through the same pipeline, the
`shapes_raw` table is a one-row-per-shape insert. The `target_res`
strategy widget already accommodates whichever resolution they pick. We
do not need to know the shapes themselves to design around them.

**Display-only file.** `data/Shipping_Lanes_v1.geojson` (NOAA / NCEI
global shipping lanes, 3 `MultiLineString` features classified
Major/Middle/Minor) is **not** ingested into `shape_cells` and is **not**
used by the generator — searoute supersedes both roles. It is kept in
`data/` purely to render as a faint translucent backdrop layer beneath
the live vessel tracks on the Lakeview map. The visual effect is
worthwhile for the QBR: vessels overlay neatly onto the real-world
aggregated traffic corridors, making the synthetic generator's output
look rooted in reality even to a maritime-fluent audience.

**Unified `shapes_raw` table.** Ingest all three sources into a single
Delta table with columns `(shape_id, source, category, name, geom
GEOMETRY(4326), source_attrs MAP<STRING,STRING>, valid_from, valid_to)`.
`category` drives `target_res` via a small lookup (and a `target_res`
column is then materialised on `shape_cells`). The IMO `feature_ty`
field is the natural category for that source; ocean basins and EEZs
each become a single category. Customer port/berth shapes can be added
later under their own categories without touching anything else.

**Notes for ingestion.** All three sources are standard geoparquet
(version 1.1.0, `geo` key-value metadata present), all WKB-encoded
MultiPolygon, coordinates in WGS84 lon/lat (EPSG:4326). The CRS field
is null in the metadata — which by geoparquet 1.1.0 means coordinates
are not declared — but both providers (Marine Regions, UKHO) publish
in WGS84 lon/lat by default, and the bounding boxes confirm it.

We store shapes as **`GEOMETRY(4326)`** rather than `GEOGRAPHY` — see
the design note below — so the ingest is a direct WKB decode with the
SRID tagged:

```sql
SELECT
  ...,
  ST_GeomFromWKB(geometry, 4326) AS geom
FROM <src>_raw;
```

The three reads unify into `shapes_raw` with a `source` column tagging
each row's origin and a `category` column for `target_res` lookup.

**Design note — `GEOMETRY(4326)` over `GEOGRAPHY`.** Although the
research brief recommends `GEOGRAPHY`, the demo (and the production
pattern it points toward) uses `GEOMETRY(4326)` throughout. The
reasoning:

- Photon's spatial-join optimisations (range-join hints, broadcast
  spatial joins) target `GEOMETRY` columns.
- `GEOMETRY` columns get bounding-box statistics collected at write
  time, which Delta uses for file-level data skipping on spatial
  predicates — material at billions-of-positions scale.
- Cartesian `ST_Contains` against the *clipped chip* is correct at
  the boundary scales we care about; the H3 indexing layer carries
  the spherical truth and the chip refinement runs at sub-cell
  resolution where planar / spheroidal differ negligibly.

Where ad-hoc geodesic distance or area is needed (e.g. validation
notebooks), cast to `GEOGRAPHY` at the call site rather than storing
the column that way.

**Antimeridian gotcha worth a glance.** Polygons that cross longitude
180° — parts of the South Pacific basin, for example — are
geodesically correct in `GEOGRAPHY` but, in `GEOMETRY(4326)`, must
already be split at the antimeridian in the source data. The
`shape_cells` build notebook will spot-check this on first run
against the ocean basins; if any basin reports a bounding box
spanning -180 to 180, we split before tessellation.

**Cluster-side dependencies stay minimal — but searoute is a legitimate
exception.** The general rule: local-only inspection tools (duckdb,
pyarrow, anything used to prepare files *before* they land in `data/`)
live in `.venv/` and never make it into the bundle's cluster library
list. The cluster runs Spark + native Spatial SQL and nothing more
exotic than that.

The one runtime exception is **searoute** in the generator's cluster
job: it solves a problem the platform does not (maritime-network
routing between arbitrary lon/lat pairs), it's pure-Python with a
small bundled graph, and it's called by code that genuinely runs on
the cluster. That's the principled distinction from the duckdb case —
duckdb would have been a dev tool encroaching on the cluster's
Spark-native turf; searoute is doing work no platform component does.

**Dependency management: `uv pip compile` + notebook install pattern.**
Top-level deps live in `requirements.txt`, pinned to latest at the
time they're added. Before each deployment we run
`uv pip compile requirements.txt -o requirements.lock` to produce a
fully-resolved lock file with all transitive dependencies (e.g.
`searoute` pulls in `geojson` and `networkx` — recorded in the lock,
not in `requirements.txt`). Notebooks install from the lock file via
a two-cell prelude:

```python
# Cell 1
%pip install uv
```

```python
# Cell 2
%sh uv pip install -r ../requirements.lock
```

This is faster than `%pip install -r ...` (uv resolves and installs
in parallel) and reproducible across cluster restarts. Crucially do
**not** pass `--system` — `%pip install uv` provisions a
notebook-scoped venv, and `--system` would bypass it, installing
into the OS Python where the notebook's interpreter cannot find the
packages.

## 5. Architectural sketch (lifted from the brief, narrowed to MVP)

```
              Zenodo AIS  ─────► Test Harness ─────► UC Volume (landing/)
                                                           │
                                                           ▼
                                                   Auto Loader / SDP
                                                           │
                                                   bronze_ais_positions
                                                           │
                                                           ▼
                                          + h3_cell (BIGINT, res = position_res)
                                                  Liquid Cluster (h3_cell, event_ts)
                                                   silver_positions
                                                           │
   shapes_raw (GEOMETRY(4326)) ─► h3_tessellateaswkb ──► shape_cells (shape_id, cell, target_res,
                                                                    core BOOL, chip_wkb BIN)
                                                Liquid Cluster (cell)
                                                           │
                          (10-min trigger) ◄────────────────┘
                                  │
                                  ▼
        ancestor-expand positions through h3_toparent at each distinct target_res,
        equi-join on (cell, target_res),
        short-circuit ST_Contains(chip, point) on core = FALSE rows
                                  │
                                  ▼
                  callings_gold (bitemporal: vessel_id, shape_id, entry_ts,
                                  exit_ts, valid_from, valid_to, as_of_ts)
                                  │
                                  ▼
                       Lakeview map + optional deck.gl app
```

The MERGE is keyed `(vessel_id, shape_id, entry_ts)`; `WHEN MATCHED UPDATE`
overwrites `exit_ts` and `as_of_ts`. Shape mutations trigger a CDF-driven
window re-emission. This is exactly the Option-A pattern from §4 of the
research brief — implemented small so it can be reasoned about, not
benchmarked.

## 6. What the demo lets us *show*

1. The chip pattern in motion — boundary cells producing `ST_Contains` calls,
   core cells short-circuiting. (Lakeview tile counting the two paths.)
2. Mixed nominal resolutions — a single vessel position generating callings
   against, say, the Dover TSS lane (res 8), the UK EEZ (res 5), and the
   North Atlantic Ocean (res 2) in one join. Visually: a tanker tracing
   the Dover Strait, three concentric polygons lighting up at once.
3. Late-arrival correctness — `lateness_pct` knob > 0; observe `as_of_ts`
   revisions on the callings row without primary-key duplication.
4. Shape-mutation replay — push a redrawn ATBA boundary at sim T; observe
   historical callings in the affected window re-emerge with a new `as_of_ts`
   and the prior version still queryable via `valid_to < as_of_ts`.
5. Resolution sensitivity — set `position_res` below the finest shape
   `target_res` and watch the IMO-scale callings disappear. The cheapest
   way to teach the team *why* the rule from §3 of the research brief is
   non-negotiable.

## 7. Open design decisions to settle before building

These are the questions whose answers shape the bundle; they map onto the
discovery questions in the research brief.

| # | Question | Why it matters for the demo |
|---|---|---|
| 1 | Bitemporal columns in MVP, or just `as_of_ts`? | The brief recommends bitemporal; if QBR scope is tight we may show `as_of_ts` only and flag `valid_from/valid_to` as the production extension. |
| 2 | Streaming MERGE via `foreachBatch`, or a triggered batch on a 10-min schedule? | Both deliver the same outcome at this scale; `foreachBatch` is more "production-shaped" and easier to extend to Option B. |
| 3 | Do we ship the deck.gl Apps surface, or stop at a Lakeview dashboard? | Apps demo better at the QBR; Lakeview is meaningfully faster to build and lower-risk. Recommend Lakeview first, Apps as stretch. |
| 4 | Should we subset the EEZs / IMO measures for the demo, or load all 452? | All 452 is small enough to load whole. Worth doing the inclusive thing so the customer can see their own EEZ light up. |
| 5 | Which named ports populate `ports.yaml`? | Working set: Rotterdam, Felixstowe, Aberdeen, Hamburg, Lisbon, Gibraltar, New York, Suez-Med-side, Singapore. 5–10 is plenty; searoute handles any pair. |
| 6 | Fleet size and runtime budget for the generator? | 2 000 vessels × 60× sim-speedup produces ~24 k records/min — comfortable on a single-node cluster. Worth a thought before we lean on it heavily. |
| 7 | Searoute on-cluster or as a prep step? | Recommend on-cluster — it's a pure-Python runtime dep (~few MB graph), the generator runs there anyway, and dynamic routing keeps the door open for the customer to extend the port list later. Distinct from the duckdb case: searoute is a runtime tool that solves a problem Spark doesn't, not a dev tool encroaching. |

## 8. Risks and watch-outs

- **Synthetic tracks risk looking "too clean".** Generated traffic without
  enough jitter, dropouts, and identity quirks may feel implausible to a
  maritime audience. Counter: the audience for the demo is engineers being
  taught a pattern, not maritime analysts being shown real-world data. We
  tune the perturbations until tracks are visually believable on the
  Lakeview map and stop there — the demo's job is to make the *architecture*
  convincing, not to be a substitute for production AIS.
- **`h3_tessellateaswkb` cell counts on ocean basins.** Even at res 3 a
  Pacific basin tessellates to tens of thousands of cells. Fine for storage;
  worth a one-shot sanity check before we let the bundle's bootstrap job
  run unattended.
- **`GEOMETRY(4326)` on DBR 18.3.** Spatial SQL is freshly GA this
  month; we should pin the cluster's DBR explicitly in the bundle and avoid
  cleverness around features that are still moving.
- **Apps surface adds a separate Azure provisioning path.** If we go to deck.gl,
  do it on day 2 of the build, not day 1.
- **Demo fidelity vs production fidelity.** The MVP deliberately stops short
  of production scale. We must be explicit about that gap whenever we show
  the demo to David — the scaling argument lives in the research brief, the
  demo proves the *pattern*.

## 9. Proposed next steps

1. **Confirm the demo scope** in §2 with you — particularly the "no Mosaic, no
   declarative pipeline, no full-scale load" boundaries.
2. **Confirm the named-port set** for the generator (working assumption
   in §7 #5): Rotterdam, Felixstowe, Aberdeen, Hamburg, Lisbon,
   Gibraltar, New York, Suez-Med-side, Singapore. Drafted as
   `src/generator/ports.yaml` — a few lines per port. The only
   hand-authoring step in the build.
3. **Pin `searoute`** in both the local `requirements.txt` (done) and
   the bundle's cluster libraries (to be added at scaffolding time).
   Shape data and land-veto are no longer needed — searoute paths are
   in-water by construction.
4. **Scaffold the bundle**: `databricks bundle init` from a default template,
   commit the layout in §3, get the bootstrap job deploying cleanly to a
   dev target in Azure Databricks.
5. **Iterate**: shapes → generator → positions → callings → mutations, each
   behind a widget-driven notebook and a bundle job, with a small validation
   notebook per layer. The generator earns its own validation notebook
   (track-plot, calls-per-port histogram) — if generated traffic doesn't
   look right, nothing downstream will.

---

*Drafted 2026-05-19 from the AR-000117956 working file. Update as decisions
land or as the discovery picture shifts.*
