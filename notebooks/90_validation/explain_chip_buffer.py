# Databricks notebook source
# MAGIC %md
# MAGIC # 90 / explain_chip_buffer
# MAGIC
# MAGIC Worked example of the planar-vs-spheroidal mismatch that motivates
# MAGIC the outward buffer applied to boundary chips in `build_shape_cells`.
# MAGIC
# MAGIC ## The issue in one sentence
# MAGIC
# MAGIC `h3_longlatash3` assigns a point to a cell using spheroidal
# MAGIC geometry, while `ST_Contains` evaluates the chip's WKB planarly —
# MAGIC joining the chip's lon/lat vertices with straight lines rather than
# MAGIC great-circle arcs. The two definitions of "where the cell ends"
# MAGIC diverge by a small but non-zero distance, leaving a thin strip
# MAGIC where a point is *in* the cell spheroidally but *outside* its chip
# MAGIC planarly. A small outward buffer on each boundary chip closes the
# MAGIC gap.
# MAGIC
# MAGIC This notebook produces the diagram and a worked numerical example
# MAGIC at a chosen H3 resolution and centre lat/lon.

# COMMAND ----------

# MAGIC %pip install uv

# COMMAND ----------

# MAGIC %sh uv pip install -r ../../requirements.lock

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import h3
import numpy as np
import matplotlib.pyplot as plt
from shapely.geometry import Polygon

dbutils.widgets.text("resolution", "2")
dbutils.widgets.text("centre_lat", "40.0")
dbutils.widgets.text("centre_lon", "-25.0")
dbutils.widgets.text("buffer_deg", "0.01")

resolution = int(dbutils.widgets.get("resolution"))
centre_lat = float(dbutils.widgets.get("centre_lat"))
centre_lon = float(dbutils.widgets.get("centre_lon"))
buffer_deg = float(dbutils.widgets.get("buffer_deg"))

print(f"resolution : {resolution}")
print(f"centre     : ({centre_lat}, {centre_lon})")
print(f"buffer_deg : {buffer_deg}  (~{buffer_deg * 111_000:.0f} m near the equator)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — great-circle arc sampling
# MAGIC
# MAGIC Each H3 cell edge is a geodesic segment on the sphere. Projected to
# MAGIC lon/lat space, it is a curve. We sample the curve at 30 points per
# MAGIC edge using spherical linear interpolation (slerp) in 3D Cartesian
# MAGIC coordinates — no extra dependencies needed.

# COMMAND ----------

R_EARTH_M = 6_371_000


def lonlat_to_xyz(lon, lat):
    lon_r, lat_r = np.radians(lon), np.radians(lat)
    return np.array([np.cos(lat_r) * np.cos(lon_r),
                     np.cos(lat_r) * np.sin(lon_r),
                     np.sin(lat_r)])


def xyz_to_lonlat(v):
    x, y, z = v
    return (float(np.degrees(np.arctan2(y, x))),
            float(np.degrees(np.arctan2(z, np.sqrt(x*x + y*y)))))


def great_circle_arc(p1, p2, n=30):
    """Sample n points along the great-circle arc from p1 to p2 (lon, lat)."""
    v1, v2 = lonlat_to_xyz(*p1), lonlat_to_xyz(*p2)
    omega = float(np.arccos(np.clip(v1 @ v2, -1.0, 1.0)))
    if omega < 1e-12:
        return [p1, p2]
    ts = np.linspace(0, 1, n)
    out = []
    for t in ts:
        a = np.sin((1 - t) * omega) / np.sin(omega)
        b = np.sin(t * omega) / np.sin(omega)
        out.append(xyz_to_lonlat(a * v1 + b * v2))
    return out


def densify_ring(verts, n=30):
    out = []
    for p1, p2 in zip(verts, verts[1:]):
        seg = great_circle_arc(p1, p2, n=n)
        out.extend(seg if not out else seg[1:])
    return out


def haversine_m(p1, p2):
    lon1, lat1 = np.radians(p1)
    lon2, lat2 = np.radians(p2)
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    return float(2 * R_EARTH_M * np.arcsin(np.sqrt(a)))


def cell_to_chord_verts(cell):
    """H3 cell vertices as (lon, lat), ring closed."""
    verts = [(lon, lat) for lat, lon in h3.cell_to_boundary(cell)]
    verts.append(verts[0])
    return verts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pick a pair of adjacent cells
# MAGIC
# MAGIC Cell A is the one containing the chosen lat/lon. From A's six
# MAGIC neighbours we pick the one whose shared edge produces the largest
# MAGIC chord-vs-arc strip — visually the clearest pair for the diagram.

# COMMAND ----------

cell_a = h3.latlng_to_cell(centre_lat, centre_lon, resolution)
verts_a      = cell_to_chord_verts(cell_a)
arc_a        = densify_ring(verts_a)
planar_a     = Polygon(verts_a)
spheroidal_a = Polygon(arc_a)
strip_a      = spheroidal_a.symmetric_difference(planar_a)   # rim around cell A

best_b, best_overlap = None, -1.0
for cand in h3.grid_disk(cell_a, 1) - {cell_a}:
    arc_c    = densify_ring(cell_to_chord_verts(cand))
    strip_c  = Polygon(arc_c).symmetric_difference(Polygon(cell_to_chord_verts(cand)))
    overlap  = strip_a.intersection(strip_c).area
    if overlap > best_overlap:
        best_overlap, best_b = overlap, cand

cell_b       = best_b
verts_b      = cell_to_chord_verts(cell_b)
arc_b        = densify_ring(verts_b)
planar_b     = Polygon(verts_b)
spheroidal_b = Polygon(arc_b)

print(f"cell A : {cell_a}")
print(f"cell B : {cell_b}  (chosen — largest disputed strip)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Numerical — chord-vs-arc deviation
# MAGIC
# MAGIC For an edge of length `L` on a sphere of radius `R`, the maximum
# MAGIC chord-to-arc deviation at the segment midpoint is
# MAGIC
# MAGIC     δ ≈ L² / (8 R)
# MAGIC
# MAGIC and the buffer we apply must comfortably exceed δ.

# COMMAND ----------

edge_length_m = haversine_m(verts_a[0], verts_a[1])
delta_m       = edge_length_m ** 2 / (8 * R_EARTH_M)
buffer_m_eq   = buffer_deg * 111_000   # rough — exact value varies with latitude

print(f"H3 res-{resolution} edge length   : {edge_length_m/1000:8.1f} km")
print(f"chord-arc max deviation δ      : {delta_m:8.1f} m   (= L²/8R)")
print(f"applied buffer                 : {buffer_deg}°  (~{buffer_m_eq:.0f} m)")
print(f"safety margin                  : {buffer_m_eq / delta_m:.1f}× δ")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagram — overview and zoom
# MAGIC
# MAGIC Solid lines are the planar chord boundaries (`ST_Contains` view);
# MAGIC dashed lines are the spheroidal arc boundaries (`h3_longlatash3`
# MAGIC view). The grey strip is the difference between them — the cell-A
# MAGIC rim where the arc bulges outside the chord.

# COMMAND ----------

def ring_xy(poly):
    return poly.exterior.xy


def fill_strip(ax, strip, **kw):
    if strip.is_empty:
        return
    geoms = strip.geoms if hasattr(strip, "geoms") else [strip]
    for g in geoms:
        if hasattr(g, "exterior"):
            ax.fill(*g.exterior.xy, **kw)


fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

# --- ax1: overview, chords + arcs together ---
ax1.plot(*ring_xy(planar_a),     color="tab:blue",   lw=1.8, label="planar (chord) — cell A")
ax1.plot(*ring_xy(planar_b),     color="tab:orange", lw=1.8, label="planar (chord) — cell B")
ax1.plot(*ring_xy(spheroidal_a), color="tab:blue",   lw=1.0, ls="--", alpha=0.7, label="spheroidal (arc) — cell A")
ax1.plot(*ring_xy(spheroidal_b), color="tab:orange", lw=1.0, ls="--", alpha=0.7, label="spheroidal (arc) — cell B")
fill_strip(ax1, strip_a, color="grey", alpha=0.35)
ax1.set_aspect("equal")
ax1.grid(alpha=0.3)
ax1.set_title("Two adjacent H3 cells — overview")
ax1.legend(loc="best", fontsize=8)
ax1.set_xlabel("longitude °"); ax1.set_ylabel("latitude °")

# --- ax2: extreme zoom on shared edge midpoint, arcs only ---
# Re-sample much more densely so the gentle curvature is visible across a
# tiny window.
arc_a_dense = densify_ring(verts_a, n=600)
arc_b_dense = densify_ring(verts_b, n=600)
spheroidal_a_dense = Polygon(arc_a_dense)
spheroidal_b_dense = Polygon(arc_b_dense)
strip_a_dense      = spheroidal_a_dense.symmetric_difference(planar_a)

# Zoom calibrated against the actual strip width: aim for the strip to
# occupy ~10% of the panel. Strip width in degrees ≈ δ / 111000.
strip_deg = delta_m / 111_000
span      = strip_deg * 5     # panel half-width — strip ~10% of total width
shared_edge = planar_a.intersection(planar_b)
if hasattr(shared_edge, "interpolate") and shared_edge.length > 0:
    mid = shared_edge.interpolate(0.5, normalized=True)
    ax2.set_xlim(mid.x - span, mid.x + span)
    ax2.set_ylim(mid.y - span, mid.y + span)

# Plot only the arcs — both cells share the same geodesic on this edge,
# so we get one curve, drawn with alternating blue/orange dashes for
# emphasis.
ax2.plot(*ring_xy(spheroidal_a_dense), color="tab:blue",   lw=2.2,
         label="spheroidal (arc) — cell A")
ax2.plot(*ring_xy(spheroidal_b_dense), color="tab:orange", lw=2.2, ls=(0, (6, 4)),
         alpha=0.9, label="spheroidal (arc) — cell B")

# Strip fill — make it pop. Red so it can't be missed.
fill_strip(ax2, strip_a_dense, color="tab:red", alpha=0.45, edgecolor="darkred", linewidth=0.8)

# Annotate the strip width so the customer sees the magnitude.
ax2.annotate(
    f"strip ≈ {delta_m:.0f} m wide\n(= L² / 8R, undrawn chord at inner edge)",
    xy=(mid.x, mid.y), xytext=(0.55, 0.15), textcoords="axes fraction",
    fontsize=10, ha="left",
    arrowprops=dict(arrowstyle="->", color="darkred", lw=1.2),
    bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="darkred"),
)

import matplotlib.patches as mpatches
handles, labels = ax2.get_legend_handles_labels()
handles.append(mpatches.Patch(facecolor="tab:red", alpha=0.45, edgecolor="darkred"))
labels.append("strip — arc minus chord")
ax2.legend(handles, labels, loc="upper left", fontsize=8)
ax2.set_aspect("equal")
ax2.grid(alpha=0.3)
ax2.set_title(f"Extreme zoom (panel ~{2 * span:.3f}° wide) — arcs only.\n"
              f"Red band is the strip where spheroidal cell extends past the chord.")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## A test point in the strip — what each indexer says

# COMMAND ----------

if strip_a.is_empty:
    print("strip is empty at this configuration — try a coarser resolution / different centre")
else:
    strip_geom = strip_a if hasattr(strip_a, "exterior") else max(strip_a.geoms, key=lambda g: g.area)
    test_pt = strip_geom.representative_point()
    indexed_cell = h3.latlng_to_cell(test_pt.y, test_pt.x, resolution)
    buffered_a   = planar_a.buffer(buffer_deg)
    print(f"test point (lon, lat)              : ({test_pt.x:.6f}, {test_pt.y:.6f})")
    print(f"h3.latlng_to_cell  →  cell index   : {indexed_cell}")
    print(f"                      == cell A ?  : {indexed_cell == cell_a}")
    print(f"                      == cell B ?  : {indexed_cell == cell_b}")
    print(f"planar   ST_Contains(chip_A, pt)   : {planar_a.contains(test_pt)}")
    print(f"planar   ST_Contains(chip_B, pt)   : {planar_b.contains(test_pt)}")
    print(f"buffered ST_Contains(chip_A, pt)   : {buffered_a.contains(test_pt)}   ← the fix")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagram — with the buffer applied
# MAGIC
# MAGIC The buffered planar chip (green fill) now covers the strip,
# MAGIC restoring continuity between the spheroidal cell membership and
# MAGIC the planar containment check.

# COMMAND ----------

fig2, ax = plt.subplots(figsize=(11, 11))

ax.fill(*ring_xy(buffered_a),     color="tab:green",  alpha=0.18,
        label=f"buffered planar (+{buffer_deg}°) — cell A")
ax.plot(*ring_xy(buffered_a),     color="tab:green",  lw=1.2, ls=":")
ax.plot(*ring_xy(planar_a),       color="tab:blue",   lw=2.2, label="planar (chord) — cell A")
ax.plot(*ring_xy(spheroidal_a),   color="tab:blue",   lw=1.2, ls="--", alpha=0.7, label="spheroidal (arc) — cell A")
ax.plot(*ring_xy(planar_b),       color="tab:orange", lw=1.5, alpha=0.6, label="planar (chord) — cell B")
fill_strip(ax, strip_a, color="grey", alpha=0.30)

if not strip_a.is_empty:
    sg = strip_a if hasattr(strip_a, "exterior") else max(strip_a.geoms, key=lambda g: g.area)
    tp = sg.representative_point()
    in_a = h3.latlng_to_cell(tp.y, tp.x, resolution) == cell_a
    ax.scatter([tp.x], [tp.y], color="red", s=110, zorder=10, edgecolor="black",
               label=f"test point — h3 says cell {'A' if in_a else 'B'}, planar A: {planar_a.contains(tp)}, buffered A: {buffered_a.contains(tp)}")

ax.set_aspect("equal")
ax.grid(alpha=0.3)
ax.legend(loc="best", fontsize=8)
ax.set_title(f"With a {buffer_deg}° outward buffer the planar chip covers the strip")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC * `h3.latlng_to_cell` / `h3_longlatash3` use **spheroidal** geometry — the
# MAGIC   cell boundary is a great-circle arc between H3 corner vertices.
# MAGIC * `ST_Contains` evaluates the chip's WKB **planarly** — the cell
# MAGIC   boundary becomes a straight chord between the same vertices.
# MAGIC * The chord lies inside the arc; the strip between them is up to
# MAGIC   `δ ≈ L² / 8R` wide at the edge midpoint (~490 m for res-2 cells,
# MAGIC   ~1.6 m for res-5, sub-cm for res-7+).
# MAGIC * Points falling in the strip are indexed to one cell by the spheroidal
# MAGIC   join but excluded by that cell's planar chip — silently filtered.
# MAGIC * A small outward buffer on each boundary chip (1/10 of the polygon
# MAGIC   simplification tolerance — ~1.1 km for ocean basins at res 2,
# MAGIC   ~110 m for EEZs at res 5) extends the planar chip past the chord
# MAGIC   and into the strip, restoring continuity. Adjacent chips slightly
# MAGIC   overlap in the strip region; `h3_longlatash3` deterministically
# MAGIC   picks one cell per point, so the overlap is harmless.
# MAGIC * The fix is meaningful only at coarse resolutions (res 2). At
# MAGIC   res 5 and finer the deviation is below vessel-positioning
# MAGIC   precision and the buffer is essentially cosmetic.
