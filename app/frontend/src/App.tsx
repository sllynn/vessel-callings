import { useEffect, useMemo, useRef, useState } from "react";
import DeckGL from "@deck.gl/react";
import { IconLayer, GeoJsonLayer, PathLayer } from "@deck.gl/layers";
import { Map } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";

import {
  api,
  VesselPosition,
  OpenCalling,
  ShapeFeatureCollection,
  TrackPoint,
  CallingHistoryRow,
} from "./api";

const MAP_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

const INITIAL_VIEW = {
  longitude: -15,
  latitude: 45,
  zoom: 3,
  pitch: 0,
  bearing: 0,
};

// Categorical palette for vessel types — extend as needed.
const VESSEL_TYPE_COLOUR: Record<string, [number, number, number]> = {
  tanker:    [232, 109, 0],     // orange
  cargo:     [85, 158, 255],    // light blue
  fishing:   [88, 209, 128],    // green
  passenger: [217, 87, 161],    // pink
  service:   [180, 180, 180],   // grey
  unknown:   [140, 140, 140],
};

// Stable colours for the three shape sources.
const SOURCE_COLOUR: Record<string, [number, number, number]> = {
  marine_regions_oceans: [58, 163, 255],   // blue
  marine_regions_eez:    [60, 178, 75],    // green
  ukho_imo_routeing:     [230, 25, 75],    // red
};

const SOURCE_LABEL: Record<string, string> = {
  marine_regions_oceans: "Ocean basins",
  marine_regions_eez:    "EEZs",
  ukho_imo_routeing:     "IMO routeing",
};

// Inline white-on-transparent ship silhouette (vector). Pointed up so
// `getAngle = -cog` makes the bow face the vessel's course.
// Stylised top-down profile: hull + superstructure pointing forward.
const SHIP_ICON_URL =
  "data:image/svg+xml;utf8," +
  encodeURIComponent(
    `<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 100 100">
       <path d="M50 6 L72 35 L72 78 L62 92 L38 92 L28 78 L28 35 Z" fill="white"/>
     </svg>`
  );

const SHIP_ICON_MAPPING = {
  ship: { x: 0, y: 0, width: 100, height: 100, mask: true, anchorY: 50 },
};

function vesselColour(t?: string): [number, number, number] {
  return VESSEL_TYPE_COLOUR[t ?? "unknown"] ?? VESSEL_TYPE_COLOUR.unknown;
}

// Consolidate consecutive calling-history rows that share a shape. The
// upstream pipeline still produces the occasional spurious exit/entry
// pair when a vessel transits a boundary cell whose chip the position
// falls just outside of (residual planar/spheroidal mismatch even after
// the chip buffer), so adjacent rows for the same shape are folded into
// one logical visit:
//   • shape_id is the run key,
//   • entry_ts = min over the run,
//   • exit_ts  = max over the run,
//   • n_positions summed.
// Only closed callings (exit_ts != null) are considered — open callings
// live in the section above the table.
function consolidateHistory(rows: CallingHistoryRow[]): CallingHistoryRow[] {
  const closed = rows.filter((r) => r.exit_ts != null);
  closed.sort((a, b) => a.entry_ts.localeCompare(b.entry_ts));
  const merged: CallingHistoryRow[] = [];
  for (const r of closed) {
    const last = merged[merged.length - 1];
    if (last && last.shape_id === r.shape_id) {
      if (r.exit_ts && (!last.exit_ts || r.exit_ts > last.exit_ts)) last.exit_ts = r.exit_ts;
      last.n_positions += r.n_positions;
    } else {
      merged.push({ ...r });
    }
  }
  merged.sort((a, b) => (b.exit_ts ?? "").localeCompare(a.exit_ts ?? ""));
  return merged;
}

// Compact "MM-DD HH:MM" formatter for the table.
function fmtTs(ts: string | null): string {
  if (!ts) return "—";
  // ISO "2026-06-23T15:14:30..." → "06-23 15:14"
  return `${ts.slice(5, 10)} ${ts.slice(11, 16)}`;
}

export default function App() {
  const [positions, setPositions]   = useState<VesselPosition[]>([]);
  const [openCalls, setOpenCalls]   = useState<OpenCalling[]>([]);
  const [shapesFC,  setShapesFC]    = useState<ShapeFeatureCollection | null>(null);
  const [error,     setError]       = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null);
  const [activeSources, setActiveSources] = useState<Set<string>>(
    new Set(["marine_regions_eez", "ukho_imo_routeing"]),
  );
  const [selectedVessel, setSelectedVessel] = useState<VesselPosition | null>(null);
  const [track,   setTrack]   = useState<TrackPoint[] | null>(null);
  const [history, setHistory] = useState<CallingHistoryRow[] | null>(null);
  const refreshRef = useRef<number | null>(null);

  // ── Initial load: shapes (cached forever), and a first positions/callings fetch.
  useEffect(() => {
    api.shapes().then(setShapesFC).catch((e) => setError(String(e)));
  }, []);

  // ── Polling loop for live data.
  useEffect(() => {
    let cancelled = false;

    const tick = async () => {
      try {
        const [pos, openC] = await Promise.all([api.latest(), api.open()]);
        if (cancelled) return;
        setPositions(pos);
        setOpenCalls(openC);
        setLastRefresh(new Date());
        setError(null);
      } catch (e) {
        if (!cancelled) setError(String(e));
      }
    };

    tick();
    refreshRef.current = window.setInterval(tick, 5000);
    return () => {
      cancelled = true;
      if (refreshRef.current) window.clearInterval(refreshRef.current);
    };
  }, []);

  // ── Selected vessel detail (track + calling history).
  useEffect(() => {
    if (!selectedVessel) {
      setTrack(null);
      setHistory(null);
      return;
    }
    let cancelled = false;
    Promise.all([
      api.track(selectedVessel.vessel_id, 2000),
      api.history(selectedVessel.vessel_id, 500),
    ])
      .then(([t, h]) => {
        if (cancelled) return;
        setTrack(t);
        setHistory(h);
      })
      .catch((e) => !cancelled && setError(String(e)));
    return () => { cancelled = true; };
  }, [selectedVessel]);

  // ── Derived: which vessels have at least one open calling right now.
  const openVesselIds = useMemo(
    () => new Set(openCalls.map((c) => c.vessel_id)),
    [openCalls],
  );

  // ── Layers.
  const layers = useMemo(() => {
    const filteredFeatures = shapesFC
      ? shapesFC.features.filter((f) => activeSources.has(f.properties.source))
      : [];

    return [
      // Shape outlines — one layer for all active sources, styled by source.
      new GeoJsonLayer({
        id: "shapes",
        data: { type: "FeatureCollection", features: filteredFeatures },
        stroked: true,
        filled: true,
        pickable: true,
        getLineColor: (f: any) => [
          ...(SOURCE_COLOUR[f.properties.source] ?? [200, 200, 200]),
          200,
        ],
        getFillColor: (f: any) => [
          ...(SOURCE_COLOUR[f.properties.source] ?? [200, 200, 200]),
          18,
        ],
        getLineWidth: 1.2,
        lineWidthMinPixels: 0.8,
      }),

      // Selected vessel's recent track.
      track && track.length > 1
        ? new PathLayer({
            id: "track",
            data: [{ path: track.map((p) => [p.lon, p.lat]) }],
            getPath: (d: any) => d.path,
            getColor: [255, 255, 255, 200],
            getWidth: 2,
            widthMinPixels: 1.5,
          })
        : null,

      // Vessels — IconLayer with a rotated ship silhouette. Colour is
      // applied via the icon's `mask: true` channel so each ship can
      // be tinted by vessel_type; openVesselIds gets a larger size.
      new IconLayer({
        id: "vessels",
        data: positions,
        pickable: true,
        iconAtlas: SHIP_ICON_URL,
        iconMapping: SHIP_ICON_MAPPING,
        sizeUnits: "pixels",
        getIcon: () => "ship",
        getPosition: (d: VesselPosition) => [d.lon, d.lat],
        getSize: (d: VesselPosition) =>
          openVesselIds.has(d.vessel_id) ? 22 : 16,
        getColor: (d: VesselPosition) => [...vesselColour(d.vessel_type), 235],
        // deck.gl IconLayer: 0° = up, positive = counter-clockwise.
        // COG is clockwise from north, so negate to align the bow.
        getAngle: (d: VesselPosition) => -d.cog,
        updateTriggers: {
          getSize: [openVesselIds],
        },
        onClick: (info: any) => {
          if (info.object) setSelectedVessel(info.object as VesselPosition);
        },
      }),
    ].filter(Boolean) as any;
  }, [positions, shapesFC, activeSources, track, openVesselIds]);

  const vesselTooltip = (info: any) => {
    const object = info?.object;
    if (!object) return null;
    if (object.vessel_id) {
      return {
        html: `
          <div style="font-family: ui-sans-serif, system-ui">
            <div style="font-weight: 600">${object.vessel_name} <span style="color:#94a3b8">(${object.vessel_id})</span></div>
            <div style="color:#94a3b8">${object.vessel_type ?? ""}</div>
            <div style="margin-top:4px">${object.sog?.toFixed(1) ?? "0.0"} kn · ${object.cog?.toFixed(0) ?? "0"}°</div>
          </div>
        `,
        style: { background: "#0f172a", color: "#e2e8f0", padding: "8px 10px", borderRadius: "6px", border: "1px solid #1e293b" },
      };
    }
    if (object.properties?.name) {
      return {
        html: `<div style="font-family: ui-sans-serif, system-ui"><div style="font-weight:600">${object.properties.name}</div><div style="color:#94a3b8">${object.properties.category}</div></div>`,
        style: { background: "#0f172a", color: "#e2e8f0", padding: "8px 10px", borderRadius: "6px", border: "1px solid #1e293b" },
      };
    }
    return null;
  };

  const toggleSource = (s: string) =>
    setActiveSources((prev) => {
      const next = new Set(prev);
      next.has(s) ? next.delete(s) : next.add(s);
      return next;
    });

  return (
    <div className="relative h-full w-full">
      <DeckGL
        initialViewState={INITIAL_VIEW}
        controller
        layers={layers}
        getTooltip={vesselTooltip}
      >
        <Map reuseMaps mapStyle={MAP_STYLE} />
      </DeckGL>

      {/* Header */}
      <div className="absolute top-0 left-0 right-0 px-6 py-4 pointer-events-none flex items-center justify-between">
        <div className="pointer-events-auto bg-slate-900/80 backdrop-blur rounded-lg px-4 py-2 border border-slate-700">
          <div className="text-base font-semibold tracking-tight">Vessel callings</div>
          <div className="text-xs text-slate-400 mt-0.5">
            {positions.length} vessels ·{" "}
            {openCalls.length} open callings ·{" "}
            {lastRefresh ? `refreshed ${lastRefresh.toLocaleTimeString()}` : "loading…"}
          </div>
        </div>

        {/* Layer toggle */}
        <div className="pointer-events-auto bg-slate-900/80 backdrop-blur rounded-lg px-4 py-3 border border-slate-700 text-xs">
          <div className="text-slate-400 uppercase tracking-wide text-[10px] mb-2">Shape layers</div>
          {Object.entries(SOURCE_LABEL).map(([k, label]) => (
            <label key={k} className="flex items-center gap-2 py-1 cursor-pointer select-none">
              <input
                type="checkbox"
                className="accent-slate-300"
                checked={activeSources.has(k)}
                onChange={() => toggleSource(k)}
              />
              <span
                className="inline-block w-2 h-2 rounded-full"
                style={{
                  background: `rgb(${SOURCE_COLOUR[k].join(",")})`,
                }}
              />
              {label}
            </label>
          ))}
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="absolute bottom-4 left-4 max-w-md bg-red-900/90 border border-red-700 text-red-100 text-xs rounded-lg px-4 py-2">
          {error}
        </div>
      )}

      {/* Selected-vessel side panel */}
      {selectedVessel && (
        <div className="absolute top-0 right-0 h-full w-96 bg-slate-900/95 backdrop-blur border-l border-slate-700 p-5 overflow-auto pointer-events-auto">
          <div className="flex items-start justify-between gap-2">
            <div>
              <div className="text-lg font-semibold leading-tight">{selectedVessel.vessel_name}</div>
              <div className="text-xs text-slate-400 mt-0.5">
                {selectedVessel.vessel_id} · {selectedVessel.vessel_type} · MMSI {selectedVessel.mmsi}
              </div>
            </div>
            <button
              className="text-slate-400 hover:text-slate-100 text-lg leading-none px-1"
              onClick={() => setSelectedVessel(null)}
              aria-label="Close"
            >×</button>
          </div>

          <div className="grid grid-cols-3 gap-2 mt-4 text-xs">
            <Stat label="lat"  value={selectedVessel.lat.toFixed(3)} />
            <Stat label="lon"  value={selectedVessel.lon.toFixed(3)} />
            <Stat label="sog"  value={`${selectedVessel.sog.toFixed(1)} kn`} />
            <Stat label="cog"  value={`${selectedVessel.cog.toFixed(0)}°`} />
            <Stat label="nav"  value={selectedVessel.nav_status} />
            <Stat label="ts"   value={selectedVessel.event_ts.slice(11, 19)} />
          </div>

          <div className="mt-5">
            <div className="text-[10px] uppercase tracking-wide text-slate-400 mb-2">
              Open callings
            </div>
            {openCalls.filter((c) => c.vessel_id === selectedVessel.vessel_id).length === 0 ? (
              <div className="text-xs text-slate-500 italic">No open callings.</div>
            ) : (
              <ul className="space-y-2">
                {openCalls
                  .filter((c) => c.vessel_id === selectedVessel.vessel_id)
                  .map((c) => (
                    <li key={c.shape_id} className="text-xs bg-slate-800/60 rounded px-3 py-2">
                      <div className="flex items-center gap-2">
                        <span
                          className="inline-block w-2 h-2 rounded-full"
                          style={{ background: `rgb(${(SOURCE_COLOUR[c.source] ?? [200,200,200]).join(",")})` }}
                        />
                        <span className="font-medium">{c.shape_name}</span>
                      </div>
                      <div className="text-slate-400 mt-0.5">
                        entered {c.entry_ts.slice(0, 19)} · {c.n_positions} positions
                      </div>
                    </li>
                  ))}
              </ul>
            )}
          </div>

          <div className="mt-5">
            <div className="text-[10px] uppercase tracking-wide text-slate-400 mb-2">
              Recent calling history
            </div>
            {!history ? (
              <div className="text-xs text-slate-500 italic">Loading…</div>
            ) : (() => {
              const rows = consolidateHistory(history);
              if (rows.length === 0) {
                return <div className="text-xs text-slate-500 italic">No closed callings yet.</div>;
              }
              return (
                <div className="max-h-80 overflow-auto pr-1">
                  <table className="w-full text-[11px]">
                    <thead className="sticky top-0 bg-slate-900/95 backdrop-blur">
                      <tr className="text-slate-400 text-left">
                        <th className="font-medium pb-1.5 pr-2">Location</th>
                        <th className="font-medium pb-1.5 pr-2">Entry</th>
                        <th className="font-medium pb-1.5">Exit</th>
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((h, i) => (
                        <tr key={i} className="border-t border-slate-800/60">
                          <td className="py-1 pr-2">
                            <span
                              className="inline-block w-1.5 h-1.5 rounded-full mr-1.5 align-middle shrink-0"
                              style={{ background: `rgb(${(SOURCE_COLOUR[h.source] ?? [200,200,200]).join(",")})` }}
                            />
                            <span className="align-middle">{h.shape_name}</span>
                          </td>
                          <td className="py-1 pr-2 text-slate-400 tabular-nums whitespace-nowrap">{fmtTs(h.entry_ts)}</td>
                          <td className="py-1 text-slate-400 tabular-nums whitespace-nowrap">{fmtTs(h.exit_ts)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })()}
          </div>

          {track && track.length > 1 && (
            <div className="mt-5 text-[10px] uppercase tracking-wide text-slate-400">
              Track shown on map ({track.length} positions)
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-slate-800/60 rounded px-2 py-1.5">
      <div className="text-[9px] uppercase text-slate-500 tracking-wide">{label}</div>
      <div className="font-mono text-sm">{value}</div>
    </div>
  );
}
