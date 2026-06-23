export interface VesselPosition {
  vessel_id: string;
  vessel_name: string;
  vessel_type: string;
  mmsi: number;
  event_ts: string;
  lon: number;
  lat: number;
  sog: number;
  cog: number;
  nav_status: string;
}

export interface TrackPoint {
  event_ts: string;
  lon: number;
  lat: number;
  sog: number;
  cog: number;
}

export interface OpenCalling {
  vessel_id: string;
  shape_id: number;
  entry_ts: string;
  last_seen_ts: string;
  n_positions: number;
  as_of_ts: string;
  shape_name: string;
  source: string;
  category: string;
}

export interface CallingHistoryRow {
  shape_id: number;
  shape_name: string;
  source: string;
  category: string;
  entry_ts: string;
  last_seen_ts: string;
  exit_ts: string | null;
  n_positions: number;
}

export interface ShapeFeatureCollection {
  type: "FeatureCollection";
  features: Array<{
    type: "Feature";
    id: number;
    properties: {
      shape_id: number;
      source: string;
      category: string;
      name: string;
    };
    geometry: any;
  }>;
}

async function jsonOrThrow<T>(r: Response): Promise<T> {
  if (!r.ok) {
    const body = await r.text();
    throw new Error(`${r.url} → ${r.status}: ${body.slice(0, 200)}`);
  }
  return r.json();
}

export const api = {
  health:   () => fetch("/api/health").then(jsonOrThrow<{ status: string }>),
  latest:   () => fetch("/api/positions/latest").then(jsonOrThrow<VesselPosition[]>),
  track:    (vessel_id: string, limit = 200) =>
              fetch(`/api/positions/track?vessel_id=${encodeURIComponent(vessel_id)}&limit=${limit}`)
                .then(jsonOrThrow<TrackPoint[]>),
  open:     () => fetch("/api/callings/open").then(jsonOrThrow<OpenCalling[]>),
  history:  (vessel_id: string, limit = 50) =>
              fetch(`/api/callings/history?vessel_id=${encodeURIComponent(vessel_id)}&limit=${limit}`)
                .then(jsonOrThrow<CallingHistoryRow[]>),
  shapes:   (source?: string) => {
              const q = source ? `?source=${encodeURIComponent(source)}` : "";
              return fetch(`/api/shapes/outlines${q}`).then(jsonOrThrow<ShapeFeatureCollection>);
            },
};
