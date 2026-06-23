# Clarksons Vessel Callings — Databricks App

Live visualisation of the vessel-callings pipeline. React + Parcel + deck.gl
+ MapLibre frontend, FastAPI backend, queries via the `databricks-sql-connector`
against `geo_sme_emea_catalog.clarksons.*` on the **sme** workspace.

Authentication is **on-behalf-of (OBO) user-to-machine** — queries run as
the logged-in user, UC enforces their grants.

## Layout

```
app/
├── app.yaml                      # Databricks Apps manifest + user_authorization
├── pyproject.toml                # uv-managed Python deps
├── app.py                        # FastAPI entry point
├── server/
│   ├── config.py                 # env + token plumbing (OBO + local-dev fallback)
│   ├── db.py                     # SQL Warehouse connection helper
│   └── routes/
│       └── positions.py          # GET /api/positions/latest (stub)
└── frontend/
    ├── package.json              # React + Parcel + deck.gl + maplibre-gl
    └── src/
        ├── App.tsx               # placeholder shell (map view pending)
        └── api.ts                # typed fetch helpers
```

## Local development

Two processes side-by-side: Parcel serving the SPA on `:5173` with `/api/*`
proxied to FastAPI on `:8000` (via `.proxyrc.json`).

```bash
# Backend
cd app
uv sync                                       # creates .venv, installs deps
DATABRICKS_PROFILE=sme \
DATABRICKS_WAREHOUSE_ID=994009ac5de169d0 \
CLARKSONS_CATALOG=geo_sme_emea_catalog \
CLARKSONS_SCHEMA=clarksons \
uv run uvicorn app:app --reload --port 8000

# Frontend (separate terminal)
cd app/frontend
npm install
npm run dev                                   # opens http://localhost:5173
```

In local dev the OBO header isn't set, so `server/config.py` falls back
to minting a token from the `DATABRICKS_PROFILE` CLI profile (sme by
default). That means you can iterate without deploying; queries still
run as you, against the real warehouse.

## Deploying to the workspace

```bash
# One-time, before the first deploy
databricks apps create clarksons-vessel-callings \
  --description "Live vessel-callings visualisation" -p sme

# Build the frontend — this writes to frontend/dist/ which is committed
# to the repo (not gitignored), so the next `databricks sync` picks it up.
cd app/frontend && npm run build && cd ..

# Sync source to the workspace. Excludes node_modules / .venv / caches.
# `databricks sync` honours .gitignore, so anything in there is excluded
# automatically — and conversely, frontend/dist/ MUST stay un-ignored or
# the workspace won't have the static assets the FastAPI app serves.
databricks sync . /Workspace/Users/stuart.lynn@databricks.com/clarksons-vessel-callings \
  --exclude node_modules \
  --exclude .venv \
  --exclude __pycache__ \
  --exclude .parcel-cache \
  --exclude frontend/src \
  -p sme --full

# Deploy
databricks apps deploy clarksons-vessel-callings \
  --source-code-path /Workspace/Users/stuart.lynn@databricks.com/clarksons-vessel-callings \
  -p sme

# Get the app URL
databricks apps get clarksons-vessel-callings -p sme
```

After the first deploy:

1. Open the app URL — the consent screen asks the user to grant the
   `sql` and `iam:current-user:read` scopes.
2. Confirm the SQL Warehouse resource binding under
   **Compute → Apps → clarksons-vessel-callings → Edit**.
3. Each user needs `USE CATALOG geo_sme_emea_catalog`, `USE SCHEMA
   clarksons`, `SELECT` on the relevant tables, and `CAN_USE` on the
   warehouse, granted via UC. For internal demo this is just you and
   anyone you share it with.

## Endpoints

| Endpoint | Status | Description |
|---|---|---|
| `GET /api/health` | ✅ stub | liveness probe |
| `GET /api/positions/latest` | ✅ stub | one row per vessel, most recent silver position |
| `GET /api/callings/open` | ⏳ | (next iteration) open callings |
| `GET /api/callings/history` | ⏳ | (next iteration) closed callings, filtered by shape |
| `GET /api/shapes/outlines` | ⏳ | (next iteration) shape polygons as GeoJSON |

## Next steps

1. Render the deck.gl `ScatterplotLayer` over a MapLibre base map.
2. Add the open-callings endpoint and a side panel.
3. Build the calling Gantt view.
4. Add the as-of slider for the bitemporal demo.
