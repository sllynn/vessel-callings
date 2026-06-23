import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from server.routes import positions, callings, shapes

app = FastAPI(
    title="Clarksons Vessel Callings",
    description="Live vessel-callings visualisation over geo_sme_emea_catalog.clarksons",
)

app.include_router(positions.router)
app.include_router(callings.router)
app.include_router(shapes.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}


# Serve the built SPA in production. In local dev Parcel serves the SPA
# on :5173 and proxies /api/* here, so this block is a no-op.
frontend_dist = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.exists(frontend_dist):
    assets_dir = os.path.join(frontend_dist, "assets")
    if os.path.isdir(assets_dir):
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

    @app.get("/{full_path:path}")
    def serve_spa(full_path: str):
        return FileResponse(os.path.join(frontend_dist, "index.html"))
