import os
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

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


# Diagnostic — surfaces the SQL connection params the backend computes
# at request time, plus the underlying exception if a real connect fails.
# Token is redacted to a 12-char prefix.
@app.get("/api/debug")
def debug(request: Request):
    from server.config import (
        IS_DATABRICKS_APP, workspace_host, workspace_hostname,
        warehouse_http_path, user_access_token, CATALOG, SCHEMA,
    )
    from databricks import sql
    out: dict = {
        "is_databricks_app": IS_DATABRICKS_APP,
        "workspace_host": workspace_host(),
        "workspace_hostname": workspace_hostname(),
        "warehouse_http_path": warehouse_http_path(),
        "catalog": CATALOG,
        "schema": SCHEMA,
        "has_xfat_header": "X-Forwarded-Access-Token" in request.headers,
    }
    try:
        token = user_access_token(request)
        out["token_prefix"] = token[:12] + "…"
        out["token_len"] = len(token)
    except Exception as e:
        out["token_error"] = repr(e)
        return out

    # Decode the JWT payload (without signature verification) to see what
    # scopes / audience / issuer the token actually carries.
    try:
        import base64, json as _json
        parts = token.split(".")
        if len(parts) >= 2:
            pad = "=" * (-len(parts[1]) % 4)
            payload = _json.loads(base64.urlsafe_b64decode(parts[1] + pad))
            # Trim long fields for readability
            for k, v in list(payload.items()):
                if isinstance(v, str) and len(v) > 200:
                    payload[k] = v[:200] + "…"
            out["jwt_payload"] = payload
    except Exception as e:
        out["jwt_decode_error"] = repr(e)

    # Probe the warehouse REST endpoint with the user's token — gives us
    # a clean HTTP status code if the token's being rejected, rather
    # than the connector's EOFError wrapper.
    try:
        import urllib.request, urllib.error
        wid = os.environ["DATABRICKS_WAREHOUSE_ID"]
        url = f"{workspace_host()}/api/2.0/sql/warehouses/{wid}"
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            resp = urllib.request.urlopen(req, timeout=10)
            body = resp.read().decode("utf-8", errors="replace")
            out["rest_probe"] = {"status": resp.status, "body_prefix": body[:300]}
        except urllib.error.HTTPError as e:
            out["rest_probe"] = {"status": e.code, "body_prefix": e.read().decode("utf-8", errors="replace")[:300]}
    except Exception as e:
        out["rest_probe_error"] = repr(e)

    # And the actual SQL connect attempt.
    try:
        conn = sql.connect(
            server_hostname=workspace_hostname(),
            http_path=warehouse_http_path(),
            access_token=token,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS x")
            out["query_result"] = cur.fetchall()
        conn.close()
        out["sql_ok"] = True
    except Exception as e:
        out["sql_ok"] = False
        out["sql_exception_type"] = type(e).__name__
        out["sql_exception_repr"] = repr(e)
        cause = getattr(e, "__cause__", None) or getattr(e, "__context__", None)
        if cause:
            out["sql_root_cause"] = repr(cause)
    return out


# Serve the built SPA in production. In local dev Parcel serves the SPA
# on :5173 and proxies /api/* here, so this block is a no-op locally.
#
# StaticFiles(html=True) at `/`:
#   - serves any file in frontend/dist/ directly (e.g. /frontend.<hash>.js,
#     /maplibre-gl.<hash>.js, etc. — Parcel writes them at the root of
#     dist/, not under /assets/),
#   - returns index.html for unmatched paths (the SPA fallback).
# Mounted LAST so the /api/* routers registered above take precedence.
frontend_dist = os.path.join(os.path.dirname(__file__), "frontend", "dist")
if os.path.exists(frontend_dist):
    app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")
