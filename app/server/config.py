"""Environment + auth helpers.

Runs in two modes:
  - Databricks Apps platform (production):
      `DATABRICKS_APP_NAME` is set; user tokens arrive on every request as
      the `X-Forwarded-Access-Token` header. `DATABRICKS_HOST` is the bare
      hostname (no scheme).
  - Local dev (`uvicorn app:app --reload`):
      We use the Databricks CLI profile named by `DATABRICKS_PROFILE`
      (default `sme`) to mint a token. Lets a developer iterate without
      deploying.
"""

import os
from fastapi import Request, HTTPException

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))

CATALOG = os.environ.get("CLARKSONS_CATALOG", "geo_sme_emea_catalog")
SCHEMA  = os.environ.get("CLARKSONS_SCHEMA",  "clarksons")


def workspace_host() -> str:
    """Workspace host with the https:// scheme.

    DATABRICKS_HOST in the Apps runtime is just the hostname; locally the
    SDK includes the scheme. We normalise so callers always get a full URL.
    """
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    if not host and not IS_DATABRICKS_APP:
        # Local fallback: ask the SDK using the configured profile.
        from databricks.sdk import WorkspaceClient
        profile = os.environ.get("DATABRICKS_PROFILE", "sme")
        host = WorkspaceClient(profile=profile).config.host
    return host


def workspace_hostname() -> str:
    """Bare hostname (no scheme) — what databricks-sql-connector expects."""
    return workspace_host().removeprefix("https://").removeprefix("http://")


def warehouse_http_path() -> str:
    wid = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not wid:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID not set")
    return f"/sql/1.0/warehouses/{wid}"


def user_access_token(request: Request) -> str:
    """OBO: the user's forwarded OAuth token from the Apps platform.

    Falls back to the local CLI profile's token in dev so the same code
    path works without deploying.
    """
    token = request.headers.get("X-Forwarded-Access-Token")
    if token:
        return token

    if IS_DATABRICKS_APP:
        # In production this header should always be present — its absence
        # means user_authorization is misconfigured in app.yaml.
        raise HTTPException(
            status_code=401,
            detail="X-Forwarded-Access-Token missing — check user_authorization.scopes in app.yaml",
        )

    # Local dev fallback — use the CLI profile to mint a token.
    from databricks.sdk import WorkspaceClient
    profile = os.environ.get("DATABRICKS_PROFILE", "sme")
    headers = WorkspaceClient(profile=profile).config.authenticate()
    if not headers or "Authorization" not in headers:
        raise HTTPException(status_code=500, detail="local dev: could not mint token from CLI profile")
    return headers["Authorization"].removeprefix("Bearer ").strip()
