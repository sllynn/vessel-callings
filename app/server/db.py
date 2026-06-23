"""SQL Warehouse connection helper.

OBO: every request opens a connection authenticated as the calling user.
We don't pool across users since each user has a distinct token; for the
expected demo concurrency (a handful of reviewers) per-request setup is
fine. A cached pool keyed by token hash is an easy optimisation if we see
latency biting.
"""

from contextlib import contextmanager
from databricks import sql

from .config import workspace_hostname, warehouse_http_path


@contextmanager
def user_connection(token: str):
    conn = sql.connect(
        server_hostname=workspace_hostname(),
        http_path=warehouse_http_path(),
        access_token=token,
    )
    try:
        yield conn
    finally:
        conn.close()


def rows_to_dicts(cursor) -> list[dict]:
    """Convert a cursor's current result set into a list of dicts.

    Timestamps come back as datetime objects; serialise to ISO strings on
    the way out so FastAPI's JSON encoder handles them deterministically.
    """
    cols = [d[0] for d in cursor.description]
    out = []
    for row in cursor.fetchall():
        d = {}
        for c, v in zip(cols, row):
            d[c] = v.isoformat() if hasattr(v, "isoformat") else v
        out.append(d)
    return out
