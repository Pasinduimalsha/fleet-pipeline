"""Postgres helpers shared by the streaming job, batch DAG and API.

All writers upsert (``ON CONFLICT ... DO UPDATE``) so replays, Spark
micro-batch retries and Airflow task retries never create duplicate rows.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterable, Sequence

import psycopg2
import psycopg2.extras

from common.config import settings


@contextmanager
def get_connection():
    conn = psycopg2.connect(settings.db_dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_rows(
    conn,
    table: str,
    rows: Sequence[dict],
    conflict_cols: Sequence[str],
    update_cols: Sequence[str] | None = None,
) -> int:
    """Bulk upsert a list of dict rows into ``table``.

    ``update_cols`` defaults to every column except the conflict key, which
    is the common case for our idempotent sink tables.
    """
    if not rows:
        return 0

    columns = list(rows[0].keys())
    if update_cols is None:
        update_cols = [c for c in columns if c not in conflict_cols]

    values = [[row.get(c) for c in columns] for row in rows]

    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    conflict_clause = ", ".join(conflict_cols)
    sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
        f"ON CONFLICT ({conflict_clause}) DO UPDATE SET {set_clause}"
    )
    if not update_cols:
        sql = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s "
            f"ON CONFLICT ({conflict_clause}) DO NOTHING"
        )

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)
        return cur.rowcount


def insert_rows(conn, table: str, rows: Sequence[dict]) -> int:
    if not rows:
        return 0
    columns = list(rows[0].keys())
    values = [[row.get(c) for c in columns] for row in rows]
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values)
        return cur.rowcount


def mark_heartbeat(conn, component: str, detail: str = "") -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_status (component, last_success_at, detail)
            VALUES (%s, now(), %s)
            ON CONFLICT (component) DO UPDATE
                SET last_success_at = EXCLUDED.last_success_at,
                    detail = EXCLUDED.detail
            """,
            (component, detail),
        )


def fetch_all(conn, sql: str, params: Iterable = ()) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def fetch_one(conn, sql: str, params: Iterable = ()) -> dict | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None
