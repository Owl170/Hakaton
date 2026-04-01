import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from backend.app.config import settings


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def get_conn():
    conn = sqlite3.connect(settings.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                filename TEXT NOT NULL,
                stored_path TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_name TEXT NOT NULL,
                territories_json TEXT NOT NULL,
                years_json TEXT NOT NULL,
                status TEXT NOT NULL,
                result_geojson_path TEXT,
                summary_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS detections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                analysis_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                territory TEXT NOT NULL,
                parcel_id TEXT NOT NULL,
                feature_type TEXT NOT NULL,
                risk_level TEXT NOT NULL,
                risk_score REAL NOT NULL,
                area_ha REAL NOT NULL,
                geometry_json TEXT NOT NULL,
                metrics_json TEXT NOT NULL,
                FOREIGN KEY (analysis_id) REFERENCES analyses(id)
            );
            """
        )
        conn.commit()


def set_setting(key: str, value: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        conn.commit()


def get_setting(key: str) -> str | None:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None


def insert_upload(kind: str, filename: str, stored_path: str) -> int:
    now = utc_now_iso()
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO uploads(kind, filename, stored_path, created_at)
            VALUES(?, ?, ?, ?)
            """,
            (kind, filename, stored_path, now),
        )
        conn.commit()
        return int(cur.lastrowid)


def insert_analysis(run_name: str, territories: list[str], years: list[int], status: str = "running") -> int:
    now = utc_now_iso()
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO analyses(
                run_name, territories_json, years_json, status,
                result_geojson_path, summary_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                run_name,
                json.dumps(territories, ensure_ascii=True),
                json.dumps(years),
                status,
                json.dumps({}),
                now,
                now,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def update_analysis(
    analysis_id: int,
    *,
    status: str | None = None,
    result_geojson_path: str | None = None,
    summary: dict[str, Any] | None = None,
) -> None:
    updates = []
    params: list[Any] = []
    if status is not None:
        updates.append("status=?")
        params.append(status)
    if result_geojson_path is not None:
        updates.append("result_geojson_path=?")
        params.append(result_geojson_path)
    if summary is not None:
        updates.append("summary_json=?")
        params.append(json.dumps(summary, ensure_ascii=True))

    updates.append("updated_at=?")
    params.append(utc_now_iso())
    params.append(analysis_id)

    with get_conn() as conn:
        conn.execute(f"UPDATE analyses SET {', '.join(updates)} WHERE id=?", params)
        conn.commit()


def insert_detections(analysis_id: int, detections: list[dict[str, Any]]) -> None:
    if not detections:
        return
    with get_conn() as conn:
        conn.executemany(
            """
            INSERT INTO detections(
                analysis_id, year, territory, parcel_id, feature_type, risk_level,
                risk_score, area_ha, geometry_json, metrics_json
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    analysis_id,
                    d["year"],
                    d["territory"],
                    d["parcel_id"],
                    d["feature_type"],
                    d["risk_level"],
                    d["risk_score"],
                    d["area_ha"],
                    d["geometry_json"],
                    d["metrics_json"],
                )
                for d in detections
            ],
        )
        conn.commit()


def row_to_analysis(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "run_name": row["run_name"],
        "territories": json.loads(row["territories_json"] or "[]"),
        "years": json.loads(row["years_json"] or "[]"),
        "status": row["status"],
        "result_geojson_path": row["result_geojson_path"],
        "summary": json.loads(row["summary_json"] or "{}"),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def list_analyses() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM analyses ORDER BY datetime(created_at) DESC, id DESC"
        ).fetchall()
    return [row_to_analysis(row) for row in rows]


def get_analysis(analysis_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM analyses WHERE id=?", (analysis_id,)).fetchone()
    return row_to_analysis(row) if row else None


def get_latest_completed_analysis_id() -> int | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT id FROM analyses
            WHERE status='completed'
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
    return int(row["id"]) if row else None


def list_detections(
    analysis_id: int,
    *,
    territory: str | None = None,
    year: int | None = None,
    risk_level: str | None = None,
    feature_type: str | None = None,
) -> list[dict[str, Any]]:
    query = "SELECT * FROM detections WHERE analysis_id=?"
    params: list[Any] = [analysis_id]
    if territory:
        query += " AND territory=?"
        params.append(territory)
    if year is not None:
        query += " AND year=?"
        params.append(year)
    if risk_level:
        query += " AND risk_level=?"
        params.append(risk_level)
    if feature_type:
        query += " AND feature_type=?"
        params.append(feature_type)

    query += " ORDER BY year, territory, risk_score DESC"
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()

    detections: list[dict[str, Any]] = []
    for row in rows:
        detections.append(
            {
                "id": row["id"],
                "analysis_id": row["analysis_id"],
                "year": row["year"],
                "territory": row["territory"],
                "parcel_id": row["parcel_id"],
                "feature_type": row["feature_type"],
                "risk_level": row["risk_level"],
                "risk_score": row["risk_score"],
                "area_ha": row["area_ha"],
                "geometry_json": row["geometry_json"],
                "metrics_json": row["metrics_json"],
            }
        )
    return detections
