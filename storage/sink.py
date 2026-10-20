"""存储层：Parquet 数据湖 + SQLite 聚合结果。

- 原始/富化数据 → Parquet（作业认可格式，MinIO 可后加）
- 批处理聚合结果 → SQLite（量小，标准库零安装）
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

log = logging.getLogger("newspulse.storage")


def write_parquet_df(spark, rows: list[dict], out_dir: Path, mode: str = "append") -> None:
    """把 dict 列表写成 Parquet。小数据量，collect 到 driver 端可接受。"""
    if not rows:
        log.warning("无数据可写: %s", out_dir)
        return
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    df = spark.createDataFrame(rows)
    df.write.mode(mode).parquet(str(out_dir))
    log.info("💾 写入 Parquet %d 行 → %s", len(rows), out_dir)


class SqliteSink:
    """聚合结果落 SQLite。"""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path))

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS hotspots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    title TEXT,
                    mentions INTEGER,
                    score REAL,
                    window_start TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    keyword TEXT,
                    cnt INTEGER,
                    window_start TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )
                """
            )

    def insert_hotspots(self, rows: list[dict]) -> None:
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                "INSERT INTO hotspots (url, title, mentions, score, window_start)"
                " VALUES (?, ?, ?, ?, ?)",
                [
                    (r.get("url"), r.get("title"), r.get("mentions"),
                     r.get("score"), r.get("window_start"))
                    for r in rows
                ],
            )
        log.info("💾 写入 SQLite hotspots %d 行", len(rows))

    def insert_trends(self, rows: list[dict]) -> None:
        if not rows:
            return
        with self._connect() as conn:
            conn.executemany(
                "INSERT INTO trends (keyword, cnt, window_start) VALUES (?, ?, ?)",
                [(r.get("keyword"), r.get("cnt"), r.get("window_start")) for r in rows],
            )
        log.info("💾 写入 SQLite trends %d 行", len(rows))

    def fetch_hotspots(self, limit: int = 10) -> list[dict]:
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT title, url, mentions, score FROM hotspots"
                " ORDER BY mentions DESC, score DESC LIMIT ?",
                (limit,),
            )
            return [dict(r) for r in cur.fetchall()]
