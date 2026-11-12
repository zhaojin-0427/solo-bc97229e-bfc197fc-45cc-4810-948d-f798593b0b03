"""SQLite 连接与表结构。

设计要点：
- WAL 模式 + busy timeout，让后台 worker 写库时 API 仍可查询；
- 输入文件、作业、结果全部持久化，重启后可恢复；
- 所有写操作走短事务，作业领取用 ``UPDATE ... WHERE status='queued'``
  原子认领，避免多 worker/重启重复计算。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Iterator

from . import config

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- 每个业务日、每种文件的当前版本（整份替换即新增一行版本记录）
CREATE TABLE IF NOT EXISTS files (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    business_date   TEXT    NOT NULL,
    file_type       TEXT    NOT NULL CHECK (file_type IN ('traffic','ownership')),
    version         INTEGER NOT NULL,
    stored_path     TEXT    NOT NULL,
    original_name   TEXT    NOT NULL,
    size_bytes      INTEGER NOT NULL,
    row_count       INTEGER NOT NULL,
    sha256          TEXT    NOT NULL,
    uploaded_at     TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (business_date, file_type, version)
);

-- 计算作业
CREATE TABLE IF NOT EXISTS jobs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    business_date         TEXT    NOT NULL,
    traffic_file_id       INTEGER NOT NULL REFERENCES files(id),
    ownership_file_id     INTEGER NOT NULL REFERENCES files(id),
    generation            INTEGER NOT NULL,   -- 同一对文件重复触发时单调递增
    status                TEXT    NOT NULL
                              CHECK (status IN ('queued','running','succeeded','failed','superseded')),
    progress_percent      INTEGER NOT NULL DEFAULT 0,
    progress_stage        TEXT    NOT NULL DEFAULT '等待调度',
    error_code            TEXT,
    error_message         TEXT,
    missing_ownership     TEXT,               -- 缺归属的基站ID，JSON 数组
    result_row_count      INTEGER,
    created_at            TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    started_at            TEXT,
    finished_at           TEXT,
    UNIQUE (traffic_file_id, ownership_file_id, generation)
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs (status, id);
CREATE INDEX IF NOT EXISTS idx_jobs_date ON jobs (business_date, id);

-- 片区汇总结果，仅成功作业写入；作业被取代后其结果查询层不再可见
CREATE TABLE IF NOT EXISTS results (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id           INTEGER NOT NULL REFERENCES jobs(id),
    business_date    TEXT    NOT NULL,
    district         TEXT    NOT NULL,
    total_bytes      INTEGER NOT NULL,       -- 该片区上下行合计
    uplink_bytes     INTEGER NOT NULL,
    downlink_bytes   INTEGER NOT NULL,
    cell_count       INTEGER NOT NULL,       -- 参与汇总的基站数
    UNIQUE (job_id, district)
);
CREATE INDEX IF NOT EXISTS idx_results_job ON results (job_id);
"""


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.db_path(),
        timeout=30,
        isolation_level=None,  # 显式 BEGIN/COMMIT，事务边界清晰
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE;")
    try:
        yield conn
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise


def init_db() -> None:
    config.ensure_dirs()
    conn = connect()
    try:
        conn.executescript(_SCHEMA)
    finally:
        conn.close()
