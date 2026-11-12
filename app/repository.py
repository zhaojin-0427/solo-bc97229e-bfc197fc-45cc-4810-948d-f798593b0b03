"""数据访问层：文件版本、作业生命周期、结果读写全部走这里。"""
from __future__ import annotations

import json
import sqlite3
from typing import List, Optional

from .db import transaction
from .errors import JobFailure

TERMINAL_STATES = ("succeeded", "failed", "superseded")


# ---------- 文件版本 ----------

def get_current_file(conn: sqlite3.Connection, business_date: str, file_type: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM files WHERE business_date=? AND file_type=? "
        "ORDER BY version DESC LIMIT 1",
        (business_date, file_type),
    ).fetchone()


def get_file_by_id(conn: sqlite3.Connection, file_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM files WHERE id=?", (file_id,)).fetchone()


def next_version(conn: sqlite3.Connection, business_date: str, file_type: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS v FROM files WHERE business_date=? AND file_type=?",
        (business_date, file_type),
    ).fetchone()
    return int(row["v"]) + 1


def insert_file(
    conn: sqlite3.Connection,
    business_date: str,
    file_type: str,
    version: int,
    stored_path: str,
    original_name: str,
    size_bytes: int,
    row_count: int,
    sha256: str,
) -> int:
    """插入一个文件版本。version 由调用方在同一事务内用 next_version 预先取得，
    以便规范化文件命名与入库版本号严格一致（共同提交）。"""
    cur = conn.execute(
        "INSERT INTO files (business_date, file_type, version, stored_path, original_name, "
        "size_bytes, row_count, sha256) VALUES (?,?,?,?,?,?,?,?)",
        (business_date, file_type, version, stored_path, original_name, size_bytes, row_count, sha256),
    )
    return int(cur.lastrowid)


# ---------- 作业调度 ----------

def supersede_active_jobs(conn: sqlite3.Connection, business_date: str) -> int:
    """两份文件齐活触发新作业前，把在途旧作业作废（迟到结果不得发布）。"""
    cur = conn.execute(
        "UPDATE jobs SET status='superseded', finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'), "
        "progress_stage='已被新上传的文件取代' "
        "WHERE business_date=? AND status IN ('queued','running')",
        (business_date,),
    )
    return cur.rowcount


def next_generation(conn: sqlite3.Connection, business_date: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(generation), 0) AS g FROM jobs WHERE business_date=?",
        (business_date,),
    ).fetchone()
    return int(row["g"]) + 1


def enqueue_job(conn: sqlite3.Connection, business_date: str, traffic_id: int, ownership_id: int) -> int:
    generation = next_generation(conn, business_date)
    cur = conn.execute(
        "INSERT INTO jobs (business_date, traffic_file_id, ownership_file_id, generation, status, "
        "progress_percent, progress_stage) VALUES (?,?,?,?,'queued',0,'等待调度')",
        (business_date, traffic_id, ownership_id, generation),
    )
    return int(cur.lastrowid)


def reset_interrupted_jobs(conn: sqlite3.Connection) -> int:
    """重启恢复：上次 running 中断的作业回到队列重头再算（结果事务化，无半成品）。"""
    cur = conn.execute(
        "UPDATE jobs SET status='queued', started_at=NULL, progress_percent=0, "
        "progress_stage='服务重启，重新排队' WHERE status='running'"
    )
    return cur.rowcount


def claim_next_job(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    """原子认领一个排队作业，保证同一时刻只有一个 worker 处理它。"""
    with transaction(conn):
        row = conn.execute(
            "SELECT * FROM jobs WHERE status='queued' ORDER BY id LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        cur = conn.execute(
            "UPDATE jobs SET status='running', started_at=strftime('%Y-%m-%dT%H:%M:%fZ','now'), "
            "progress_stage='计算中' WHERE id=? AND status='queued'",
            (row["id"],),
        )
        if cur.rowcount == 0:
            return None
    return conn.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()


def update_progress(conn: sqlite3.Connection, job_id: int, percent: int, stage: str) -> None:
    """仅当作业仍处于 running 时更新；已被取代则抛 JobFailure 中止计算。"""
    cur = conn.execute(
        "UPDATE jobs SET progress_percent=?, progress_stage=? WHERE id=? AND status='running'",
        (percent, stage, job_id),
    )
    if cur.rowcount == 0:
        raise JobFailure("SUPERSEDED", "作业已被新上传的文件取代，结果放弃发布")


def complete_job(conn: sqlite3.Connection, job_id: int, district_rows: List[tuple]) -> bool:
    """发布结果。返回 False 表示作业在计算期间被取代，结果丢弃。"""
    with transaction(conn):
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None or row["status"] != "running":
            return False
        conn.execute(
            "UPDATE jobs SET status='succeeded', progress_percent=100, progress_stage='计算完成', "
            "result_row_count=?, finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (len(district_rows), job_id),
        )
        conn.executemany(
            "INSERT INTO results (job_id, business_date, district, total_bytes, uplink_bytes, "
            "downlink_bytes, cell_count) VALUES (?,?,?,?,?,?,?)",
            [(job_id, row["business_date"], d, t, u, dl2, c) for (d, c, u, dl2, t) in district_rows],
        )
    return True


def fail_job(conn: sqlite3.Connection, job_id: int, code: str, message: str,
             missing_ownership: Optional[List[str]] = None) -> bool:
    cur = conn.execute(
        "UPDATE jobs SET status='failed', progress_stage='计算失败', error_code=?, error_message=?, "
        "missing_ownership=?, finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
        "WHERE id=? AND status='running'",
        (code, message, json.dumps(missing_ownership or [], ensure_ascii=False), job_id),
    )
    return cur.rowcount > 0


# ---------- 查询 ----------

def get_job(conn: sqlite3.Connection, job_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()


def list_jobs(conn: sqlite3.Connection, business_date: Optional[str] = None) -> List[sqlite3.Row]:
    if business_date:
        return list(conn.execute(
            "SELECT * FROM jobs WHERE business_date=? ORDER BY id DESC", (business_date,)))
    return list(conn.execute("SELECT * FROM jobs ORDER BY id DESC"))


def get_latest_job_for_pair(
    conn: sqlite3.Connection, business_date: str, traffic_id: int, ownership_id: int
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM jobs WHERE business_date=? AND traffic_file_id=? AND ownership_file_id=? "
        "ORDER BY generation DESC, id DESC LIMIT 1",
        (business_date, traffic_id, ownership_id),
    ).fetchone()


def get_results(conn: sqlite3.Connection, job_id: int) -> List[sqlite3.Row]:
    return list(conn.execute(
        "SELECT * FROM results WHERE job_id=? ORDER BY district", (job_id,)))


def list_business_dates(conn: sqlite3.Connection) -> List[str]:
    return [r["business_date"] for r in conn.execute(
        "SELECT DISTINCT business_date FROM ("
        "SELECT business_date FROM files UNION SELECT business_date FROM jobs) "
        "ORDER BY business_date")]
