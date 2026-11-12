"""业务编排：上传入库、作业触发、取数视图组装。"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from typing import Optional

from . import config, repository
from .csvparser import ParsedCSV, parse_csv
from .db import transaction
from .schemas import DaySummary, DistrictResult, FileInfo, JobProgress, ResultResponse


def _file_info(row: sqlite3.Row) -> FileInfo:
    return FileInfo(
        file_type=row["file_type"],
        version=row["version"],
        original_name=row["original_name"],
        size_bytes=row["size_bytes"],
        row_count=row["row_count"],
        sha256=row["sha256"],
        uploaded_at=row["uploaded_at"],
    )


def _stored_path(business_date: str, file_type: str, version: int, digest: str) -> str:
    day_dir = config.storage_dir() / business_date
    day_dir.mkdir(parents=True, exist_ok=True)
    return str(day_dir / f"{file_type}_v{version}_{digest[:12]}.csv")


def store_upload(conn: sqlite3.Connection, business_date: str, file_type: str,
                 filename: str, raw: bytes) -> tuple[int, JobProgress | None]:
    """校验并落盘一份新文件，必要时作废在途作业并触发重算。

    返回 (file_id, 新作业进度或 None)。校验失败抛 UploadError，现有数据不动。
    """
    if len(raw) > config.max_upload_bytes():
        from .errors import UploadError
        raise UploadError(
            "FILE_TOO_LARGE",
            f"文件 {len(raw)} 字节超过上限 {config.max_upload_bytes()} 字节",
        )
    parsed: ParsedCSV = parse_csv(raw, file_type)
    digest = hashlib.sha256(raw).hexdigest()

    new_job: Optional[JobProgress] = None
    with transaction(conn):
        version = repository.next_version(conn, business_date, file_type)
        path = _stored_path(business_date, file_type, version, digest)
        # 规范化落盘：UTF-8、逻辑列顺序，worker 之后无需再嗅探编码与表头
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8", newline="") as fh:
            fh.write(",".join(parsed.header) + "\n")
            for row in parsed.rows:
                fh.write(",".join(row) + "\n")
        os.replace(tmp_path, path)

        file_id = repository.insert_file(
            conn, business_date, file_type, path, filename,
            len(raw), parsed.row_count, digest,
        )

        traffic = repository.get_current_file(conn, business_date, "traffic")
        ownership = repository.get_current_file(conn, business_date, "ownership")
        if traffic is not None and ownership is not None:
            # 旧结果立即失效：在途作业全部作废，按当前两份文件重新排队
            repository.supersede_active_jobs(conn, business_date)
            job_id = repository.enqueue_job(
                conn, business_date, traffic["id"], ownership["id"])
            row = repository.get_job(conn, job_id)
            new_job = build_job_progress(conn, row)
    return file_id, new_job


def build_job_progress(conn: sqlite3.Connection, row: sqlite3.Row) -> JobProgress:
    missing = json.loads(row["missing_ownership"]) if row["missing_ownership"] else []
    traffic = repository.get_file_by_id(conn, row["traffic_file_id"])
    ownership = repository.get_file_by_id(conn, row["ownership_file_id"])
    current_traffic = repository.get_current_file(conn, row["business_date"], "traffic")
    current_ownership = repository.get_current_file(conn, row["business_date"], "ownership")
    # current 同时要求：文件对仍是当前版本，且该作业是这个文件对的最新一代
    latest_pair_job = repository.get_latest_job_for_pair(
        conn, row["business_date"], row["traffic_file_id"], row["ownership_file_id"])
    is_current = bool(
        current_traffic is not None and current_ownership is not None
        and current_traffic["id"] == traffic["id"]
        and current_ownership["id"] == ownership["id"]
        and latest_pair_job is not None and latest_pair_job["id"] == row["id"]
    )
    return JobProgress(
        job_id=row["id"],
        business_date=row["business_date"],
        status=row["status"],
        progress_percent=row["progress_percent"],
        progress_stage=row["progress_stage"],
        error_code=row["error_code"],
        error_message=row["error_message"],
        missing_ownership=missing,
        result_row_count=row["result_row_count"],
        generation=row["generation"],
        current=is_current,
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def build_result_response(conn: sqlite3.Connection, business_date: str) -> ResultResponse:
    """取数只认当前两份文件对应的最新作业，其他状态明确说明，绝不返回陈旧结果。"""
    traffic = repository.get_current_file(conn, business_date, "traffic")
    ownership = repository.get_current_file(conn, business_date, "ownership")

    base = dict(business_date=business_date)
    if traffic is None or ownership is None:
        missing = [t for t, f in (("traffic", traffic), ("ownership", ownership)) if f is None]
        return ResultResponse(
            **base, ready=False, status="waiting_files",
            message=f"文件未到齐，缺少：{ '、'.join(missing) }（traffic=流量文件，ownership=归属文件）",
            traffic_file=_file_info(traffic) if traffic else None,
            ownership_file=_file_info(ownership) if ownership else None,
        )

    job = repository.get_latest_job_for_pair(
        conn, business_date, traffic["id"], ownership["id"])
    if job is None:
        return ResultResponse(
            **base, ready=False, status="waiting_files",
            message="两份文件已到齐，作业尚未创建，请稍后查询",
            traffic_file=_file_info(traffic), ownership_file=_file_info(ownership),
        )

    common = dict(
        job_id=job["id"], generation=job["generation"],
        traffic_file=_file_info(traffic), ownership_file=_file_info(ownership),
        error_code=job["error_code"], error_message=job["error_message"],
        missing_ownership=json.loads(job["missing_ownership"])
        if job["missing_ownership"] else [],
    )

    if job["status"] == "succeeded":
        rows = repository.get_results(conn, job["id"])
        results = [
            DistrictResult(
                district=r["district"], cell_count=r["cell_count"],
                uplink_bytes=r["uplink_bytes"], downlink_bytes=r["downlink_bytes"],
                total_bytes=r["total_bytes"],
            ) for r in rows
        ]
        return ResultResponse(
            **base, ready=True, status="ready",
            message=f"计算完成，共 {len(results)} 个片区", results=results, **common,
        )
    if job["status"] == "failed":
        return ResultResponse(
            **base, ready=False, status="failed",
            message=f"计算失败：{job['error_message']}", **common,
        )
    if job["status"] in ("queued", "running"):
        return ResultResponse(
            **base, ready=False, status="computing",
            message=f"计算中：{job['progress_stage']}（{job['progress_percent']}%）", **common,
        )
    # superseded：理论上当前文件对的最新作业不会是该状态，防御性处理
    return ResultResponse(
        **base, ready=False, status="superseded",
        message="当前作业已被取代，等待新作业完成", **common,
    )


def build_day_summary(conn: sqlite3.Connection, business_date: str) -> DaySummary:
    traffic = repository.get_current_file(conn, business_date, "traffic")
    ownership = repository.get_current_file(conn, business_date, "ownership")
    latest_job_id = None
    ready = False
    status = "waiting_files"
    if traffic and ownership:
        job = repository.get_latest_job_for_pair(
            conn, business_date, traffic["id"], ownership["id"])
        if job is not None:
            latest_job_id = job["id"]
            status = job["status"]
            ready = job["status"] == "succeeded"
    return DaySummary(
        business_date=business_date, ready=ready, status=status,
        traffic_version=traffic["version"] if traffic else None,
        ownership_version=ownership["version"] if ownership else None,
        latest_job_id=latest_job_id,
    )
