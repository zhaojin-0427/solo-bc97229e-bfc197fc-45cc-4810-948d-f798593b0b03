"""业务编排：流式上传入库、作业触发、取数视图组装。

内存与事务边界（改造重点）：
- 接收阶段分块把上传流落盘到暂存区，边读边算 SHA-256 与大小，常驻内存只有一个块；
- 解码、整份基站ID 查重、规范化写盘全部基于磁盘流式进行，期间不打开正式库的写事务，
  其他业务日（乃至同业务日的不同类型）上传不受影响；
- 规范化文件 fsync 完整持久化之后，才在一个短事务内共同提交
  “文件版本 + 重算作业”，并在同一事务里把暂存文件原子改名到正式目录；
- 任一步失败都只清理本次暂存，正式文件、作业、结果保持不变。
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
from typing import BinaryIO, Optional, Tuple

from . import config, repository, staging
from .csvparser import stream_ingest
from .db import transaction
from .errors import UploadError
from .schemas import DaySummary, DistrictResult, FileInfo, JobProgress, ResultResponse

# 显式保留内建 open 的模块级引用，便于测试注入写盘故障
_open = open


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


def _final_stored_path(business_date: str, file_type: str, version: int, digest: str) -> str:
    day_dir = config.storage_dir() / business_date
    day_dir.mkdir(parents=True, exist_ok=True)
    return str(day_dir / f"{file_type}_v{version}_{digest[:12]}.csv")


def _stream_raw_to_staging(source: BinaryIO, raw_path, max_bytes: int) -> Tuple[str, int]:
    """分块读取上传流写入暂存原件，逐块累计 SHA-256 与字节数。

    内存占用固定为一个块（``UPLOAD_CHUNK_BYTES``），与文件总行数/总大小无关。
    超过上限立即拒收，不落完整文件。
    """
    digest = hashlib.sha256()
    total = 0
    chunk_size = config.upload_chunk_bytes()
    with _open(raw_path, "wb") as raw_fh:
        while True:
            chunk = source.read(chunk_size)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise UploadError(
                    "FILE_TOO_LARGE",
                    f"文件超过上限 {max_bytes} 字节（已接收 {total} 字节），整份拒收",
                )
            digest.update(chunk)
            raw_fh.write(chunk)
        raw_fh.flush()
        os.fsync(raw_fh.fileno())
    if total == 0:
        raise UploadError("EMPTY_FILE", "上传文件为空")
    return digest.hexdigest(), total


def ingest_upload_stream(
    conn: sqlite3.Connection,
    business_date: str,
    file_type: str,
    filename: str,
    source: BinaryIO,
) -> Tuple[int, Optional[JobProgress]]:
    """流式接收、校验、规范化并原子提交一份新文件。

    返回 (file_id, 新作业进度或 None)。任何校验/写盘失败抛 :class:`UploadError`，
    现有版本、作业与结果均不变，本次暂存被清理。
    """
    max_bytes = config.max_upload_bytes()
    paths = staging.StagingPaths.create()
    dedup = staging.SqliteDedup(str(paths.dedup_db_path))
    try:
        # 1) 流式接收：内存只有一个块；超限/空文件在此即拒收
        digest, size_bytes = _stream_raw_to_staging(source, paths.raw_path, max_bytes)

        # 2) 流式解码 + 磁盘查重 + 规范化写盘（不持有正式库写事务）
        try:
            with _open(paths.normalized_path, "w", encoding="utf-8", newline="") as norm_fh:
                stats = stream_ingest(
                    str(paths.raw_path), file_type, norm_fh, dedup,
                )
                # 规范化内容必须完整落到磁盘，后续才允许提交
                norm_fh.flush()
                os.fsync(norm_fh.fileno())
            dedup.commit()  # 查重事务仅在整份校验通过后提交
        except UploadError:
            raise
        except OSError as exc:
            raise UploadError("WRITE_FAILED", f"规范化文件写盘失败，整份拒收：{exc!r}")

        # 3) 短事务共同提交：文件版本 + 重算作业，改名在事务内原子完成
        try:
            with transaction(conn):
                version = repository.next_version(conn, business_date, file_type)
                final_path = _final_stored_path(business_date, file_type, version, digest)
                file_id = repository.insert_file(
                    conn, business_date, file_type, version, final_path, filename,
                    size_bytes, stats.row_count, digest,
                )
                try:
                    os.replace(str(paths.normalized_path), final_path)
                    staging.fsync_dir(config.storage_dir() / business_date)
                except OSError as exc:
                    # 改名失败：事务回滚，files 行不会留下，正式数据不变
                    raise UploadError(
                        "WRITE_FAILED", f"规范化文件落盘失败，整份拒收：{exc!r}"
                    )

                new_job: Optional[JobProgress] = None
                traffic = repository.get_current_file(conn, business_date, "traffic")
                ownership = repository.get_current_file(conn, business_date, "ownership")
                if traffic is not None and ownership is not None:
                    # 旧结果立即失效：在途作业全部作废，按当前两份文件重新排队
                    repository.supersede_active_jobs(conn, business_date)
                    job_id = repository.enqueue_job(
                        conn, business_date, traffic["id"], ownership["id"])
                    row = repository.get_job(conn, job_id)
                    new_job = build_job_progress(conn, row)
        except UploadError:
            raise

        return file_id, new_job
    finally:
        dedup.remove()
        paths.cleanup()


def ingest_upload_bytes(
    conn: sqlite3.Connection,
    business_date: str,
    file_type: str,
    filename: str,
    raw: bytes,
) -> Tuple[int, Optional[JobProgress]]:
    """字节数组便捷封装（测试/小文件用），内部仍走同一套流式管道。"""
    return ingest_upload_stream(
        conn, business_date, file_type, filename, io.BytesIO(raw),
    )


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
