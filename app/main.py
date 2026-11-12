"""FastAPI 入口：上传、替换、作业进度、取数、交互式文档。

启动后访问：
- http://localhost:8000/        操作指引
- http://localhost:8000/docs   Swagger UI（可直接上传演示）
"""
from __future__ import annotations

import re
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, File, HTTPException, Path as PathParam, Query, UploadFile
from fastapi.responses import HTMLResponse

from . import config, repository, services, staging
from .db import connect, init_db
from .errors import UploadError
from .schemas import (
    DaySummary,
    FileInfo,
    HealthResponse,
    JobProgress,
    ResultResponse,
    UploadAck,
)
from .worker import worker

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_VALID_TYPES = {"traffic", "ownership"}


def _recover_persistent_state() -> None:
    """启动恢复：保证不出现“版本引用缺失文件”，并清掉未提交残留。

    1. 清空暂存目录：里面只有未提交的接收产物，绝不可能被正式库引用；
    2. 清扫正式目录中没有任何 files 行引用的孤儿文件（提交前崩溃的残留），
       已提交/历史版本文件一律保留；
    3. 体检：若仍有数据库引用指向缺失文件，记录错误便于运维介入
       （正常崩溃窗口下不会发生，因为引用与改名在同一事务内提交）。
    """
    staging.wipe_staging_dir()
    conn = connect()
    try:
        removed = staging.sweep_orphan_files(conn)
        for path in removed:
            print(f"[startup] 清理未提交的孤儿文件：{path}")
        missing = staging.check_referenced_files(conn)
        for path in missing:  # 正常流程不应出现：出现意味着卷被外部破坏
            print(f"[startup][ERROR] 数据库引用的文件缺失：{path}")
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # 启动：建表、崩溃恢复、恢复中断作业、拉起后台 worker
    init_db()
    _recover_persistent_state()
    if config.background_worker_enabled():
        worker.start()
    try:
        yield
    finally:
        worker.stop()


app = FastAPI(
    title="基站流量入仓服务",
    version="2.0.0",
    lifespan=lifespan,
    description=(
        "按业务日接收基站流量 CSV 与片区归属 CSV，两份到齐后后台按片区汇总。\n\n"
        "- 流式接收/解码/基站ID 查重/规范化写盘，内存占用只与块大小有关，不随总行数增长；\n"
        "- 校验规范化期间不占正式库写事务，其他业务日可正常上传；\n"
        "- 规范化文件完整持久化后，文件版本与重算作业在同一短事务共同提交，崩溃可恢复；\n"
        "- 任一文件可整份替换，替换成功后旧结果立即失效并自动重算；\n"
        "- 计算期间可继续上传与查询，迟到的旧作业不会发布结果；\n"
        "- 取数只返回当前两份文件对应的完整结果，未就绪时明确说明原因。"
    ),
)


def _validate_date(business_date: str) -> str:
    if not _DATE_RE.match(business_date):
        raise HTTPException(status_code=422, detail={
            "code": "INVALID_DATE",
            "message": "业务日格式必须为 YYYY-MM-DD，例如 2026-11-12",
        })
    # 进一步校验真实日历日期
    year, month, day = (int(x) for x in business_date.split("-"))
    import datetime as _dt
    try:
        _dt.date(year, month, day)
    except ValueError:
        raise HTTPException(status_code=422, detail={
            "code": "INVALID_DATE", "message": f"{business_date} 不是有效的日历日期",
        })
    return business_date


@app.exception_handler(UploadError)
def _upload_error_handler(_request, exc: UploadError):
    from fastapi.responses import JSONResponse
    return JSONResponse(status_code=400, content={
        "detail": {"code": exc.code, "message": exc.message, **exc.details},
    })


@app.get("/", include_in_schema=False)
def index() -> HTMLResponse:
    return HTMLResponse(
        """
        <!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
        <title>基站流量入仓服务</title>
        <style>body{font-family:system-ui,sans-serif;max-width:860px;margin:2rem auto;line-height:1.7}
        code{background:#f3f3f3;padding:2px 6px;border-radius:4px}</style></head>
        <body>
        <h1>基站流量入仓服务</h1>
        <p>按业务日接收流量 CSV 与归属 CSV，两份到齐后后台按片区分组求和。</p>
        <p>大文件友好：接收、解码、全文件基站ID 查重与规范化写盘全程流式，
        内存不随总行数增长；校验期间不占正式库写事务，文件版本与重算作业在
        规范化文件完整落盘后共同提交，进程终止重启可自动清理未提交暂存且不误删已提交文件。</p>
        <h2>演示步骤（在 <a href="/docs">Swagger UI</a> 中操作）</h2>
        <ol>
          <li><code>POST /days/2026-11-12/files/traffic</code> 上传流量 CSV</li>
          <li><code>POST /days/2026-11-12/files/ownership</code> 上传归属 CSV，后台自动开算</li>
          <li><code>GET /days/2026-11-12/results</code> 查看进度或取片区汇总结果</li>
          <li>再次 POST 任一端点即可整份替换，旧结果立即失效并自动重算</li>
          <li><code>GET /jobs/{job_id}</code> 查进度与失败原因；缺归属的作业会标记 failed</li>
        </ol>
        <p>完整接口定义见 <a href="/openapi.json">openapi.json</a>。</p>
        </body></html>
        """
    )


@app.get("/health", response_model=HealthResponse, tags=["运维"])
def health() -> HealthResponse:
    return HealthResponse(status="ok", database=str(config.db_path()))


@app.post(
    "/days/{business_date}/files/{file_type}",
    response_model=UploadAck,
    status_code=201,
    tags=["上传与替换"],
    summary="上传或整份替换某业务日的 CSV（traffic=流量，ownership=归属）",
    responses={400: {"description": "CSV 校验失败，整份拒收，现有版本不受影响"}},
)
def upload_file(
    business_date: str = PathParam(..., description="业务日 YYYY-MM-DD"),
    file_type: str = PathParam(..., description="traffic 或 ownership"),
    file: UploadFile = File(..., description="CSV 文件；基站ID列在单文件内唯一，字节数为非负整数"),
) -> UploadAck:
    _validate_date(business_date)
    if file_type not in _VALID_TYPES:
        raise HTTPException(status_code=422, detail={
            "code": "INVALID_FILE_TYPE",
            "message": f"file_type 必须是 {sorted(_VALID_TYPES)} 之一",
        })
    # 直接传递文件流：服务层分块落盘，内存不随文件大小/总行数增长
    conn = connect()
    try:
        _file_id, new_job = services.ingest_upload_stream(
            conn, business_date, file_type, file.filename or "upload.csv", file.file,
        )
        stored_row = repository.get_current_file(conn, business_date, file_type)
        both = (repository.get_current_file(conn, business_date, "traffic") is not None
                and repository.get_current_file(conn, business_date, "ownership") is not None)
    finally:
        conn.close()
    if new_job is not None:
        message = "文件已接收并生效，在途旧作业已作废，已按当前两份文件重新排队计算"
    else:
        other = "ownership（归属）" if file_type == "traffic" else "traffic（流量）"
        message = f"文件已接收并生效，尚缺 {other} 文件，两份到齐后自动开算"
    return UploadAck(
        business_date=business_date,
        file_type=file_type,
        stored_file=services._file_info(stored_row),
        both_files_present=both,
        message=message,
        job=new_job,
    )


@app.get(
    "/days/{business_date}/files",
    response_model=List[FileInfo],
    tags=["上传与替换"],
    summary="查看某业务日当前生效的两份文件版本",
)
def list_files(business_date: str) -> List[FileInfo]:
    _validate_date(business_date)
    conn = connect()
    try:
        rows = [
            repository.get_current_file(conn, business_date, "traffic"),
            repository.get_current_file(conn, business_date, "ownership"),
        ]
        return [services._file_info(r) for r in rows if r is not None]
    finally:
        conn.close()


@app.get(
    "/days/{business_date}/results",
    response_model=ResultResponse,
    tags=["取数"],
    summary="取当前两份文件对应的片区汇总；未就绪时明确说明状态与原因",
)
def get_results(business_date: str) -> ResultResponse:
    _validate_date(business_date)
    conn = connect()
    try:
        return services.build_result_response(conn, business_date)
    finally:
        conn.close()


@app.get(
    "/days",
    response_model=List[DaySummary],
    tags=["取数"],
    summary="列出所有有数据的业务日及其就绪状态",
)
def list_days() -> List[DaySummary]:
    conn = connect()
    try:
        return [services.build_day_summary(conn, d)
                for d in repository.list_business_dates(conn)]
    finally:
        conn.close()


@app.get(
    "/jobs/{job_id}",
    response_model=JobProgress,
    tags=["作业"],
    summary="查询单个作业的进度、失败原因与是否对应当前文件",
    responses={404: {"description": "作业不存在"}},
)
def get_job(job_id: int = PathParam(..., ge=1)) -> JobProgress:
    conn = connect()
    try:
        row = repository.get_job(conn, job_id)
        if row is None:
            raise HTTPException(status_code=404, detail={
                "code": "JOB_NOT_FOUND", "message": f"作业 {job_id} 不存在",
            })
        return services.build_job_progress(conn, row)
    finally:
        conn.close()


@app.get(
    "/jobs",
    response_model=List[JobProgress],
    tags=["作业"],
    summary="列出作业（可按业务日过滤），新作业在前",
)
def list_jobs(business_date: Optional[str] = Query(None, description="可选，按业务日过滤")) -> List[JobProgress]:
    if business_date is not None:
        _validate_date(business_date)
    conn = connect()
    try:
        return [services.build_job_progress(conn, r)
                for r in repository.list_jobs(conn, business_date)]
    finally:
        conn.close()
