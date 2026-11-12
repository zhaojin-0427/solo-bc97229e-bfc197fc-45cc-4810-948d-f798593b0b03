"""对外 API 的响应模型。"""
from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class FileInfo(BaseModel):
    file_type: str = Field(..., description="traffic=流量文件，ownership=归属文件")
    version: int
    original_name: str
    size_bytes: int
    row_count: int
    sha256: str
    uploaded_at: str


class JobProgress(BaseModel):
    job_id: int
    business_date: str
    status: str = Field(..., description="queued/running/succeeded/failed/superseded")
    progress_percent: int
    progress_stage: str
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    missing_ownership: List[str] = []
    result_row_count: Optional[int] = None
    generation: int
    current: bool = Field(..., description="该作业是否对应当前两份文件")
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class DistrictResult(BaseModel):
    district: str
    cell_count: int
    uplink_bytes: int
    downlink_bytes: int
    total_bytes: int


class ResultResponse(BaseModel):
    """取数统一信封：未就绪时 ready=false 并明确说明原因。"""

    business_date: str
    ready: bool
    status: str = Field(..., description="ready/waiting_files/computing/failed/superseded")
    message: str
    job_id: Optional[int] = None
    generation: Optional[int] = None
    traffic_file: Optional[FileInfo] = None
    ownership_file: Optional[FileInfo] = None
    results: List[DistrictResult] = []
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    missing_ownership: List[str] = []


class UploadAck(BaseModel):
    """上传/替换回执。两份文件齐活时同时返回新作业进度。"""

    business_date: str
    file_type: str
    stored_file: FileInfo
    both_files_present: bool
    message: str
    job: Optional[JobProgress] = None


class DaySummary(BaseModel):
    business_date: str
    ready: bool
    status: str
    traffic_version: Optional[int] = None
    ownership_version: Optional[int] = None
    latest_job_id: Optional[int] = None


class HealthResponse(BaseModel):
    status: str
    database: str
