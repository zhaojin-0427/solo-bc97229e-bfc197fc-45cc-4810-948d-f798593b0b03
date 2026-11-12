"""领域异常与 HTTP 错误响应构造。"""
from __future__ import annotations

from typing import Any, Dict, Optional


class UploadError(Exception):
    """上传解析/校验失败：整份拒收，不替换现有文件。"""

    def __init__(self, code: str, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


class JobFailure(Exception):
    """作业计算失败（如缺少归属），需要落库为 failed 状态。"""

    def __init__(self, code: str, message: str, missing_ownership: Optional[list[str]] = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.missing_ownership = missing_ownership or []
