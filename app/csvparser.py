"""CSV 解析与校验（输入是不可信数据，全部按严格规则处理）。

规则：
- 允许 UTF-8（含 BOM）与 GB18030 编码，自动嗅探；
- 表头支持中英文列名；
- 基站ID 在单份文件内必须唯一；
- 字节数必须是非负整数；
- 不允许缺列、空值、负数或无法解析的行，任何问题整份拒收（不产生新版本）。
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Tuple

from .errors import UploadError

TRAFFIC_COLUMNS = {
    "cell_id": ["基站id", "基站编号", "cell_id", "cellid", "id"],
    "uplink": ["上行字节数", "上行字节", "上行流量", "uplink_bytes", "uplink", "ul_bytes"],
    "downlink": ["下行字节数", "下行字节", "下行流量", "downlink_bytes", "downlink", "dl_bytes"],
}

OWNERSHIP_COLUMNS = {
    "cell_id": ["基站id", "基站编号", "cell_id", "cellid", "id"],
    "district": ["片区", "片区名称", "区域", "district", "area", "zone"],
}

_ENCODINGS = ("utf-8-sig", "gb18030", "latin-1")


@dataclass
class ParsedCSV:
    header: List[str]
    rows: List[Tuple[str, ...]]  # 已按逻辑列顺序排列：traffic -> (cell, up, down)
    row_count: int


def _decode(raw: bytes) -> str:
    for enc in _ENCODINGS:
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    raise UploadError("UNSUPPORTED_ENCODING", "文件编码无法识别，请使用 UTF-8 或 GB18030 编码")


def _normalize(name: str) -> str:
    return name.strip().lstrip("﻿").lower().replace(" ", "")


def _resolve_header(fieldnames: Optional[Iterable[str]], spec: Dict[str, List[str]]) -> Dict[str, int]:
    if not fieldnames:
        raise UploadError("EMPTY_HEADER", "CSV 缺少表头行")
    normalized = {_normalize(f): idx for idx, f in enumerate(fieldnames)}
    resolved: Dict[str, int] = {}
    for logical, aliases in spec.items():
        for alias in aliases:
            key = _normalize(alias)
            if key in normalized:
                resolved[logical] = normalized[key]
                break
        if logical not in resolved:
            raise UploadError(
                "MISSING_COLUMN",
                f"缺少必需列 '{logical}'，可接受的列名：{', '.join(spec[logical])}",
            )
    return resolved


def _parse_non_negative_int(value: str, line_no: int, column: str) -> int:
    token = value.strip()
    if token == "":
        raise UploadError("INVALID_VALUE", f"第 {line_no} 行列 '{column}' 值为空，要求非负整数")
    # 只接受 ASCII 十进制整数，拒绝小数、正负号、十六进制、全角数字等写法
    if not token or not token.isascii() or not token.isdigit():
        raise UploadError("INVALID_VALUE", f"第 {line_no} 行列 '{column}' 的值 '{value}' 不是非负整数")
    result = int(token)
    if result < 0:  # 理论上 isdigit 已排除，防御性保留
        raise UploadError("INVALID_VALUE", f"第 {line_no} 行列 '{column}' 出现负数")
    return result


def parse_csv(raw: bytes, file_type: str) -> ParsedCSV:
    """解析并整份校验。成功返回结构化行，失败抛 UploadError，不产生部分数据。"""
    if not raw:
        raise UploadError("EMPTY_FILE", "上传文件为空")

    text = _decode(raw)
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise UploadError("EMPTY_FILE", "CSV 没有任何内容")

    spec = TRAFFIC_COLUMNS if file_type == "traffic" else OWNERSHIP_COLUMNS
    idx = _resolve_header(header, spec)

    logical_rows: List[Tuple[str, ...]] = []
    seen: Dict[str, int] = {}
    data_lines = 0

    for line_no, record in enumerate(reader, start=2):
        # 跳过完全空白的尾行（每个字段都为空）
        if len(record) == 0 or all((c is None or c.strip() == "") for c in record):
            continue
        data_lines += 1
        needed = max(idx.values()) + 1
        if len(record) < needed:
            raise UploadError("MALFORMED_ROW", f"第 {line_no} 行列数不足，期望至少 {needed} 列")

        cell = record[idx["cell_id"]].strip()
        if cell == "":
            raise UploadError("INVALID_VALUE", f"第 {line_no} 行基站ID为空")
        if cell in seen:
            raise UploadError(
                "DUPLICATE_CELL_ID",
                f"基站ID '{cell}' 在文件内重复（首次出现于第 {seen[cell]} 行，再次出现于第 {line_no} 行）",
            )

        if file_type == "traffic":
            uplink = _parse_non_negative_int(record[idx["uplink"]], line_no, "uplink_bytes")
            downlink = _parse_non_negative_int(record[idx["downlink"]], line_no, "downlink_bytes")
            logical_rows.append((cell, str(uplink), str(downlink)))
        else:
            district = record[idx["district"]].strip()
            if district == "":
                raise UploadError("INVALID_VALUE", f"第 {line_no} 行片区名称为空")
            logical_rows.append((cell, district))
        seen[cell] = line_no

    if data_lines == 0:
        raise UploadError("NO_DATA_ROWS", "CSV 只有表头，没有数据行")

    return ParsedCSV(header=[header[i] for i in sorted(idx.values())], rows=logical_rows, row_count=data_lines)


def iter_parsed_rows(stored_path: str) -> Iterable[Tuple[str, ...]]:
    """worker 重算时从落盘的已校验文件流式读取，避免大文件全量驻留内存。"""
    with open(stored_path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # 规范化后的表头
        for record in reader:
            if record and not all(c.strip() == "" for c in record):
                yield tuple(record)


# 让 mypy/静态检查器安心：保留可注入的进度回调类型
ProgressCallback = Optional[Callable[[int, str], None]]
