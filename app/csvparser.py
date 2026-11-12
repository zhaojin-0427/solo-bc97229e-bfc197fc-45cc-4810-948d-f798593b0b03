"""CSV 流式解码、校验与规范化（输入是不可信数据，全部按严格规则处理）。

规则（与旧版完全一致）：
- 允许 UTF-8（含 BOM）与 GB18030 编码，自动嗅探；
- 表头支持中英文列名；
- 基站ID 在单份文件内必须唯一；
- 字节数必须是非负整数；
- 不允许缺列、空值、负数或无法解析的行，任何问题整份拒收（不产生新版本）。

流式保证：
- 输入是磁盘上的暂存原件，按块增量解码，从不把整份文件或全部行读进内存；
- 基站ID 查重由调用方提供的磁盘后端（``dedup``）完成，内存中不保存全部 ID；
- 规范化结果逐行写入调用方提供的输出句柄，内存占用只与单行/单块大小有关，
  不随总行数增长。
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Callable, IO, Iterable, List, Optional, Protocol, Tuple

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

# 仅接受需求允许的两种编码；utf-8-sig 自动吃掉 BOM。
# 解码失败再整体用 GB18030 重试（GB18030 是定宽/变宽兼容编码，合法文件总能解出）。
_ENCODINGS = ("utf-8-sig", "gb18030")


class DedupBackend(Protocol):
    """基站ID 查重后端（磁盘实现，内存不随 ID 数量增长）。"""

    def prepare(self) -> None: ...

    def seen_line(self, cell_id: str, line_no: int) -> Optional[int]:
        """返回该 ID 首次出现的行号；未见过返回 None 并登记当前行。"""


def _normalize(name: str) -> str:
    return name.strip().lstrip("﻿").lower().replace(" ", "")


def _resolve_header(fieldnames: Optional[Iterable[str]], spec: dict) -> dict:
    if not fieldnames:
        raise UploadError("EMPTY_HEADER", "CSV 缺少表头行")
    normalized = {_normalize(f): idx for idx, f in enumerate(fieldnames)}
    resolved: dict = {}
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


@dataclass
class IngestStats:
    """一次流式接收+校验+规范化的累计结果（只保存计数，不保存行内容）。"""

    row_count: int = 0
    header: Optional[List[str]] = None


def _open_decoded(raw_path: str, encoding: str) -> Tuple[IO[str], io.TextIOWrapper]:
    # newline="" 让 csv 模块自己处理 \r\n / \n，与旧版 StringIO 行为一致
    binary = open(raw_path, "rb")
    text = io.TextIOWrapper(binary, encoding=encoding, newline="", errors="strict")
    return binary, text


def stream_ingest(
    raw_path: str,
    file_type: str,
    out_handle: IO[str],
    dedup: DedupBackend,
) -> IngestStats:
    """从磁盘暂存原件流式解码、整份校验，并把规范化行逐行写入 ``out_handle``。

    - 任何规则不满足都抛 :class:`UploadError`，由调用方丢弃本次暂存，正式数据不动；
    - 成功时 ``out_handle`` 内是 UTF-8、逻辑列顺序的规范化 CSV（含表头）；
    - 全程不缓存整份文本或全部行；基站ID 查重走 ``dedup`` 磁盘后端。
    """
    spec = TRAFFIC_COLUMNS if file_type == "traffic" else OWNERSHIP_COLUMNS
    last_error: Optional[Exception] = None

    for encoding in _ENCODINGS:
        binary: Optional[IO[bytes]] = None
        text: Optional[IO[str]] = None
        try:
            binary, text = _open_decoded(raw_path, encoding)
            reader = csv.reader(text)
            try:
                header = next(reader)
            except StopIteration:
                raise UploadError("EMPTY_FILE", "CSV 没有任何内容")

            idx = _resolve_header(header, spec)
            logical_header = [header[i] for i in sorted(idx.values())]
            needed = max(idx.values()) + 1

            # 每次编码尝试都是一趟全新校验：重置查重后端与规范化输出
            dedup.prepare()
            out_handle.seek(0)
            out_handle.truncate(0)
            writer = csv.writer(out_handle)
            writer.writerow(logical_header)

            stats = IngestStats(header=logical_header)
            for line_no, record in enumerate(reader, start=2):
                # 跳过完全空白的尾行（每个字段都为空）
                if len(record) == 0 or all((c is None or c.strip() == "") for c in record):
                    continue
                stats.row_count += 1
                if len(record) < needed:
                    raise UploadError(
                        "MALFORMED_ROW",
                        f"第 {line_no} 行列数不足，期望至少 {needed} 列",
                    )

                cell = record[idx["cell_id"]].strip()
                if cell == "":
                    raise UploadError("INVALID_VALUE", f"第 {line_no} 行基站ID为空")
                first_line = dedup.seen_line(cell, line_no)
                if first_line is not None:
                    raise UploadError(
                        "DUPLICATE_CELL_ID",
                        f"基站ID '{cell}' 在文件内重复（首次出现于第 {first_line} 行，再次出现于第 {line_no} 行）",
                    )

                if file_type == "traffic":
                    uplink = _parse_non_negative_int(record[idx["uplink"]], line_no, "uplink_bytes")
                    downlink = _parse_non_negative_int(record[idx["downlink"]], line_no, "downlink_bytes")
                    writer.writerow((cell, str(uplink), str(downlink)))
                else:
                    district = record[idx["district"]].strip()
                    if district == "":
                        raise UploadError("INVALID_VALUE", f"第 {line_no} 行片区名称为空")
                    writer.writerow((cell, district))

            if stats.row_count == 0:
                raise UploadError("NO_DATA_ROWS", "CSV 只有表头，没有数据行")
            out_handle.flush()
            return stats
        except UnicodeError as exc:
            # 当前编码解不动（TextIOWrapper 可能抛 UnicodeDecodeError 等 UnicodeError）：
            # 留证并整体换下一编码重试（输出与查重已会重置）
            last_error = exc
            continue
        finally:
            if text is not None:
                text.detach()  # 解除对 binary 的包装，避免重复关闭
            if binary is not None:
                binary.close()

    raise UploadError(
        "UNSUPPORTED_ENCODING",
        f"文件编码无法识别，请使用 UTF-8（含 BOM）或 GB18030 编码：{last_error!r}",
    )


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
