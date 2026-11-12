"""后台计算 worker。

并发与陈旧控制：
- 单 worker 线程 + 数据库原子认领（claim），同一作业不会被计算两次；
- 进度写入时若作业已被新上传取代，立即中止（迟到的旧任务不得发布结果）；
- 结果发布在一个事务内并再次确认作业仍是 running，取代发生在最后一刻也会丢弃结果；
- 服务重启时 running 作业重置为 queued 接续执行，结果无半成品。
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from . import config, repository
from .csvparser import iter_parsed_rows
from .db import connect
from .errors import JobFailure

# 每处理多少行刷新一次进度（同时也是检查“是否已被取代”的频率）
_PROGRESS_EVERY = 5000
# 失败原因里最多回显的缺归属基站数量，避免超大 JSON
_MISSING_SAMPLE_LIMIT = 500


class JobWorker:
    def __init__(self) -> None:
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # ---------- 生命周期 ----------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="job-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        conn = connect()
        try:
            # 重启接续：上次 running 中断的作业重新排队
            repository.reset_interrupted_jobs(conn)
            while not self._stop.is_set():
                worked = self.run_pending_once(conn)
                if worked:
                    continue
                self._stop.wait(config.worker_poll_interval())
        finally:
            conn.close()

    # ---------- 核心步骤（也供测试同步驱动） ----------

    def run_pending_once(self, conn=None) -> bool:
        """处理一个排队作业。返回 True 表示处理过一个作业。"""
        own_conn = conn is None
        if own_conn:
            conn = connect()
        try:
            job = repository.claim_next_job(conn)
            if job is None:
                return False
            try:
                self._process(conn, job)
            except JobFailure as exc:
                # 作业在计算期间被取代：状态已由上传事务改为 superseded
                if exc.code != "SUPERSEDED":
                    repository.fail_job(conn, job["id"], exc.code, exc.message, exc.missing_ownership)
            except Exception as exc:  # 防御：任何意外都要落失败原因，不能卡死队列
                repository.fail_job(conn, job["id"], "INTERNAL_ERROR", f"内部错误：{exc!r}")
            return True
        finally:
            if own_conn:
                conn.close()

    def _process(self, conn, job) -> None:
        job_id = job["id"]
        traffic = repository.get_file_by_id(conn, job["traffic_file_id"])
        ownership = repository.get_file_by_id(conn, job["ownership_file_id"])

        # 兜底：若当前文件已不是作业对应的两份，直接作废不发布
        cur_traffic = repository.get_current_file(conn, job["business_date"], "traffic")
        cur_ownership = repository.get_current_file(conn, job["business_date"], "ownership")
        if (cur_traffic is None or cur_ownership is None
                or cur_traffic["id"] != traffic["id"]
                or cur_ownership["id"] != ownership["id"]):
            conn.execute(
                "UPDATE jobs SET status='superseded', "
                "progress_stage='文件已被替换，作业作废', "
                "finished_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=? AND status='running'",
                (job_id,),
            )
            return

        repository.update_progress(conn, job_id, 5, "读取归属文件")
        ownership_map: Dict[str, str] = {}
        for cell, district in iter_parsed_rows(ownership["stored_path"]):
            ownership_map[cell] = district

        repository.update_progress(conn, job_id, 15, "按片区分组汇总")
        agg: Dict[str, List[int]] = defaultdict(lambda: [0, 0, 0])  # district -> [cells, up, down]
        missing: List[str] = []
        total = max(traffic["row_count"], 1)
        processed = 0

        for cell, up_str, down_str in iter_parsed_rows(traffic["stored_path"]):
            district = ownership_map.get(cell)
            if district is None:
                missing.append(cell)
            else:
                bucket = agg[district]
                bucket[0] += 1
                bucket[1] += int(up_str)
                bucket[2] += int(down_str)
            processed += 1
            if processed % _PROGRESS_EVERY == 0:
                percent = 15 + int(80 * processed / total)
                repository.update_progress(conn, job_id, min(percent, 95), "按片区分组汇总")

        if missing:
            # 缺少归属则整次失败：不写任何结果，错误原因可查询
            sample = missing[:_MISSING_SAMPLE_LIMIT]
            more = len(missing) - len(sample)
            message = (
                f"有 {len(missing)} 个基站在归属文件中找不到片区，整次计算失败"
                + (f"（仅回显前 {_MISSING_SAMPLE_LIMIT} 个）" if more > 0 else "")
            )
            repository.fail_job(conn, job_id, "MISSING_OWNERSHIP", message, sample)
            return

        repository.update_progress(conn, job_id, 97, "写入片区结果")
        district_rows: List[Tuple] = [
            (district, cells, uplink, downlink, uplink + downlink)
            for district, (cells, uplink, downlink) in sorted(agg.items())
        ]
        published = repository.complete_job(conn, job_id, district_rows)
        if not published:
            # 发布瞬间被取代：结果随事务回滚丢弃，符合“迟到的旧任务不得发布结果”
            raise JobFailure("SUPERSEDED", "作业已被取代，结果未发布")


worker = JobWorker()
