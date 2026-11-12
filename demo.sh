#!/usr/bin/env bash
# 端到端演示脚本：上传 -> 取数 -> 替换重算 -> 缺归属失败 -> 进度/失败原因查询。
# 用法：先启动服务（docker compose up 或本地 uvicorn），再执行 bash demo.sh
set -euo pipefail

BASE="${BASE:-http://localhost:8000}"
DAY="${DAY:-2026-11-12}"
pp() {
  # 先缓存输入：JSON 能美化就美化，否则原样输出（如错误响应末尾的 HTTP 状态行）
  local text
  text="$(cat)"
  printf '%s' "$text" | python3 -m json.tool 2>/dev/null || printf '%s\n' "$text"
}

echo "############ 0. 健康检查 ############"
curl -s "$BASE/health"; echo

echo; echo "############ 1. 空业务日查询（明确告知未就绪） ############"
curl -s "$BASE/days/$DAY/results" | pp

echo; echo "############ 2. 上传流量 CSV ############"
curl -s -X POST "$BASE/days/$DAY/files/traffic" \
  -F "file=@samples/traffic_2026-11-12.csv" | pp

echo; echo "############ 3. 上传归属 CSV（两份到齐，后台自动开算） ############"
curl -s -X POST "$BASE/days/$DAY/files/ownership" \
  -F "file=@samples/ownership_2026-11-12.csv" | pp

sleep 1
echo; echo "############ 4. 取片区汇总结果 ############"
curl -s "$BASE/days/$DAY/results" | pp

echo; echo "############ 5. 整份替换流量文件 -> 旧结果失效、自动重算 ############"
curl -s -X POST "$BASE/days/$DAY/files/traffic" \
  -F "file=@samples/traffic_2026-11-12_v2_replacement.csv" | pp
sleep 1
curl -s "$BASE/days/$DAY/results" | pp

echo; echo "############ 6. 作业进度与历史 ############"
curl -s "$BASE/jobs?business_date=$DAY" | pp

echo; echo "############ 7. 缺归属 -> 整次失败，原因可查询 ############"
BAD_DAY="2026-11-13"
curl -s -X POST "$BASE/days/$BAD_DAY/files/traffic" \
  -F "file=@samples/traffic_missing_ownership.csv" >/dev/null
curl -s -X POST "$BASE/days/$BAD_DAY/files/ownership" \
  -F "file=@samples/ownership_2026-11-12.csv" >/dev/null
sleep 1
curl -s "$BASE/days/$BAD_DAY/results" | pp

echo; echo "############ 8. 坏文件整份拒收（HTTP 400，旧版本不动） ############"
curl -s -w "\nHTTP %{http_code}\n" -X POST "$BASE/days/$DAY/files/traffic" \
  -F "file=@samples/traffic_invalid_values.csv" | pp

echo; echo "############ 9. 业务日总览 ############"
curl -s "$BASE/days" | pp
