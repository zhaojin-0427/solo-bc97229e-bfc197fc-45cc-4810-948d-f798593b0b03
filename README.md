# 基站流量入仓服务（FastAPI + SQLite）

网络容量数据工程师的入仓工具：按**业务日**接收运营商每日基站流量 CSV 与片区归属 CSV，
两份到齐后在**后台按片区分组求和**，供扩容分析读取。

## 能力对照（需求 → 实现）

| 需求 | 实现方式 |
| --- | --- |
| 按业务日上传流量/归属两类 CSV | `POST /days/{业务日}/files/{traffic\|ownership}`，文件内基站ID唯一、字节数为非负整数，校验不过整份拒收（HTTP 400，旧版本不受影响） |
| 有限内存接收大文件，内存不随总行数增长 | 接收分块（默认 1 MiB）落盘暂存，`TextIOWrapper` 增量解码、`csv.reader` 逐行校验、规范化结果逐行写盘；常驻内存只有一个块+单行，与总行数无关 |
| 全文件基站ID 查重不把全部 ID 放进内存 | 磁盘 SQLite `WITHOUT ROWID` 主键表（位于暂存目录）承载全部 ID，约 2 MB 页缓存上限，查重压力落在磁盘 |
| 保留 UTF-8（含 BOM）/GB18030、中英文表头、原校验规则 | 编码两趟嗅探（`utf-8-sig` 失败再整体 `gb18030` 重试）；表头别名、错误码、非负整数/唯一/非空等规则与旧版完全一致 |
| 校验规范化期间不占正式 SQLite 写事务 | 接收/解码/查重/写盘期间零正式库写事务，其他业务日上传不受影响；仅在规范化文件 fsync 后开一个短事务 |
| 末行重复、超限、写盘失败时文件/作业/结果不变 | 失败只清理本次暂存，不产生 files/jobs 行；超限在流式接收时逐块判定即停 |
| 规范化文件完整持久化后才共同提交版本与重算作业 | 规范化临时文件 `flush+fsync` 后，在同一短事务内 `os.replace` 改名 + 插入 files 版本 + 作废旧作业 + 入队重算作业 |
| 提交前后进程终止，重启无引用缺失文件且能清暂存、不误删已提交 | 暂存目录与正式目录分离；启动清空暂存、清扫“无 files 行引用”的孤儿文件、体检悬空引用；已提交/历史版本一律保留 |
| 两份到齐后后台按片区求和 | 后台 worker 线程轮询队列，原子认领作业，按 `片区 → 基站数/上行/下行/合计` 汇总 |
| 缺少归属则整次失败 | 流量中任一基站在归属文件中找不到片区，作业标记 `failed`，不写任何结果，失败原因与缺失基站清单可查 |
| 任一文件可整份替换 | 同一端点再次上传即生成新版本（version 递增，原始文件与哈希全部留痕） |
| 替换成功后旧结果立即失效并自动重算 | 上传事务内把在途作业置 `superseded` 并按当前文件对入队新作业；取数只认当前文件对的最新作业，永不返回陈旧结果 |
| 计算期间仍可上传和查询 | SQLite WAL + 短事务，查询不阻塞；未就绪返回 `ready=false` 与明确状态（`waiting_files`/`computing`/`failed`） |
| 迟到的旧任务不得发布结果 | 进度更新与结果发布两道检查：作业一旦被取代即中止；发布事务内二次确认仍为 `running`，否则丢弃结果 |
| 输入、作业、结果持久化，重启接续 | 全部落在数据卷 `/data`（SQLite + 规范化 CSV 原件 + 独立暂存区）；启动时残留 `running` 作业重置为 `queued` 重算 |
| 进度和失败原因查询 | `GET /jobs/{id}`、`GET /jobs`，含百分比、阶段、错误码、错误信息、缺失基站清单 |
| Docker Compose 可实际启动 | 见下方“快速启动”，含健康检查与命名卷 |
| 交互式 API 文档演示 | 启动后访问 `http://localhost:8000/docs`（Swagger UI） |

## 流式上传与崩溃安全设计（本次改造重点）

```
上传流
  │ 分块读取（UPLOAD_CHUNK_BYTES，默认 1 MiB）
  ▼
暂存原件 staging/upload_*.raw ── 边读边累计 SHA-256、字节数（超限即拒收）
  │ TextIOWrapper 增量解码（UTF-8 BOM → GB18030 两趟）
  ▼
逐行 csv 校验 ── 基站ID 查重写磁盘 SQLite（staging/upload_*.dedup.db）
  │ 规范化行逐行写出
  ▼
规范化临时文件 staging/upload_*.normalized.tmp（flush + fsync 完整持久化）
  │
  ▼ 短事务（BEGIN IMMEDIATE）：
       os.replace → 正式目录 files/<业务日>/...csv（目录 fsync）
       INSERT files 版本 + 作废旧作业 + INSERT 重算作业
       COMMIT（任一步失败整体 ROLLBACK，正式数据不变）
  ▼
清理本次暂存
```

要点：

- **内存有界**：整条链路不缓存整份字节/文本，不持有全部行或全部 ID；
  查重用独立磁盘 SQLite，Python 堆峰值与总行数无关（20 万行实测增量约 2 MB）。
- **不占正式库写事务**：耗时的接收、解码、查重、规范化写盘全部在正式库写事务之外完成，
  其他业务日上传/查询不被阻塞；正式库只在最后的毫秒级短事务内被写入。
- **共同提交 + 崩溃恢复**：文件改名与版本/作业记录在同一事务，要么都可见要么都不可见。
  - 暂存目录 `staging/` 只放未提交产物，启动整体清空，绝不会被正式库引用；
  - 正式目录 `files/` 仅清扫“数据库中无任何 files 行引用”的孤儿文件，
    已提交文件与全部历史版本绝不删除；
  - 启动体检若发现数据库引用指向缺失文件（正常窗口不会发生）会打印 `[startup][ERROR]`。


## 快速启动

```bash
docker compose up --build
# 服务： http://localhost:8000
# 文档： http://localhost:8000/docs   （可直接在页面上上传文件演示）
# 指引： http://localhost:8000/
```

数据保存在命名卷 `warehouse-data`（容器内 `/data`），`docker compose down` 后再 `up` 数据仍在；
如需清空：`docker compose down -v`。

### 本地非 Docker 运行（开发/测试）

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
DATA_DIR=./data uvicorn app.main:app --reload
pytest -q
```

### 一键演示（服务启动后另开终端）

```bash
bash demo.sh
```

## 交互式文档演示步骤（Swagger UI）

1. `POST /days/{business_date}/files/traffic`，`business_date` 填 `2026-11-12`，
   `file` 选择 `samples/traffic_2026-11-12.csv`，Execute；
2. 同日用 `samples/ownership_2026-11-12.csv` 调 `.../files/ownership`，响应里能看到新作业已 `queued`；
3. `GET /days/2026-11-12/results` 查看结果（计算中会明确返回 `computing` 与进度）；
4. 再次调 traffic 上传，选择 `samples/traffic_2026-11-12_v2_replacement.csv` —— 旧作业立即变
   `superseded`、新作业自动重算，取数只返回新结果；
5. `GET /jobs/{job_id}` 查看任意作业进度；上传 `samples/traffic_missing_ownership.csv`
   可看到 `MISSING_OWNERSHIP` 失败原因与缺失基站清单。

## CSV 格式

- 编码：UTF-8（含 BOM）或 GB18030，自动嗅探；
- 流量文件列（中英文表头均可）：`基站ID/cell_id`、`上行字节数/uplink_bytes`、`下行字节数/downlink_bytes`；
- 归属文件列：`基站ID/cell_id`、`片区/district`；
- 字节数仅接受十进制非负整数（`0` 合法，负数、小数、空值、非数字一律拒收）；
- 单文件内基站ID必须唯一，重复即整份拒收；空文件、仅表头、缺列同样拒收。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/days/{business_date}/files/{file_type}` | 上传或整份替换（201；校验失败 400 且不影响现有版本） |
| GET | `/days/{business_date}/files` | 当前生效的两份文件版本信息 |
| GET | `/days/{business_date}/results` | **取数**：统一信封，`ready=true` 才带完整片区结果，否则明确说明原因 |
| GET | `/days` | 所有业务日及其就绪状态 |
| GET | `/jobs/{job_id}` | 单作业进度/失败原因/是否对应当前文件 |
| GET | `/jobs?business_date=...` | 作业列表（可按日过滤） |
| GET | `/health` | 健康检查 |

取数响应状态：`ready`（就绪）、`waiting_files`（文件未到齐）、`computing`（计算中，附进度）、
`failed`（失败，附原因）、`superseded`（作业已被取代）。

## 数据模型与并发设计

```
files     每个 (业务日, 文件类型) 的版本链，保存存储路径、行数、SHA-256、上传时间
jobs      作业：状态机 queued → running → succeeded / failed / superseded，含进度与错误信息
results   成功作业的片区汇总（作业内片区唯一），与作业同事务提交
```

要点：

- **WAL 模式 + busy_timeout**：worker 写库时 API 查询不阻塞；
- **原子认领**：`UPDATE ... WHERE status='queued'` 在 `BEGIN IMMEDIATE` 事务内完成，单 worker 模型下作业不会被重复计算；
- **世代（generation）单调递增**：同一业务日每次触发都产生新一代，取数只认当前文件对的最新一代，
  历史成功作业保留审计但 `current=false`；
- **两道陈旧防护**：① 上传替换立即作废旧在途作业，worker 下次进度写入即感知并中止；
  ② 发布结果事务内二次确认状态，取代发生在最后一刻也整体回滚，迟到结果绝不落库；
- **重启恢复**：启动时先清空暂存目录、清扫正式目录中的未引用孤儿文件（已提交文件保留），
  再把残留 `running` 作业统一回到 `queued` 从头重算（输入文件已持久化），不存在半成品结果；
- **大文件友好**：上传分块落盘、增量解码、磁盘 SQLite 查重、规范化逐行写盘，
  内存占用平稳，与总行数无关；worker 流式逐行读取并按 5000 行刷新进度。

## 目录结构

```
app/            FastAPI 应用（config/csvparser/staging/db/repository/services/worker/main）
  staging.py    暂存区生命周期、磁盘 SQLite 查重、启动崩溃恢复（清暂存/扫孤儿/体检）
  csvparser.py  流式解码、逐行校验、规范化写盘（保留全部表头别名与错误码）
tests/          pytest 用例（主流程、失败路径、并发陈旧、重启接续、编码兼容、真实线程、
                流式内存/磁盘查重/写盘失败/崩溃恢复专项）
samples/        演示用 CSV（含替换版、缺归属、重复ID、非法值样本）
Dockerfile      python:3.11-slim 镜像，含健康检查
docker-compose.yml  单服务 + 命名卷 warehouse-data（含 MAX_UPLOAD_BYTES/UPLOAD_CHUNK_BYTES）
demo.sh         端到端 curl 演示脚本
```

数据卷 `/data` 内部分区：

```
/data/warehouse.db        正式 SQLite（files/jobs/results）
/data/files/<业务日>/      已提交的规范化 UTF-8 CSV（仅这些文件可被 files 行引用）
/data/staging/            未提交暂存（*.raw 原件、*.normalized.tmp、*.dedup.db），启动即清空
```

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest -q          # 39 passed
```
