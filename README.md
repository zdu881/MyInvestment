# MyInvestment

一个以“人工审核优先”为核心约束的投资代理系统，包含：
- CLI 交易决策流水线（选股、研究、提案、审核、执行）
- 调度与运维能力（告警、质量反馈、行动中心、技能沉淀）
- FastAPI + 原生前端 WebUI（中英切换）

项目目标不是“全自动交易”，而是把高频的巡检、审核、执行流程标准化并可审计。

## 1. 核心能力

- 多阶段运行：`preopen` / `intraday` / `postclose` / `all`
- 人工审核闭环：`approve` / `hold` / `reject`
- 执行闭环：支持 `dry-run` 与 `force`，执行后状态与产物回写
- 运维可观测：`ops_report`、`alerts`、`action_center`、`quality_feedback`
- 技能沉淀：候选技能收集、晋升与版本化注册
- WebUI 一站式入口：Action Center -> Review -> Execution -> Runs -> Ops
- 前端 i18n：`zh-CN` / `en-US`，语言切换持久化
- Agent 交互栏：`ask` / `plan` / `operation` 三模式（含 operation 预览与执行）

## 2. 项目结构

```text
MyInvestment/
├── agent_system.py                 # 主运行入口（多阶段）
├── agent_scheduler.py              # 调度入口（--once）
├── agent_review.py                 # 人工审核命令
├── agent_execute.py                # 执行命令
├── agent_ops_report.py             # 运维报告
├── agent_alerts.py                 # 告警生成
├── agent_action_center.py          # 行动中心聚合
├── agent_feedback.py               # 质量反馈生成
├── agent_skill_manager.py          # 技能晋升
├── agent_config.json               # 运行配置
├── state/                          # 状态文件（队列、快照、反馈、审计）
├── runs/                           # 每次 run 与 ops 产物
├── knowledge/                      # 技能与知识沉淀
├── webapi/                         # FastAPI
├── webui/static/                   # 原生前端
│   ├── index.html
│   ├── app.js
│   ├── styles.css
│   └── locales/
│       ├── zh-CN.json
│       └── en-US.json
├── tests/
├── scripts/
│   ├── start_webui.sh
│   ├── run_tests.sh
│   └── run_webui_e2e.sh
└── docs/webui/
```

## 3. 运行环境

推荐：
- Python `3.10+`
- Linux/macOS（项目当前主要在 Linux 环境验证）

核心 Python 依赖（按代码实际使用）：
- `fastapi`
- `uvicorn`
- `pydantic`
- `pandas`
- `requests`
- `pytest`
- 可选：`playwright`（浏览器 E2E）

业务数据源相关（按策略脚本需要，非 WebAPI 必需）：
- `akshare`
- `baostock`
- `lixinger_openapi`

如果你已有 conda 环境（例如 `adri`），可直接在该环境安装所需依赖。

## 4. 快速开始

### 4.1 启动 WebUI + API

```bash
./scripts/start_webui.sh
```

默认监听：
- `HOST=0.0.0.0`
- `PORT=8787`

打开：`http://localhost:8787/`

### 4.2 最小 CLI 流程

```bash
# 1) 全流程演练（不改状态）
python3 agent_system.py --phase all --dry-run

# 2) 查看待审核提案后提交审核
python3 agent_review.py --decision approve --run-id <RUN_ID> --reviewer your_name --note "manual approved"

# 3) 执行（建议先 dry-run）
python3 agent_execute.py --run-id <RUN_ID> --executor your_name --dry-run
```

### 4.3 调度一次

```bash
python3 agent_scheduler.py --once
```

常用变体：

```bash
python3 agent_scheduler.py --once --dry-run
python3 agent_scheduler.py --once --skip-maintenance --skip-ops-report
python3 agent_scheduler.py --once --skip-feedback --skip-skill-promotion
python3 agent_scheduler.py --once --skip-alerts
python3 agent_scheduler.py --once --skip-notifier
```

## 5. 常用命令清单

### 5.1 运行阶段

```bash
python3 agent_system.py --phase preopen
python3 agent_system.py --phase intraday
python3 agent_system.py --phase postclose
python3 agent_system.py --phase all
```

### 5.2 审核与执行

```bash
python3 agent_review.py --decision approve --run-id <RUN_ID> --reviewer your_name --note "..."
python3 agent_review.py --decision hold --run-id <RUN_ID> --reviewer your_name --note "..."
python3 agent_review.py --decision reject --run-id <RUN_ID> --reviewer your_name --note "..."

python3 agent_execute.py --run-id <RUN_ID> --executor your_name
python3 agent_execute.py --run-id <RUN_ID> --executor your_name --dry-run
python3 agent_execute.py --run-id <RUN_ID> --executor your_name --force
```

### 5.3 运维与反馈

```bash
python3 agent_ops_report.py --days 7
python3 agent_alerts.py
python3 agent_action_center.py
python3 agent_notifier.py --enabled --ntfy-enabled --ntfy-topic <YOUR_TOPIC>
python3 agent_feedback.py --days 30
python3 agent_queue_maintenance.py --dry-run
python3 agent_queue_maintenance.py
python3 agent_skill_manager.py
```

## 6. WebAPI 与前端

### 6.1 环境变量

`webapi.settings.AppSettings` 支持以下变量：

- `MYINVEST_ROOT`：项目根目录（默认当前目录）
- `MYINVEST_RUNS_ROOT`：`runs` 相对路径（默认 `runs`）
- `MYINVEST_STATE_ROOT`：`state` 相对路径（默认 `state`）
- `MYINVEST_KNOWLEDGE_ROOT`：`knowledge` 相对路径（默认 `knowledge`）
- `MYINVEST_CONFIG_PATH`：配置文件路径（默认 `agent_config.json`）
- `MYINVEST_COMMAND_TIMEOUT_SEC`：命令超时（默认 `120`，最小 `10`）
- `MYINVEST_API_TOKEN`：写接口 Token（未设置则不鉴权）


- `MYINVEST_NTFY_TOPIC`：ntfy topic（可覆盖配置中的 topic）
- `MYINVEST_NTFY_BASE_URL`：ntfy server 地址（默认 `https://ntfy.sh`）

### 6.2 API 路由

读接口：
- `GET /health`
- `GET /`
- `GET /api/action-center`
- `GET /api/ops/report`
- `GET /api/alerts`
- `GET /api/alerts/events`
- `GET /api/quality/latest`
- `GET /api/agent/operations`
- `GET /api/agent/operations/history`
- `GET /api/runs`
- `GET /api/runs/{run_id}`
- `GET /api/runs/{run_id}/artifacts`
- `GET /api/runs/{run_id}/artifact-content`
- `GET /api/proposals/pending`
- `GET /api/proposals/{run_id}`
- `GET /api/executions/pending`
- `GET /api/config`

写接口（支持 Token 鉴权）：
- `POST /api/reviews/{run_id}`
- `POST /api/executions/{run_id}`
- `POST /api/scheduler/once`
- `POST /api/agent/interact`
- `PATCH /api/config`

`POST /api/agent/interact` 在 `operation` 模式下为两步执行：
1. 先 `confirm=false` 预览并获取 `confirmation_id`
2. 再 `confirm=true` 且携带 `confirmation_id` 执行

同时同一 `operation_id + options` 在短时间内有冷却保护（默认 30 秒）。

### 6.3 审计

所有写操作会写入：
- `state/webui_audit_log.jsonl`

记录字段包括：
- 时间戳
- action 名称
- payload
- command / exit_code / stdout_tail / stderr_tail（若有命令执行）

## 7. 测试

### 7.1 常规测试

```bash
./scripts/run_tests.sh
```

覆盖：
- WebAPI 读写端点
- 配置 patch
- 审计日志写入
- 前端 i18n key 一致性与引用完整性

### 7.2 浏览器 E2E（可选）

```bash
pip install playwright
python -m playwright install chromium
./scripts/run_webui_e2e.sh
```

E2E 用例：
- 打开 WebUI
- 切换语言 `zh-CN -> en-US`
- 校验关键文案更新
- 刷新后校验语言持久化

若未安装 Playwright，该测试会自动 `skip`。

## 8. 前端 i18n 说明

- 字典位置：
  - `webui/static/locales/zh-CN.json`
  - `webui/static/locales/en-US.json`
- 前端启动时会加载默认语言字典；切换语言时按需加载
- 语言状态保存在 `localStorage`：`myinvestment_locale`
- `index.html` 使用 `data-i18n` 与 `data-i18n-placeholder` 标记
- 自动化守护：
  - `tests/test_webui_i18n.py` 确保中英 key 集一致
  - 确保 HTML 与 JS 引用到的 key 都在字典中

## 9. 关键产物说明

每次运行的关键输出位于 `runs/{trading_date}/{run_id}/`，常见包括：
- `run_manifest.json`
- `allocation_proposal.json`
- `advice_report.md`
- `rebalance_actions.csv`
- `execution_orders.csv`
- `execution_result.json`
- `portfolio_change_report.md`

运维聚合输出位于 `runs/ops/`，常见包括：
- `ops_report_latest.json` / `.md`
- `alerts_latest.json` / `.md`
- `action_center_latest.json` / `.md`
- `proposal_quality_latest.json` / `.md`

状态与队列位于 `state/`，常见包括：
- `review_queue.jsonl`
- `execution_queue.jsonl`
- `alerts_events.jsonl`
- `model_feedback.json`
- `webui_audit_log.jsonl`

## 10. 安全与治理建议

当前实现：
- `MYINVEST_API_TOKEN` 控制写接口
- 写操作均有审计日志
- 提案/执行前会检查是否仍为 pending

建议在生产化时补强：
- 角色权限（viewer/reviewer/executor/admin）
- 反向代理层鉴权与访问控制
- 更细粒度命令白名单与速率限制

## 11. 常见问题

### Q1: WebUI 打开为空或接口 404

检查：
- 是否在项目根目录启动：`./scripts/start_webui.sh`
- `runs/ops/*.json` 是否存在（例如 `action_center_latest.json`）

### Q2: 写接口返回 401

检查：
- 是否设置了 `MYINVEST_API_TOKEN`
- 前端是否在右上角保存了对应 Token
- Header 是否携带 `X-API-Token`

### Q3: E2E 运行跳过

原因通常是未安装 Playwright 或浏览器二进制未安装。按第 7.2 节安装即可。

### Q4: 执行被阻塞

常见原因：
- 交易成本/约束校验未通过
- 提案未进入可执行状态

可先用 `--dry-run` 验证，再根据风险评估决定是否 `--force`。

## 12. 相关文档

- 运行与命令清单：`README_AGENT_SYSTEM.md`
- WebUI 需求：`docs/webui/WEBUI_PRD.md`
- WebAPI 开发说明：`docs/webui/DEVELOPMENT.md`
- 多终端通知接入（短信/微信）：`docs/notifications/NOTIFICATION_GUIDE.md`
- OpenAPI 描述：`docs/webui/openapi.yaml`

---
如果你希望，我可以再给这个 README 补一版“面向新同事的 5 分钟上手流程”（含截图点位和推荐演练顺序）。
