# MyInvestment WebUI PRD (v1)

## 1. 背景与目标

当前系统核心能力已经完整，主要通过 Python CLI 执行：
- 运行主流程：`agent_system.py`
- 调度与维护：`agent_scheduler.py` / `agent_queue_maintenance.py`
- 人工审核：`agent_review.py`
- 执行落地：`agent_execute.py`
- 运维观察：`agent_ops_report.py` / `agent_alerts.py` / `agent_action_center.py` / `agent_feedback.py`

现状问题：
- 日常操作依赖命令行和文件检索，操作链路分散。
- 待审核、待执行、告警、质量反馈缺少统一入口。
- 对 run 证据链的回溯成本高。

WebUI v1 目标：
1. 将“查看 -> 审核 -> 执行 -> 复盘”流程收敛到单一界面。
2. 保留现有脚本作为业务内核，Web 层只做编排和可视化。
3. 首版优先上线运营价值，不重构策略逻辑。

## 2. 角色与权限

角色定义：
- `viewer`：只读查看（Action Center、Runs、Ops、Alerts、Quality）。
- `reviewer`：包含 `viewer` 权限，可执行审核动作（approve/hold/reject）。
- `executor`：包含 `viewer` 权限，可执行执行队列动作（execute）。
- `admin`：全权限，可触发 scheduler/hooks、更新配置。

最小权限矩阵：
- `GET` 页面与数据：`viewer+`
- `POST /api/reviews/*`：`reviewer+`
- `POST /api/executions/*`：`executor+`
- `POST /api/scheduler/once`、`PATCH /api/config`：`admin`

## 3. 非目标 (v1)

- 不做策略编辑器（不在线改策略代码）。
- 不做真实券商下单集成（继续使用当前执行模拟/状态更新链路）。
- 不做复杂多租户（单实例单账户）。

## 4. 信息架构

一级导航：
1. `Action Center`
2. `Proposal Review`
3. `Execution Queue`
4. `Runs`
5. `Alerts & Ops`
6. `Quality Feedback`
7. `State & Config`

建议默认首页：`Action Center`

## 5. 关键页面规格

## 5.1 Action Center

目标：汇总今日最优先动作。

数据来源：
- `runs/ops/action_center_latest.json`
- 补充跳转：`state/review_queue.jsonl`、`state/execution_queue.jsonl`

核心模块：
- KPI 条：`health_score`、`health_label`、`alert_status`、`active_alert_count`、`pending_review_count`、`pending_execution_count`、`quality_sample_size`
- `Priority Alerts`：按 `critical -> warn` 排序
- `Pending Manual Reviews`：显示 run_id、proposal_id、suggested_decision
- `Pending Executions`：显示 queue_id、run_id、order_count

动作：
- 跳转提案详情：`/proposals/{run_id}`
- 跳转执行详情：`/executions/{run_id}`
- 一键刷新 hooks（admin）：调用 `POST /api/scheduler/once`（可带 skip 选项）

## 5.2 Proposal Review

目标：完成人工审核闭环。

数据来源：
- `state/review_queue.jsonl` (pending)
- `runs/{date}/{run_id}/allocation_proposal.json`
- `runs/{date}/{run_id}/advice_report.md`
- `runs/{date}/{run_id}/stock_research.jsonl`
- `runs/{date}/{run_id}/rebalance_actions.csv`

页面结构：
- 左栏：提案列表（pending，支持按 suggested_decision 过滤）
- 中栏：`advice_report.md` 渲染 + 证据链摘要
- 右栏：结构化指标卡

结构化指标卡字段：
- 基本信息：`proposal_id`、`run_id`、`trading_date`、`as_of_ts`
- 决策信息：`decision`、`review_status`、`human_decision`
- 风险门控：`gate_failures[]`、`constraint_violations[]`、`risk_delta.hard_risk_block`
- 交易估算：`turnover_est`、`transaction_cost_est`
- 证据强度：`evidence_completeness`
- 目标仓位：`target_weights`

审核动作：
- `Approve` / `Hold` / `Reject`
- 必填项：`reviewer`，`note`
- 提交后调用：
  - `python3 agent_review.py --decision ... --run-id ... --reviewer ... --note ...`

交互规则：
- 同一 run 已非 pending 时禁止重复提交，UI 提示“已处理”。
- 审核成功后刷新 pending 列表与 action center 概览。

## 5.3 Execution Queue

目标：执行 approved rebalance 并跟踪结果。

数据来源：
- `state/execution_queue.jsonl` (pending)
- `runs/{date}/{run_id}/execution_orders.csv`
- `runs/{date}/{run_id}/execution_plan.md`
- 执行后：`execution_result.json`、`portfolio_change_report.md`

页面结构：
- 列表：queue_id、run_id、proposal_id、order_count、created_at
- 详情：订单明细、预估权重变化、风险约束提示

执行动作：
- 参数：`executor`、`dry_run`、`force`
- 调用：
  - `python3 agent_execute.py --run-id ... --executor ... [--dry-run] [--force]`

执行后回显：
- 新状态：`execution_queue.status`
- 核心产物：`execution_result.json` 中 `execution_costs`、`constraint_validation`、`warnings`

## 5.4 Runs

目标：全量 run 检索与审计。

数据来源：
- `runs/{trading_date}/{run_id}/run_manifest.json`
- 同目录 artifacts

能力：
- 列表筛选：`date`、`phase`、`status`、`dry_run`
- run 详情：manifest 时间线 + artifacts 列表
- artifact 预览：json/csv/md (只读)

## 5.5 Alerts & Ops

目标：运营健康持续监控。

数据来源：
- `runs/ops/alerts_latest.json`
- `state/alerts_events.jsonl`
- `runs/ops/ops_report_latest.json`

模块：
- 当前告警状态：`status`、active 分组（critical/warn）
- 事件时间线：opened/escalated/deescalated/resolved/reminder
- 健康报告：run 成功率、queue backlog、execution quality

## 5.6 Quality Feedback

目标：把模型反馈可视化，支持解释“为什么当前更保守”。

数据来源：
- `runs/ops/proposal_quality_latest.json`
- `state/model_feedback.json`
- `state/proposal_quality_history.jsonl`

模块：
- 样本量与平均质量分
- `min_confidence_buy`、`max_new_positions_override`
- 低质量案例列表（run_id 跳转）

## 5.7 State & Config

目标：可视化查看关键状态，审慎开放编辑。

只读优先：
- `state/account_snapshot.json`
- `state/current_positions.csv`
- `state/watchlist.csv`
- `agent_config.json`

编辑策略：
- v1 仅支持 `agent_config.json` 局部字段更新（admin）。
- 所有配置更新写审计日志并保留前版本快照。

## 6. 后端架构建议

推荐：`FastAPI` 薄后端，职责如下：
1. 提供统一读接口（聚合 JSON/JSONL/CSV）。
2. 提供受控动作接口（调用 review/execute/scheduler 命令）。
3. 处理权限、审计、幂等、错误映射。

目录建议：
- `webapi/main.py`
- `webapi/routes/*.py`
- `webapi/services/file_repo.py`
- `webapi/services/command_runner.py`
- `webapi/models/*.py`
- `webapi/security/*.py`

命令白名单：
- `agent_review.py`
- `agent_execute.py`
- `agent_scheduler.py`
- 只允许固定参数组合，禁止自由 shell 拼接。

## 7. 数据与刷新策略

刷新策略：
- 首页与待办：轮询 5-10 秒。
- 历史页面：手动刷新 + 30 秒轮询。

缓存策略：
- 文件读取可短缓存 1-3 秒。
- 动作接口返回后主动失效相关缓存键。

## 8. 安全与审计

必须项：
1. 认证：token 或 basic auth（内网最少防护）。
2. 授权：RBAC。
3. 审计：所有 POST 行为写 `state/webui_audit_log.jsonl`。
4. 路径安全：artifact 访问做白名单与 path normalize，防目录穿越。
5. 幂等：审核/执行前二次检查当前状态是否仍为 pending。

## 9. 可观测性

后端日志：
- 请求日志：path、method、user、耗时、status
- 动作日志：command、args、exit_code、stdout_tail、stderr_tail

指标：
- API p95
- 动作成功率
- 文件读取失败率

## 10. 里程碑与验收

M1 (5-7 天)：只读运营台
- 页面：Action Center + Runs + Alerts/Ops + Quality
- 验收：无需命令行可完成日常巡检

M2 (4-6 天)：审核闭环
- 页面：Proposal Review
- 动作：approve/hold/reject
- 验收：可替代 `agent_review.py` 手动流程

M3 (4-6 天)：执行闭环
- 页面：Execution Queue
- 动作：execute(dry-run/force)
- 验收：可替代 `agent_execute.py` 手动流程

M4 (3-5 天)：配置与治理
- 页面：State & Config
- 能力：配置变更 + 审计 + 权限补强

## 11. 风险与对策

风险：文件并发读写导致状态抖动。
对策：写操作加文件锁，读操作支持重试。

风险：重复提交审核/执行。
对策：后端状态检查 + 前端按钮防抖 + 幂等键。

风险：命令执行超时。
对策：命令超时控制（例如 60 秒）+ 明确错误反馈。

## 12. UI 视觉方向

建议方向：
- 风格：深色中性底 + 红黄绿风险语义色
- 字体：中文正文 `Noto Sans SC`，数字 `JetBrains Mono`
- 组件：高信息密度表格 + 可折叠证据卡
- 动效：仅在关键动作（审核/执行）显示状态过渡

