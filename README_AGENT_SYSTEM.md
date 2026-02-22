# Agent System Runtime

This document describes the first build milestone of the daily investment agent workflow.

## What is added

- `agent_system.py`: unified runtime for `preopen`, `intraday`, `postclose`, and `all` phases.
- `agent_config.json`: runtime thresholds, gating constraints, and output settings.
- `state/current_positions.csv`: current holdings input.
- `state/account_snapshot.json`: account snapshot and risk constraints.
- `state/watchlist.csv`: watchlist input.
- `runs/{trading_date}/{run_id}/...`: per-run artifacts.
- `decision_log.jsonl`: rolling decision log across runs.

## Run commands

- Dry run all phases:
  - `python3 agent_system.py --phase all --dry-run`
- Run postclose only:
  - `python3 agent_system.py --phase postclose`
- Run preopen only:
  - `python3 agent_system.py --phase preopen`
- Scheduler once (execute the next due phase only):
  - `python3 agent_scheduler.py --once`
- Scheduler once in dry-run:
  - `python3 agent_scheduler.py --once --dry-run`
- Scheduler once without maintenance/report hooks:
  - `python3 agent_scheduler.py --once --skip-maintenance --skip-ops-report`
- Scheduler once without feedback/skill promotion hooks:
  - `python3 agent_scheduler.py --once --skip-feedback --skip-skill-promotion`
- Scheduler once without alert channel hook:
  - `python3 agent_scheduler.py --once --skip-alerts`
- Manual review (approve/hold/reject):
  - `python3 agent_review.py --decision approve --run-id <RUN_ID> --reviewer your_name --note "approved after manual check"`
- Execute approved task and update state:
  - `python3 agent_execute.py --run-id <RUN_ID> --executor your_name`
- Execution dry-run (do not mutate state):
  - `python3 agent_execute.py --run-id <RUN_ID> --executor your_name --dry-run`
- Force execution when cost guard blocks (use carefully):
  - `python3 agent_execute.py --run-id <RUN_ID> --executor your_name --force`
- Generate operations health report:
  - `python3 agent_ops_report.py --days 7`
- Queue maintenance dry-run (stale mark + archive preview):
  - `python3 agent_queue_maintenance.py --dry-run`
- Queue maintenance apply:
  - `python3 agent_queue_maintenance.py`
- Generate proposal quality feedback and next-round thresholds:
  - `python3 agent_feedback.py --days 30`
- Promote high-quality skill candidates into versioned registry:
  - `python3 agent_skill_manager.py`
- Refresh operational alert channel:
  - `python3 agent_alerts.py`
- Refresh operations action center:
  - `python3 agent_action_center.py`
- Start integrated WebUI + API console:
  - `./scripts/start_webui.sh`
- Run WebUI/API acceptance tests:
  - `./scripts/run_tests.sh`
- Run optional browser E2E (language switch):
  - `./scripts/run_webui_e2e.sh`

## Scheduling (cron example)

- Run scheduler every 5 minutes:
  - `*/5 * * * * cd /data/home/sim6g/MyInvestment && /usr/bin/python3 agent_scheduler.py --once >> /tmp/myinvestment_scheduler.log 2>&1`
- By default scheduler runs queue maintenance + proposal quality feedback + skill promotion + alert channel + action center after each trigger, and refreshes ops report when a phase is executed.
- Optional: refresh ops report even when no phase is due:
  - `*/30 * * * * cd /data/home/sim6g/MyInvestment && /usr/bin/python3 agent_scheduler.py --once --ops-on-idle --skip-maintenance >> /tmp/myinvestment_ops.log 2>&1`

## Core outputs per run

- `run_manifest.json`: status, step results, and artifact list.
- `preopen_brief.md`: morning portfolio brief.
- `intraday_alerts.jsonl`: intraday alerts.
- `intraday_brief.md`: intraday summary.
- `candidates_step1.csv` and `candidates_step2.csv`: copied screening artifacts.
- `stock_research.jsonl`: per-ticker AI/tool research summary.
- `allocation_proposal.json`: target portfolio and gate results.
- `rebalance_actions.csv`: action table (`BUY/SELL/INCREASE/DECREASE/HOLD`).
- `decision_log.jsonl`: run-level decision log.
- `review_request.json`: pending manual review payload.
- `skill_candidates.jsonl`: run-level skill discovery candidates.
- `execution_orders.csv`: queued execution orders generated after approved rebalance.
- `execution_result.json`: execution outcome and state update summary.
- `runs/ops/proposal_quality_latest.md` and `runs/ops/proposal_quality_latest.json`: proposal quality scoring snapshots.
- `portfolio_change_report.md`: before/after portfolio and risk exposure diff report.
- `portfolio_before_snapshot.csv` and `portfolio_after_snapshot.csv`: execution snapshots.
- `advice_report.md`: human-review proposal report.
- `runs/ops/alerts_latest.md` and `runs/ops/alerts_latest.json`: current alert status and transitions.
- `runs/ops/action_center_latest.md` and `runs/ops/action_center_latest.json`: one-page actionable console for pending reviews/executions.

## Manual review policy

- The runtime only generates proposals.
- Every action is marked for manual review.
- Use `advice_report.md` and gate results before any execution.
- Use `agent_review.py` to finalize `approve/hold/reject`.
- Use `agent_execute.py` to apply an approved rebalance to state files.

## Skill accumulation

- Run-level skill candidates are generated in `runs/.../skill_candidates.jsonl`.
- Global skill candidate pool is appended to `knowledge/skill_candidates.jsonl`.
- Auto-promotion writes versioned skills to `knowledge/skills_registry.csv`.
- Promotion history is tracked in `knowledge/skills_registry_history.jsonl`.

## Quality feedback loop

- `agent_feedback.py` reads `state/execution_history.jsonl` and per-run artifacts to score proposal quality.
- Quality history is persisted in `state/proposal_quality_history.jsonl`.
- Next-round selection feedback is persisted in `state/model_feedback.json`.
- `agent_system.py` applies `min_confidence_buy`, `max_new_positions_override`, and penalty maps from `state/model_feedback.json` during target weight construction.

## Alert channel

- `agent_alerts.py` consumes latest ops/quality/feedback artifacts and emits warn/critical alerts.
- Alert state is persisted in `state/alerts_state.json`.
- Alert transition events are appended to `state/alerts_events.jsonl` (`opened`, `escalated`, `deescalated`, `resolved`, `reminder`).
- Use `ops_alerts` section in `agent_config.json` to tune thresholds.

## Action center

- `agent_action_center.py` consolidates health, alerts, pending review queue, and pending execution queue into a single decision dashboard.
- It is refreshed by scheduler by default and can be consumed as `runs/ops/action_center_latest.md`.

## Notes

- In `--dry-run` mode, external tools are not required.
- In non-dry mode, `postclose` attempts to run `step1_screener.py` and `step2_financial_cleaner.py`.
- If those scripts fail, existing candidate CSV files are still used as fallback when available.
- `agent_execute.py` writes state mutations only when not using `--dry-run`.
- Execution cost model is configurable in `agent_config.json`:
  - `execution.slippage_bps`
  - `execution.commission_rate`
  - `execution.stamp_duty_sell_rate`
  - `execution.max_cost_ratio_total_asset`
  - `execution.enforce_constraint_guard`
  - `execution.constraint_tolerance`
- Post-execution constraints are validated (single name, industry concentration, cash ratio).
- Constraint violations block execution by default and can be overridden with `--force`.
- `agent_ops_report.py` writes summary reports to:
  - `runs/ops/ops_report_latest.md`
  - `runs/ops/ops_report_latest.json`
- Scheduler hook controls:
  - `--skip-maintenance`: skip queue stale-mark/archive pass
  - `--skip-ops-report`: skip ops report refresh
  - `--skip-feedback`: skip proposal quality scoring / feedback refresh
  - `--skip-skill-promotion`: skip auto skill promotion refresh
  - `--skip-alerts`: skip alert channel refresh
  - `--skip-action-center`: skip action center refresh
  - `--ops-on-idle`: refresh ops report even when no phase is due
- Ops report queue metrics include:
  - `pending_review`, `pending_execution`
  - `stale_review`, `stale_execution`
  - `oldest_pending_review_hours`, `oldest_pending_execution_hours`
