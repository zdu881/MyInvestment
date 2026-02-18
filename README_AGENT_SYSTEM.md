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
- Manual review (approve/hold/reject):
  - `python3 agent_review.py --decision approve --run-id <RUN_ID> --reviewer your_name --note "approved after manual check"`
- Execute approved task and update state:
  - `python3 agent_execute.py --run-id <RUN_ID> --executor your_name`
- Execution dry-run (do not mutate state):
  - `python3 agent_execute.py --run-id <RUN_ID> --executor your_name --dry-run`
- Force execution when cost guard blocks (use carefully):
  - `python3 agent_execute.py --run-id <RUN_ID> --executor your_name --force`

## Scheduling (cron example)

- Run scheduler every 5 minutes:
  - `*/5 * * * * cd /data/home/sim6g/MyInvestment && /usr/bin/python3 agent_scheduler.py --once >> /tmp/myinvestment_scheduler.log 2>&1`

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
- `portfolio_change_report.md`: before/after portfolio and risk exposure diff report.
- `portfolio_before_snapshot.csv` and `portfolio_after_snapshot.csv`: execution snapshots.
- `advice_report.md`: human-review proposal report.

## Manual review policy

- The runtime only generates proposals.
- Every action is marked for manual review.
- Use `advice_report.md` and gate results before any execution.
- Use `agent_review.py` to finalize `approve/hold/reject`.
- Use `agent_execute.py` to apply an approved rebalance to state files.

## Skill accumulation

- Run-level skill candidates are generated in `runs/.../skill_candidates.jsonl`.
- Global skill candidate pool is appended to `knowledge/skill_candidates.jsonl`.
- Initial registry scaffold is created at `knowledge/skills_registry.csv`.

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
