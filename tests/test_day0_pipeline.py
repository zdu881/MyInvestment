from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INIT_SCRIPT = ROOT / "agent_init_state.py"
SYSTEM_SCRIPT = ROOT / "agent_system.py"
REVIEW_SCRIPT = ROOT / "agent_review.py"
EXEC_SCRIPT = ROOT / "agent_execute.py"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _read_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def test_day0_bootstrap_rebalance_flow(tmp_path: Path) -> None:
    config_path = tmp_path / "agent_config.json"
    _write_json(
        config_path,
        {
            "timezone_offset_hours": 8,
            "paths": {
                "runs_root": "runs",
                "state_root": "state",
                "knowledge_root": "knowledge",
                "step1_csv": "candidates.csv",
                "step2_csv": "candidates_step2.csv",
            },
            "postclose": {
                "max_candidates_for_research": 8,
                "max_new_positions": 3,
                "default_transaction_cost_rate": 0.0015,
                "bootstrap_on_empty_positions": True,
                "bootstrap_max_positions": 3,
                "bootstrap_min_score": 0.0,
            },
            "gates": {
                "min_evidence_completeness": 0.6,
                "min_action_delta": 0.02,
                "max_allowed_constraint_violations": 0,
            },
            "constraints": {
                "max_single_weight": 0.3,
                "max_industry_weight": 0.5,
                "min_cash_ratio": 0.1,
            },
            "execution": {
                "slippage_bps": 5.0,
                "commission_rate": 0.0003,
                "stamp_duty_sell_rate": 0.001,
                "max_cost_ratio_total_asset": 0.01,
                "enforce_constraint_guard": True,
                "constraint_tolerance": 0.001,
            },
        },
    )

    _write_csv(
        tmp_path / "candidates_step2.csv",
        fieldnames=[
            "股票代码",
            "名称",
            "PE(TTM)",
            "PB",
            "股息率(%)",
            "行业",
        ],
        rows=[
            {"股票代码": "600023", "名称": "浙能电力", "PE(TTM)": 9.2, "PB": 0.9, "股息率(%)": 4.2, "行业": "公用事业"},
            {"股票代码": "601668", "名称": "中国建筑", "PE(TTM)": 4.7, "PB": 0.5, "股息率(%)": 4.0, "行业": "建筑"},
            {"股票代码": "601390", "名称": "中国中铁", "PE(TTM)": 5.6, "PB": 0.4, "股息率(%)": 3.7, "行业": "交运"},
            {"股票代码": "600820", "名称": "隧道股份", "PE(TTM)": 8.1, "PB": 0.7, "股息率(%)": 3.5, "行业": "环保"},
        ],
    )

    init_proc = subprocess.run(
        [
            sys.executable,
            str(INIT_SCRIPT),
            "--config",
            str(config_path),
            "--initial-capital",
            "100000",
            "--reset-runtime",
            "--reset-knowledge",
            "--reset-watchlist",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert init_proc.returncode == 0, init_proc.stdout + init_proc.stderr

    postclose_proc = subprocess.run(
        [
            sys.executable,
            str(SYSTEM_SCRIPT),
            "--phase",
            "postclose",
            "--dry-run",
            "--config",
            str(config_path),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert postclose_proc.returncode == 0, postclose_proc.stdout + postclose_proc.stderr

    review_queue = _read_jsonl(tmp_path / "state" / "review_queue.jsonl")
    assert len(review_queue) == 1
    assert review_queue[0]["status"] == "pending"
    run_id = str(review_queue[0]["run_id"])

    run_dirs = list((tmp_path / "runs").glob(f"*/{run_id}"))
    assert len(run_dirs) == 1
    run_dir = run_dirs[0]

    proposal = _read_json(run_dir / "allocation_proposal.json")
    assert proposal["decision"] == "rebalance"
    assert proposal.get("feedback_context", {}).get("bootstrap_mode") is True
    assert "evidence_below_threshold_bootstrap_override" in proposal.get("gate_warnings", [])

    rebalance_rows = _read_csv(run_dir / "rebalance_actions.csv")
    actionable = [row for row in rebalance_rows if str(row.get("action", "")) != "HOLD"]
    assert len(actionable) == 3
    assert all(str(row.get("action", "")) == "BUY" for row in actionable)

    review_proc = subprocess.run(
        [
            sys.executable,
            str(REVIEW_SCRIPT),
            "--decision",
            "approve",
            "--run-id",
            run_id,
            "--runs-root",
            "runs",
            "--reviewer",
            "day0_tester",
            "--note",
            "approve day0 bootstrap plan",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert review_proc.returncode == 0, review_proc.stdout + review_proc.stderr

    execution_queue = _read_jsonl(tmp_path / "state" / "execution_queue.jsonl")
    pending_items = [row for row in execution_queue if row.get("run_id") == run_id and row.get("status") == "pending"]
    assert len(pending_items) == 1

    orders_rows = _read_csv(run_dir / "execution_orders.csv")
    assert len(orders_rows) == pending_items[0]["order_count"]
    assert all(str(row.get("action", "")) == "BUY" for row in orders_rows)
    assert all(float(row["target_weight"]) > 0 for row in orders_rows)

    execute_proc = subprocess.run(
        [
            sys.executable,
            str(EXEC_SCRIPT),
            "--config",
            str(config_path),
            "--run-id",
            run_id,
            "--executor",
            "day0_tester",
            "--dry-run",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert execute_proc.returncode == 0, execute_proc.stdout + execute_proc.stderr

    execution_result = _read_json(run_dir / "execution_result.json")
    assert execution_result["dry_run"] is True
    assert execution_result["position_count"] == len(orders_rows)
    assert execution_result["note"] == "dry-run only, state not changed"


def test_agent_execute_blocks_non_dry_run_in_manual_only_mode(tmp_path: Path) -> None:
    config_path = tmp_path / "agent_config.json"
    _write_json(
        config_path,
        {
            "paths": {
                "runs_root": "runs",
                "state_root": "state",
            },
            "execution": {
                "manual_only": True,
            },
        },
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(EXEC_SCRIPT),
            "--config",
            str(config_path),
            "--run-id",
            "dummy-run",
            "--executor",
            "tester",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert proc.returncode != 0
    message = (proc.stdout or "") + (proc.stderr or "")
    assert "manual_only" in message
