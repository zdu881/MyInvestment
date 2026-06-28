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


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


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


def test_day0_empty_account_stays_in_cash_without_high_conviction_buy(tmp_path: Path) -> None:
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
                "bootstrap_on_empty_positions": False,
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
            "feedback": {
                "default_min_confidence_buy": 0.75,
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
    assert proposal["decision"] == "stay_in_cash"
    assert proposal["strategy_mode"] == "parallel_lines"
    assert proposal["strategy_lines"]["enabled"] is True
    assert proposal.get("feedback_context", {}).get("selected_count") == 0
    assert "evidence_below_threshold" in proposal.get("gate_failures", [])
    assert proposal.get("abstain_context", {}).get("baseline") == "cash"
    assert (run_dir / "strategy_line_plan.json").exists()
    assert (run_dir / "strategy_line_allocations.csv").exists()

    rebalance_rows = _read_csv(run_dir / "rebalance_actions.csv")
    actionable = [row for row in rebalance_rows if str(row.get("action", "")) != "HOLD"]
    assert actionable == []

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
    assert pending_items == []
    assert not (run_dir / "execution_orders.csv").exists()


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


def test_agent_execute_virtual_portfolio_applies_without_mutating_real_state(tmp_path: Path) -> None:
    config_path = tmp_path / "agent_config.json"
    _write_json(
        config_path,
        {
            "paths": {
                "runs_root": "runs",
                "state_root": "state",
            },
            "constraints": {
                "max_single_weight": 0.3,
                "max_industry_weight": 0.8,
                "min_cash_ratio": 0.1,
            },
            "execution": {
                "manual_only": True,
                "confirmation_required": True,
                "max_cost_ratio_total_asset": 0.01,
            },
            "virtual_portfolio": {
                "positions_path": "state/virtual_positions.csv",
                "account_path": "state/virtual_account_snapshot.json",
                "history_path": "state/virtual_execution_history.jsonl",
                "initialize_from_real": True,
            },
        },
    )
    real_account = {
        "cash": 5000.0,
        "stock_asset": 20000.0,
        "total_asset": 25000.0,
        "cash_ratio": 0.2,
        "max_single_weight": 0.3,
        "max_industry_weight": 0.8,
        "min_cash_ratio": 0.1,
    }
    _write_json(tmp_path / "state" / "account_snapshot.json", real_account)
    _write_csv(
        tmp_path / "state" / "current_positions.csv",
        fieldnames=["ticker", "name", "shares", "avg_cost", "last_price", "market_value", "weight", "industry", "updated_at"],
        rows=[
            {
                "ticker": "600941",
                "name": "中国移动",
                "shares": "100",
                "avg_cost": "93.94",
                "last_price": "91.65",
                "market_value": "9165.00",
                "weight": "0.3666",
                "industry": "通信",
                "updated_at": "2026-06-18T15:00:00+08:00",
            },
            {
                "ticker": "601816",
                "name": "京沪高铁",
                "shares": "2100",
                "avg_cost": "4.952",
                "last_price": "4.59",
                "market_value": "9639.00",
                "weight": "0.3856",
                "industry": "交通运输",
                "updated_at": "2026-06-18T15:00:00+08:00",
            },
        ],
    )
    real_positions_before = (tmp_path / "state" / "current_positions.csv").read_text(encoding="utf-8-sig")
    real_account_before = _read_json(tmp_path / "state" / "account_snapshot.json")

    run_id = "virtual-run-0001"
    proposal_id = "proposal-virtual-0001"
    run_dir = tmp_path / "runs" / "2026-06-28" / run_id
    _write_json(
        run_dir / "allocation_proposal.json",
        {
            "run_id": run_id,
            "proposal_id": proposal_id,
            "review_status": "approved",
            "target_weights": {"600941": 0.2, "601816": 0.1},
            "new_portfolio": [
                {"ticker": "600941", "name": "中国移动", "industry": "通信"},
                {"ticker": "601816", "name": "京沪高铁", "industry": "交通运输"},
            ],
        },
    )
    _write_json(
        run_dir / "review_decision.json",
        {"run_id": run_id, "proposal_id": proposal_id, "human_decision": "approve"},
    )
    _write_csv(
        run_dir / "candidates_step2.csv",
        fieldnames=["股票代码", "名称", "现价", "行业"],
        rows=[
            {"股票代码": "600941", "名称": "中国移动", "现价": "90.00", "行业": "通信"},
            {"股票代码": "601816", "名称": "京沪高铁", "现价": "5.00", "行业": "交通运输"},
        ],
    )
    _write_csv(
        run_dir / "execution_orders.csv",
        fieldnames=["ticker", "action", "delta_weight"],
        rows=[
            {"ticker": "600941", "action": "DECREASE", "delta_weight": "-0.1666"},
            {"ticker": "601816", "action": "DECREASE", "delta_weight": "-0.2856"},
        ],
    )
    queue_item = {
        "queue_id": "exec-virtual-0001",
        "run_id": run_id,
        "proposal_id": proposal_id,
        "status": "pending",
        "execution_orders_path": str(run_dir / "execution_orders.csv"),
    }
    _write_text(
        tmp_path / "state" / "execution_queue.jsonl",
        json.dumps(queue_item, ensure_ascii=False) + "\n",
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(EXEC_SCRIPT),
            "--config",
            str(config_path),
            "--run-id",
            run_id,
            "--executor",
            "virtual_tester",
            "--virtual",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "virtual=True" in proc.stdout
    assert (tmp_path / "state" / "current_positions.csv").read_text(encoding="utf-8-sig") == real_positions_before
    assert _read_json(tmp_path / "state" / "account_snapshot.json") == real_account_before
    assert _read_jsonl(tmp_path / "state" / "execution_queue.jsonl")[0]["status"] == "pending"

    virtual_account = _read_json(tmp_path / "state" / "virtual_account_snapshot.json")
    assert virtual_account["portfolio_type"] == "virtual"
    assert virtual_account["last_virtual_run_id"] == run_id
    assert virtual_account["stock_asset"] == 7500.0
    assert virtual_account["cash"] < 17500.0

    virtual_positions = _read_csv(tmp_path / "state" / "virtual_positions.csv")
    assert [row["ticker"] for row in virtual_positions] == ["600941", "601816"]
    assert [row["market_value"] for row in virtual_positions] == ["5000.0", "2500.0"]

    result = _read_json(run_dir / "virtual_execution_result.json")
    assert result["virtual"] is True
    assert result["dry_run"] is False
    assert result["position_count"] == 2
    assert (run_dir / "virtual_portfolio_before_snapshot.csv").exists()
    assert (run_dir / "virtual_portfolio_after_snapshot.csv").exists()
    assert (run_dir / "virtual_portfolio_change_report.md").exists()
    assert _read_jsonl(tmp_path / "state" / "virtual_execution_history.jsonl")[0]["run_id"] == run_id
    assert not (run_dir / "execution_result.json").exists()


def test_agent_execute_virtual_portfolio_can_simulate_unqueued_proposal(tmp_path: Path) -> None:
    config_path = tmp_path / "agent_config.json"
    _write_json(
        config_path,
        {
            "paths": {
                "runs_root": "runs",
                "state_root": "state",
            },
            "constraints": {
                "max_single_weight": 0.4,
                "max_industry_weight": 0.8,
                "min_cash_ratio": 0.1,
            },
            "execution": {
                "manual_only": True,
                "confirmation_required": True,
                "max_cost_ratio_total_asset": 0.01,
            },
            "virtual_portfolio": {
                "initialize_from_real": False,
                "initial_cash": 50000.0,
            },
        },
    )
    _write_json(
        tmp_path / "state" / "account_snapshot.json",
        {"cash": 12000.0, "stock_asset": 0.0, "total_asset": 12000.0, "cash_ratio": 1.0},
    )
    _write_csv(
        tmp_path / "state" / "current_positions.csv",
        fieldnames=["ticker", "name", "shares", "avg_cost", "last_price", "market_value", "weight", "industry", "updated_at"],
        rows=[],
    )

    run_id = "virtual-unqueued-0001"
    run_dir = tmp_path / "runs" / "2026-06-28" / run_id
    _write_json(
        run_dir / "allocation_proposal.json",
        {
            "run_id": run_id,
            "decision_id": "proposal-unqueued-0001",
            "target_weights": {"600941": 0.3, "601816": 0.2},
            "new_portfolio": [
                {"ticker": "600941", "industry": "通信"},
                {"ticker": "601816", "industry": "交通运输"},
            ],
        },
    )
    _write_csv(
        run_dir / "candidates_step2.csv",
        fieldnames=["股票代码", "名称", "现价", "行业"],
        rows=[
            {"股票代码": "600941", "名称": "中国移动", "现价": "100.00", "行业": "通信"},
            {"股票代码": "601816", "名称": "京沪高铁", "现价": "5.00", "行业": "交通运输"},
        ],
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(EXEC_SCRIPT),
            "--config",
            str(config_path),
            "--run-id",
            run_id,
            "--executor",
            "virtual_tester",
            "--virtual",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "queue_id=virtual:virtual-unqueued-0001" in proc.stdout
    assert not (tmp_path / "state" / "execution_queue.jsonl").exists()

    virtual_account = _read_json(tmp_path / "state" / "virtual_account_snapshot.json")
    assert virtual_account["total_asset"] < 50000.0
    assert virtual_account["stock_asset"] == 25000.0
    assert virtual_account["cash"] < 25000.0

    result = _read_json(run_dir / "virtual_execution_result.json")
    assert result["proposal_id"] == "proposal-unqueued-0001"
    assert result["queue_id"] == "virtual:virtual-unqueued-0001"
    assert result["virtual"] is True
    assert any("without pending execution queue item" in item for item in result["warnings"])
    assert any("without review decision" in item for item in result["warnings"])
    assert any("execution orders file not found" in item for item in result["warnings"])


def test_agent_execute_virtual_portfolio_uses_position_price_when_run_price_missing(tmp_path: Path) -> None:
    config_path = tmp_path / "agent_config.json"
    _write_json(
        config_path,
        {
            "paths": {
                "runs_root": "runs",
                "state_root": "state",
            },
            "constraints": {
                "max_single_weight": 0.4,
                "max_industry_weight": 0.8,
                "min_cash_ratio": 0.1,
            },
            "execution": {
                "manual_only": True,
                "confirmation_required": True,
                "max_cost_ratio_total_asset": 0.01,
            },
            "virtual_portfolio": {
                "initialize_from_real": True,
            },
        },
    )
    _write_json(
        tmp_path / "state" / "account_snapshot.json",
        {"cash": 14114.05, "stock_asset": 9639.0, "total_asset": 23753.05, "cash_ratio": 0.594199},
    )
    _write_csv(
        tmp_path / "state" / "current_positions.csv",
        fieldnames=["ticker", "name", "shares", "avg_cost", "last_price", "market_value", "weight", "industry", "updated_at"],
        rows=[
            {
                "ticker": "601816",
                "name": "京沪高铁",
                "shares": "2100",
                "avg_cost": "4.952",
                "last_price": "4.59",
                "market_value": "9639.00",
                "weight": "0.405801",
                "industry": "交通运输",
                "updated_at": "2026-06-23T15:00:00+08:00",
            },
        ],
    )
    _write_json(
        tmp_path / "state" / "virtual_account_snapshot.json",
        {"cash": 10.0, "stock_asset": 10.0, "total_asset": 20.0, "cash_ratio": 0.5},
    )
    _write_csv(
        tmp_path / "state" / "virtual_positions.csv",
        fieldnames=["ticker", "name", "shares", "avg_cost", "last_price", "market_value", "weight", "industry", "updated_at"],
        rows=[
            {
                "ticker": "601816",
                "name": "N/A",
                "shares": "10",
                "avg_cost": "1.0",
                "last_price": "1.0",
                "market_value": "10.0",
                "weight": "0.5",
                "industry": "交通运输",
                "updated_at": "2026-06-20T15:00:00+08:00",
            },
        ],
    )

    run_id = "virtual-price-fallback-0001"
    run_dir = tmp_path / "runs" / "2026-06-28" / run_id
    _write_json(
        run_dir / "allocation_proposal.json",
        {
            "run_id": run_id,
            "decision_id": "proposal-price-fallback-0001",
            "target_weights": {"601816": 0.3},
            "new_portfolio": [{"ticker": "601816", "industry": "交通运输"}],
        },
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(EXEC_SCRIPT),
            "--config",
            str(config_path),
            "--run-id",
            run_id,
            "--executor",
            "virtual_tester",
            "--virtual",
            "--virtual-reset",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    virtual_positions = _read_csv(tmp_path / "state" / "virtual_positions.csv")
    assert virtual_positions[0]["ticker"] == "601816"
    assert virtual_positions[0]["name"] == "京沪高铁"
    assert virtual_positions[0]["last_price"] == "4.59"
    assert virtual_positions[0]["shares"] == "1552.4869"
    result = _read_json(run_dir / "virtual_execution_result.json")
    assert result["virtual_reset"] is True
    assert not any("missing price for 601816" in item for item in result["warnings"])


def test_postclose_degrades_to_current_positions_when_refresh_fails(tmp_path: Path) -> None:
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
                "max_candidates_for_research": 2,
                "allow_stale_candidate_fallback": "false",
                "external_refresh_timeout_sec": 1,
            },
            "constraints": {
                "max_single_weight": 0.3,
                "max_industry_weight": 0.5,
                "min_cash_ratio": 0.1,
            },
        },
    )
    _write_json(
        tmp_path / "state" / "account_snapshot.json",
        {
            "cash": 5000.0,
            "total_asset": 25000.0,
            "stock_asset": 20000.0,
            "cash_ratio": 0.2,
            "max_single_weight": 0.3,
            "max_industry_weight": 0.5,
            "min_cash_ratio": 0.1,
        },
    )
    _write_csv(
        tmp_path / "state" / "current_positions.csv",
        fieldnames=["ticker", "name", "shares", "avg_cost", "last_price", "market_value", "weight", "industry", "updated_at"],
        rows=[
            {
                "ticker": "600941",
                "name": "中国移动",
                "shares": "100",
                "avg_cost": "93.94",
                "last_price": "91.65",
                "market_value": "9165.00",
                "weight": "0.3837",
                "industry": "通信",
                "updated_at": "2026-06-18T15:00:00+08:00",
            },
            {
                "ticker": "601816",
                "name": "京沪高铁",
                "shares": "2100",
                "avg_cost": "4.952",
                "last_price": "4.59",
                "market_value": "9639.00",
                "weight": "0.4036",
                "industry": "交通运输",
                "updated_at": "2026-06-18T15:00:00+08:00",
            },
        ],
    )
    _write_csv(
        tmp_path / "candidates_step2.csv",
        fieldnames=["股票代码", "名称", "PE(TTM)", "PB", "股息率(%)", "行业"],
        rows=[
            {
                "股票代码": "600000",
                "名称": "浦发银行",
                "PE(TTM)": "5.0",
                "PB": "0.5",
                "股息率(%)": "4.0",
                "行业": "银行",
            }
        ],
    )
    _write_text(
        tmp_path / "step1_screener.py",
        "import sys\nprint('step1 failed on purpose')\nsys.exit(2)\n",
    )
    _write_text(
        tmp_path / "step2_financial_cleaner.py",
        "import sys\nprint('step2 failed on purpose')\nsys.exit(3)\n",
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(SYSTEM_SCRIPT),
            "--phase",
            "postclose",
            "--config",
            str(config_path),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    output = (proc.stdout or "") + (proc.stderr or "")
    assert "status=success" in output

    manifests = list((tmp_path / "runs").glob("*/*/run_manifest.json"))
    assert len(manifests) == 1
    manifest = _read_json(manifests[0])
    assert manifest["status"] == "success"
    assert manifest["error_summary"] == ""
    assert not any(path.endswith("candidates_step2.csv") for path in manifest["artifacts"])

    run_dir = manifests[0].parent
    proposal = _read_json(run_dir / "allocation_proposal.json")
    assert proposal["decision"] == "watch"
    assert proposal["data_source_degraded"] is True
    assert proposal["strategy_mode"] == "parallel_lines"
    assert proposal["strategy_lines"]["lines"]["value"]["allow_existing_fallback"] is True
    assert "data_source_degraded" in proposal["gate_warnings"]
    assert proposal["target_weights"] == {"600941": 0.3, "601816": 0.3}

    actions = _read_csv(run_dir / "rebalance_actions.csv")
    assert [row["action"] for row in actions] == ["DECREASE", "DECREASE"]
    assert _read_jsonl(tmp_path / "state" / "review_queue.jsonl")[0]["run_id"] == proposal["run_id"]


def test_postclose_uses_fresh_step1_candidates_when_step2_fails(tmp_path: Path) -> None:
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
                "max_candidates_for_research": 2,
                "allow_stale_candidate_fallback": "false",
                "external_refresh_timeout_sec": 5,
            },
            "constraints": {
                "max_single_weight": 0.3,
                "max_industry_weight": 0.6,
                "min_cash_ratio": 0.1,
            },
        },
    )
    _write_json(
        tmp_path / "state" / "account_snapshot.json",
        {
            "cash": 100000.0,
            "total_asset": 100000.0,
            "stock_asset": 0.0,
            "cash_ratio": 1.0,
            "max_single_weight": 0.3,
            "max_industry_weight": 0.6,
            "min_cash_ratio": 0.1,
        },
    )
    _write_csv(
        tmp_path / "state" / "current_positions.csv",
        fieldnames=["ticker", "name", "shares", "avg_cost", "last_price", "market_value", "weight", "industry", "updated_at"],
        rows=[],
    )
    _write_csv(
        tmp_path / "candidates_step2.csv",
        fieldnames=["股票代码", "名称", "PE(TTM)", "PB", "股息率(%)", "行业"],
        rows=[
            {"股票代码": "600999", "名称": "旧候选", "PE(TTM)": "5.0", "PB": "0.5", "股息率(%)": "6.0", "行业": "旧行业"}
        ],
    )
    _write_text(
        tmp_path / "step1_screener.py",
        "\n".join(
            [
                "import pandas as pd",
                "pd.DataFrame([",
                " {'股票代码':'600741','名称':'华域汽车','PE(TTM)':7.3,'PB':0.77,'股息率(%)':6.02,'行业':'汽车','命中条件数':5},",
                " {'股票代码':'601668','名称':'中国建筑','PE(TTM)':5.06,'PB':0.39,'股息率(%)':5.85,'行业':'建筑','命中条件数':5},",
                "]).to_csv('candidates.csv', index=False, encoding='utf-8-sig')",
            ]
        )
        + "\n",
    )
    _write_text(
        tmp_path / "step2_financial_cleaner.py",
        "import sys\nprint('step2 failed on purpose')\nsys.exit(3)\n",
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(SYSTEM_SCRIPT),
            "--phase",
            "postclose",
            "--config",
            str(config_path),
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    manifest_path = next((tmp_path / "runs").glob("*/*/run_manifest.json"))
    manifest = _read_json(manifest_path)
    assert manifest["status"] == "success"
    assert any(path.endswith("candidates_step1.csv") for path in manifest["artifacts"])
    assert not any(path.endswith("candidates_step2.csv") for path in manifest["artifacts"])

    run_dir = manifest_path.parent
    proposal = _read_json(run_dir / "allocation_proposal.json")
    assert proposal["data_source_degraded"] is False
    assert "step2_refresh_failed_using_step1" in proposal["gate_warnings"]
    assert proposal["target_weights"] == {"600741": 0.3, "601668": 0.3}
    assert proposal["decision"] == "rebalance"

    research_rows = _read_jsonl(run_dir / "stock_research.jsonl")
    assert {row["source"] for row in research_rows} == {"step1_partial_refresh"}
    assert {row["analysis_mode"] for row in research_rows} == {"step1_candidate_heuristic"}

    actions = _read_csv(run_dir / "rebalance_actions.csv")
    assert [row["action"] for row in actions] == ["BUY", "BUY"]
    assert [row["name"] for row in actions] == ["华域汽车", "中国建筑"]
