from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "agent_init_state.py"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_workspace(tmp_path: Path) -> tuple[Path, Path, Path, Path]:
    runs_root = tmp_path / "runs"
    state_root = tmp_path / "state"
    knowledge_root = tmp_path / "knowledge"
    config_path = tmp_path / "agent_config.json"

    _write_json(
        config_path,
        {
            "timezone_offset_hours": 8,
            "paths": {
                "runs_root": "runs",
                "state_root": "state",
                "knowledge_root": "knowledge",
                "decision_log": "state/decision_log.jsonl",
            },
            "constraints": {
                "max_single_weight": 0.3,
                "max_industry_weight": 0.5,
                "min_cash_ratio": 0.1,
            },
        },
    )
    return config_path, runs_root, state_root, knowledge_root


def test_init_state_reset_runtime_and_knowledge(tmp_path: Path) -> None:
    config_path, runs_root, state_root, knowledge_root = _build_workspace(tmp_path)
    state_root.mkdir(parents=True, exist_ok=True)

    # Seed test artifacts.
    (runs_root / "2026-01-01" / "dummy-run").mkdir(parents=True, exist_ok=True)
    (runs_root / "2026-01-01" / "dummy-run" / "run_manifest.json").write_text(
        '{"run_id":"dummy"}',
        encoding="utf-8",
    )
    (runs_root / "ops").mkdir(parents=True, exist_ok=True)
    (runs_root / "ops" / "ops_report_latest.json").write_text("{}", encoding="utf-8")

    (state_root / "review_queue.jsonl").write_text(
        '{"status":"pending","run_id":"x"}\n',
        encoding="utf-8",
    )
    (state_root / "execution_queue.jsonl").write_text(
        '{"status":"pending","run_id":"x"}\n',
        encoding="utf-8",
    )
    (state_root / "execution_history.jsonl").write_text(
        '{"dry_run":false,"run_id":"x"}\n',
        encoding="utf-8",
    )
    (state_root / "decision_log.jsonl").write_text('{"run_id":"x"}\n', encoding="utf-8")

    (knowledge_root / "skill_candidates.jsonl").parent.mkdir(parents=True, exist_ok=True)
    (knowledge_root / "skill_candidates.jsonl").write_text('{"id":"x"}\n', encoding="utf-8")
    (knowledge_root / "skills_registry_history.jsonl").write_text(
        '{"id":"x"}\n',
        encoding="utf-8",
    )
    (knowledge_root / "skill_promotion_last_run.json").write_text("{}", encoding="utf-8")
    (knowledge_root / "skills_registry.csv").write_text(
        "skill_name,skill_key\n",
        encoding="utf-8",
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--config",
            str(config_path),
            "--initial-capital",
            "50000",
            "--reset-runtime",
            "--reset-knowledge",
            "--reset-watchlist",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert proc.returncode == 0, proc.stderr

    account = json.loads((state_root / "account_snapshot.json").read_text(encoding="utf-8"))
    assert account["cash"] == 50000.0
    assert account["total_asset"] == 50000.0
    assert account["stock_asset"] == 0.0
    assert account["cash_ratio"] == 1.0

    positions_text = (state_root / "current_positions.csv").read_text(encoding="utf-8-sig")
    assert positions_text.strip().startswith("ticker,name,shares,avg_cost,last_price")
    assert len(positions_text.strip().splitlines()) == 1

    watchlist_text = (state_root / "watchlist.csv").read_text(encoding="utf-8-sig")
    assert watchlist_text.strip() == "ticker,name,reason,added_at,priority,status"

    # Runtime artifacts should be cleared.
    assert not (runs_root / "2026-01-01").exists()
    assert (runs_root / "ops").exists()
    assert list((runs_root / "ops").glob("*")) == []

    assert (state_root / "review_queue.jsonl").read_text(encoding="utf-8") == ""
    assert (state_root / "execution_queue.jsonl").read_text(encoding="utf-8") == ""
    assert not (state_root / "execution_history.jsonl").exists()
    assert (state_root / "decision_log.jsonl").read_text(encoding="utf-8") == ""

    assert (knowledge_root / "skill_candidates.jsonl").read_text(encoding="utf-8") == ""
    assert (knowledge_root / "skills_registry_history.jsonl").read_text(encoding="utf-8") == ""
    assert not (knowledge_root / "skill_promotion_last_run.json").exists()
    assert (knowledge_root / "skills_registry.csv").exists()


def test_init_state_blocks_if_runtime_data_exists_without_reset(tmp_path: Path) -> None:
    config_path, _runs_root, state_root, _knowledge_root = _build_workspace(tmp_path)
    (state_root / "review_queue.jsonl").parent.mkdir(parents=True, exist_ok=True)
    (state_root / "review_queue.jsonl").write_text(
        '{"status":"pending","run_id":"x"}\n',
        encoding="utf-8",
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--config",
            str(config_path),
            "--initial-capital",
            "60000",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert proc.returncode != 0
    error_text = (proc.stderr or "") + (proc.stdout or "")
    assert "detected existing runtime data" in error_text


def test_init_state_seed_watchlist(tmp_path: Path) -> None:
    config_path, _runs_root, state_root, _knowledge_root = _build_workspace(tmp_path)
    seed_csv = tmp_path / "seed_watchlist.csv"
    with seed_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["股票代码", "名称", "reason"])
        writer.writeheader()
        writer.writerow({"股票代码": "600519", "名称": "贵州茅台", "reason": "core"})
        writer.writerow({"股票代码": "600519", "名称": "贵州茅台", "reason": "dup"})
        writer.writerow({"股票代码": "000333", "名称": "美的集团", "reason": "quality"})

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--config",
            str(config_path),
            "--initial-capital",
            "80000",
            "--seed-watchlist",
            str(seed_csv),
            "--force",
        ],
        cwd=tmp_path,
        text=True,
        capture_output=True,
    )
    assert proc.returncode == 0, proc.stderr

    with (state_root / "watchlist.csv").open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    assert rows[0]["ticker"] == "600519"
    assert rows[1]["ticker"] == "000333"
    assert rows[0]["status"] == "active"
