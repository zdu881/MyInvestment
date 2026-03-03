from __future__ import annotations

import json
import shutil
from pathlib import Path

from fastapi.testclient import TestClient

from webapi.main import create_app
from webapi.services.command_runner import CommandResult
from webapi.settings import AppSettings


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _ensure_demo_runtime_data(settings: AppSettings) -> None:
    runs_root = settings.runs_root
    state_root = settings.state_root
    runs_root.mkdir(parents=True, exist_ok=True)
    (runs_root / "ops").mkdir(parents=True, exist_ok=True)
    state_root.mkdir(parents=True, exist_ok=True)

    has_manifest = any(runs_root.glob("*/**/run_manifest.json"))
    if not has_manifest:
        trading_date = "2026-01-02"
        review_run_id = "demo-review-run-0001"
        exec_run_id = "demo-exec-run-0001"

        review_dir = runs_root / trading_date / review_run_id
        exec_dir = runs_root / trading_date / exec_run_id
        review_dir.mkdir(parents=True, exist_ok=True)
        exec_dir.mkdir(parents=True, exist_ok=True)

        _write_json(
            review_dir / "run_manifest.json",
            {
                "run_id": review_run_id,
                "trading_date": trading_date,
                "as_of_ts": "2026-01-02T20:30:00+08:00",
                "phase": "postclose",
                "dry_run": True,
                "status": "success",
                "steps": [{"phase": "postclose", "status": "success", "error": ""}],
                "artifacts": [
                    f"runs/{trading_date}/{review_run_id}/allocation_proposal.json",
                    f"runs/{trading_date}/{review_run_id}/advice_report.md",
                    f"runs/{trading_date}/{review_run_id}/stock_research.jsonl",
                    f"runs/{trading_date}/{review_run_id}/rebalance_actions.csv",
                ],
            },
        )
        _write_json(
            exec_dir / "run_manifest.json",
            {
                "run_id": exec_run_id,
                "trading_date": trading_date,
                "as_of_ts": "2026-01-02T20:31:00+08:00",
                "phase": "postclose",
                "dry_run": True,
                "status": "success",
                "steps": [{"phase": "postclose", "status": "success", "error": ""}],
                "artifacts": [
                    f"runs/{trading_date}/{exec_run_id}/allocation_proposal.json",
                    f"runs/{trading_date}/{exec_run_id}/review_decision.json",
                    f"runs/{trading_date}/{exec_run_id}/execution_orders.csv",
                    f"runs/{trading_date}/{exec_run_id}/advice_report.md",
                ],
            },
        )

        _write_json(
            review_dir / "allocation_proposal.json",
            {
                "run_id": review_run_id,
                "proposal_id": "proposal-demo-review",
                "trading_date": trading_date,
                "decision": "watch",
                "review_status": "pending",
                "evidence_completeness": 0.8,
                "target_weights": {"600519": 0.2},
                "new_portfolio": [{"ticker": "600519", "industry": "消费"}],
            },
        )
        _write_text(
            review_dir / "advice_report.md",
            "# Advice Report\n\n- demo review run\n",
        )
        _write_text(
            review_dir / "stock_research.jsonl",
            '{"ticker":"600519","name":"贵州茅台","risk_flags":[],"confidence":0.82}\n',
        )
        _write_text(
            review_dir / "rebalance_actions.csv",
            "ticker,name,action,current_weight,target_weight,delta_weight\n"
            "600519,贵州茅台,BUY,0.0000,0.2000,0.2000\n",
        )

        _write_json(
            exec_dir / "allocation_proposal.json",
            {
                "run_id": exec_run_id,
                "proposal_id": "proposal-demo-exec",
                "trading_date": trading_date,
                "decision": "rebalance",
                "review_status": "approved",
                "target_weights": {"600000": 0.2},
                "new_portfolio": [{"ticker": "600000", "industry": "银行"}],
            },
        )
        _write_json(
            exec_dir / "review_decision.json",
            {
                "timestamp": "2026-01-02T20:31:30+08:00",
                "run_id": exec_run_id,
                "decision_id": "proposal-demo-exec",
                "reviewer": "tester",
                "human_decision": "approve",
                "final_action": "approved_rebalance",
                "proposal_decision": "rebalance",
                "note": "demo approved",
            },
        )
        _write_text(
            exec_dir / "advice_report.md",
            "# Advice Report\n\n- demo execution run\n",
        )
        _write_text(
            exec_dir / "stock_research.jsonl",
            '{"ticker":"600000","name":"浦发银行","risk_flags":[],"confidence":0.9}\n',
        )
        _write_text(
            exec_dir / "execution_orders.csv",
            "order_id,run_id,proposal_id,ticker,name,action,current_weight,target_weight,delta_weight,status,created_at\n"
            "demo-order-1,demo-exec-run-0001,proposal-demo-exec,600000,浦发银行,BUY,0.0000,0.2000,0.2000,pending,2026-01-02T20:31:30+08:00\n",
        )
        _write_text(
            exec_dir / "rebalance_actions.csv",
            "ticker,name,action,current_weight,target_weight,delta_weight\n"
            "600000,浦发银行,BUY,0.0000,0.2000,0.2000\n",
        )
        _write_text(
            exec_dir / "candidates_step2.csv",
            "ticker,name,current_price\n600000,浦发银行,10.0\n",
        )

    if not (state_root / "account_snapshot.json").exists():
        _write_json(
            state_root / "account_snapshot.json",
            {
                "cash": 100000.0,
                "total_asset": 100000.0,
                "stock_asset": 0.0,
                "cash_ratio": 1.0,
                "max_single_weight": 0.3,
                "max_industry_weight": 0.5,
                "min_cash_ratio": 0.1,
                "risk_profile": "defensive",
            },
        )
    if not (state_root / "current_positions.csv").exists():
        _write_text(
            state_root / "current_positions.csv",
            "ticker,name,shares,avg_cost,last_price,market_value,weight,industry,updated_at\n",
        )
    if not (state_root / "watchlist.csv").exists():
        _write_text(
            state_root / "watchlist.csv",
            "ticker,name,reason,added_at,priority,status\n",
        )

    if not (state_root / "review_queue.jsonl").exists() or not (state_root / "review_queue.jsonl").read_text(
        encoding="utf-8"
    ).strip():
        _write_text(
            state_root / "review_queue.jsonl",
            (
                '{"timestamp":"2026-01-02T20:30:05+08:00","run_id":"demo-review-run-0001",'
                '"proposal_id":"proposal-demo-review","status":"pending","suggested_decision":"watch",'
                '"required_action":"manual_review","advice_report_path":"runs/2026-01-02/demo-review-run-0001/advice_report.md",'
                '"proposal_path":"runs/2026-01-02/demo-review-run-0001/allocation_proposal.json"}\n'
            ),
        )

    if not (state_root / "execution_queue.jsonl").exists() or not (
        state_root / "execution_queue.jsonl"
    ).read_text(encoding="utf-8").strip():
        _write_text(
            state_root / "execution_queue.jsonl",
            (
                '{"queue_id":"demo-queue-1","run_id":"demo-exec-run-0001","proposal_id":"proposal-demo-exec",'
                '"status":"pending","created_at":"2026-01-02T20:31:31+08:00","created_by":"tester","order_count":1,'
                '"execution_orders_path":"runs/2026-01-02/demo-exec-run-0001/execution_orders.csv"}\n'
            ),
        )

    if not (state_root / "review_history.jsonl").exists():
        _write_text(state_root / "review_history.jsonl", "")
    if not (state_root / "alerts_events.jsonl").exists():
        _write_text(
            state_root / "alerts_events.jsonl",
            (
                '{"timestamp":"2026-01-02T20:32:00+08:00","event":"opened","check_id":"demo_warn",'
                '"level":"warn","value":1,"message":"demo alert","source":"tests"}\n'
            ),
        )

    ops_dir = runs_root / "ops"
    if not (ops_dir / "ops_report_latest.json").exists():
        _write_json(
            ops_dir / "ops_report_latest.json",
            {
                "generated_at": "2026-01-02T20:35:00+08:00",
                "window_start": "2025-12-27",
                "window_end": "2026-01-02",
                "health_score": 90.0,
                "health_label": "healthy",
                "queue_stats": {
                    "pending_review": 1,
                    "pending_execution": 1,
                    "stale_review": 0,
                    "stale_execution": 0,
                    "oldest_pending_review_hours": 1.0,
                    "oldest_pending_execution_hours": 1.0,
                },
            },
        )
    if not (ops_dir / "action_center_latest.json").exists():
        _write_json(
            ops_dir / "action_center_latest.json",
            {
                "generated_at": "2026-01-02T20:35:00+08:00",
                "overview": {
                    "health_score": 90.0,
                    "health_label": "healthy",
                    "alert_status": "warn",
                    "active_alert_count": 1,
                    "pending_review_count": 1,
                    "pending_execution_count": 1,
                    "quality_sample_size": 0,
                    "quality_avg_score": 0.0,
                },
                "pending_review": [],
                "pending_execution": [],
                "active_alerts": [],
            },
        )
    if not (ops_dir / "alerts_latest.json").exists():
        _write_json(
            ops_dir / "alerts_latest.json",
            {
                "generated_at": "2026-01-02T20:35:00+08:00",
                "status": "warn",
                "active_alert_count": 1,
                "active_critical_count": 0,
                "active_warn_count": 1,
                "active_alerts": [
                    {
                        "check_id": "demo_warn",
                        "level": "warn",
                        "value": 1,
                        "message": "demo alert",
                        "source": "tests",
                    }
                ],
                "event_count": 1,
                "events": [],
                "sources": {},
            },
        )
    if not (ops_dir / "proposal_quality_latest.json").exists():
        _write_json(
            ops_dir / "proposal_quality_latest.json",
            {
                "generated_at": "2026-01-02T20:35:00+08:00",
                "window_start": "2025-12-04",
                "window_end": "2026-01-02",
                "sample_size": 0,
                "avg_quality_score": 0.0,
                "avg_cost_ratio": 0.0,
                "model_feedback": {
                    "generated_at": "2026-01-02T20:35:00+08:00",
                    "quality_sample_size": 0,
                    "average_quality_score": 0.0,
                    "average_cost_ratio": 0.0,
                    "min_confidence_buy": 0.6,
                    "max_new_positions_override": 3,
                    "ticker_penalties": {},
                    "risk_flag_penalties": {},
                    "window_days": 30,
                },
                "quality_rows": [],
            },
        )
    if not (state_root / "model_feedback.json").exists():
        _write_json(
            state_root / "model_feedback.json",
            {
                "generated_at": "2026-01-02T20:35:00+08:00",
                "quality_sample_size": 0,
                "average_quality_score": 0.0,
                "average_cost_ratio": 0.0,
                "min_confidence_buy": 0.6,
                "max_new_positions_override": 3,
                "ticker_penalties": {},
                "risk_flag_penalties": {},
                "window_days": 30,
            },
        )


def _build_test_workspace(tmp_path: Path) -> AppSettings:
    repo_root = Path(__file__).resolve().parents[1]
    shutil.copytree(repo_root / "runs", tmp_path / "runs")
    shutil.copytree(repo_root / "state", tmp_path / "state")
    shutil.copytree(repo_root / "knowledge", tmp_path / "knowledge")
    shutil.copytree(repo_root / "webui", tmp_path / "webui")
    shutil.copy2(repo_root / "agent_config.json", tmp_path / "agent_config.json")
    settings = AppSettings(
        root_dir=tmp_path,
        runs_root=tmp_path / "runs",
        state_root=tmp_path / "state",
        knowledge_root=tmp_path / "knowledge",
        config_path=tmp_path / "agent_config.json",
        command_timeout_sec=30,
        api_token="",
    )
    _ensure_demo_runtime_data(settings)
    return settings


def _build_client(tmp_path: Path) -> tuple[TestClient, list[list[str]], AppSettings]:
    calls: list[list[str]] = []

    def fake_runner(command: list[str], _cwd: Path, _timeout: int) -> CommandResult:
        calls.append(command)
        return CommandResult(
            command=command,
            exit_code=0,
            stdout_tail="ok",
            stderr_tail="",
        )

    settings = _build_test_workspace(tmp_path)
    app = create_app(settings=settings, command_runner=fake_runner)
    return TestClient(app), calls, settings


def _copy_runtime_scripts(repo_root: Path, workspace_root: Path) -> None:
    script_names = [
        "agent_review.py",
        "agent_execute.py",
        "agent_scheduler.py",
        "agent_queue_maintenance.py",
        "agent_ops_report.py",
        "agent_feedback.py",
        "agent_skill_manager.py",
        "agent_alerts.py",
        "agent_action_center.py",
        "agent_notifier.py",
        "agent_system.py",
    ]
    for name in script_names:
        shutil.copy2(repo_root / name, workspace_root / name)


def test_read_endpoints(tmp_path: Path) -> None:
    client, _, _ = _build_client(tmp_path)

    root = client.get("/")
    assert root.status_code == 200
    assert "MyInvestment Console" in root.text
    static_js = client.get("/static/app.js")
    assert static_js.status_code == 200
    locale_zh = client.get("/static/locales/zh-CN.json")
    assert locale_zh.status_code == 200
    locale_en = client.get("/static/locales/en-US.json")
    assert locale_en.status_code == 200

    assert client.get("/health").status_code == 200
    assert client.get("/api/action-center").status_code == 200
    assert client.get("/api/ops/report").status_code == 200
    assert client.get("/api/alerts").status_code == 200
    assert client.get("/api/alerts/events").status_code == 200
    assert client.get("/api/quality/latest").status_code == 200
    assert client.get("/api/agent/operations").status_code == 200
    history = client.get("/api/agent/operations/history")
    assert history.status_code == 200
    assert isinstance(history.json()["items"], list)

    runs = client.get("/api/runs?limit=10")
    assert runs.status_code == 200
    assert len(runs.json()["items"]) > 0


def test_run_detail_and_artifacts(tmp_path: Path) -> None:
    client, _, _ = _build_client(tmp_path)

    runs = client.get("/api/runs?limit=1").json()["items"]
    run_id = runs[0]["run_id"]

    run = client.get(f"/api/runs/{run_id}")
    assert run.status_code == 200
    assert run.json()["run_id"] == run_id

    artifacts = client.get(f"/api/runs/{run_id}/artifacts")
    assert artifacts.status_code == 200
    items = artifacts.json()["items"]
    assert len(items) > 0

    artifact_name = items[0]["name"]
    content = client.get(
        f"/api/runs/{run_id}/artifact-content",
        params={"artifact": artifact_name},
    )
    assert content.status_code == 200
    assert content.json()["artifact"] == artifact_name


def test_proposal_and_pending_lists(tmp_path: Path) -> None:
    client, _, _ = _build_client(tmp_path)

    pending = client.get("/api/proposals/pending")
    assert pending.status_code == 200
    items = pending.json()["items"]
    assert len(items) > 0

    run_id = items[0]["run_id"]
    detail = client.get(f"/api/proposals/{run_id}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["run_id"] == run_id
    assert "proposal" in body
    assert "advice_report" in body


def test_mutation_endpoints_and_config_patch(tmp_path: Path) -> None:
    client, calls, settings = _build_client(tmp_path)

    pending_review = client.get("/api/proposals/pending").json()["items"]
    assert len(pending_review) > 0
    review_run_id = pending_review[0]["run_id"]

    review_resp = client.post(
        f"/api/reviews/{review_run_id}",
        json={"decision": "hold", "reviewer": "tester", "note": "test-note"},
    )
    assert review_resp.status_code == 200
    assert "agent_review.py" in " ".join(calls[-1])

    pending_execution = client.get("/api/executions/pending").json()["items"]
    assert len(pending_execution) > 0
    exec_run_id = pending_execution[0]["run_id"]

    exec_resp = client.post(
        f"/api/executions/{exec_run_id}",
        json={"executor": "tester", "dry_run": True, "force": False},
    )
    assert exec_resp.status_code == 200
    assert "agent_execute.py" in " ".join(calls[-1])

    sched_resp = client.post(
        "/api/scheduler/once",
        json={"dry_run": True, "skip_maintenance": True, "skip_alerts": True},
    )
    assert sched_resp.status_code == 200
    assert "agent_scheduler.py" in " ".join(calls[-1])

    patch_resp = client.patch("/api/config", json={"action_center": {"max_alerts": 5}})
    assert patch_resp.status_code == 200
    cfg = client.get("/api/config").json()
    assert cfg["action_center"]["max_alerts"] == 5

    audit_path = settings.state_root / "webui_audit_log.jsonl"
    assert audit_path.exists()
    assert len(audit_path.read_text(encoding="utf-8").strip().splitlines()) >= 4


def test_agent_interact_modes(tmp_path: Path) -> None:
    client, calls, settings = _build_client(tmp_path)

    ask_resp = client.post(
        "/api/agent/interact",
        json={"mode": "ask", "message": "当前状态如何"},
    )
    assert ask_resp.status_code == 200
    ask_payload = ask_resp.json()
    assert ask_payload["mode"] == "ask"
    assert ask_payload["ok"] is True
    assert "系统当前快照" in ask_payload["reply"]

    plan_resp = client.post(
        "/api/agent/interact",
        json={"mode": "plan", "message": "给我今天的计划"},
    )
    assert plan_resp.status_code == 200
    plan_payload = plan_resp.json()
    assert plan_payload["mode"] == "plan"
    assert "1." in plan_payload["reply"]

    preview_resp = client.post(
        "/api/agent/interact",
        json={"mode": "operation", "message": "refresh alerts", "confirm": False},
    )
    assert preview_resp.status_code == 200
    preview_payload = preview_resp.json()
    assert preview_payload["ok"] is True
    assert preview_payload["operation"]["executed"] is False
    confirmation_id = preview_payload["confirmation"]["confirmation_id"]
    assert confirmation_id

    before_calls = len(calls)
    no_confirm_resp = client.post(
        "/api/agent/interact",
        json={"mode": "operation", "message": "refresh alerts", "confirm": True},
    )
    assert no_confirm_resp.status_code == 200
    assert no_confirm_resp.json()["ok"] is False

    execute_resp = client.post(
        "/api/agent/interact",
        json={
            "mode": "operation",
            "message": "refresh alerts",
            "confirm": True,
            "confirmation_id": confirmation_id,
        },
    )
    assert execute_resp.status_code == 200
    execute_payload = execute_resp.json()
    assert execute_payload["ok"] is True
    assert execute_payload["operation"]["executed"] is True
    assert len(calls) == before_calls + 1
    assert "agent_alerts.py" in " ".join(calls[-1])

    preview_again_resp = client.post(
        "/api/agent/interact",
        json={"mode": "operation", "message": "refresh alerts", "confirm": False},
    )
    assert preview_again_resp.status_code == 200
    confirmation_id_again = preview_again_resp.json()["confirmation"]["confirmation_id"]
    cooldown_resp = client.post(
        "/api/agent/interact",
        json={
            "mode": "operation",
            "message": "refresh alerts",
            "confirm": True,
            "confirmation_id": confirmation_id_again,
        },
    )
    assert cooldown_resp.status_code == 200
    assert cooldown_resp.json()["ok"] is False
    assert "cooldown" in str(cooldown_resp.json()["reply"]).lower()

    scheduler_preview_resp = client.post(
        "/api/agent/interact",
        json={
            "mode": "operation",
            "message": "",
            "confirm": False,
            "operation_id": "scheduler_once",
            "operation_options": {
                "dry_run": True,
                "skip_maintenance": True,
                "skip_alerts": True,
                "skip_notifier": True,
            },
        },
    )
    assert scheduler_preview_resp.status_code == 200
    scheduler_confirmation_id = scheduler_preview_resp.json()["confirmation"]["confirmation_id"]

    structured_resp = client.post(
        "/api/agent/interact",
        json={
            "mode": "operation",
            "message": "",
            "confirm": True,
            "operation_id": "scheduler_once",
            "confirmation_id": scheduler_confirmation_id,
            "operation_options": {
                "dry_run": True,
                "skip_maintenance": True,
                "skip_alerts": True,
                "skip_notifier": True,
            },
        },
    )
    assert structured_resp.status_code == 200
    structured_payload = structured_resp.json()
    assert structured_payload["ok"] is True
    cmd = calls[-1]
    assert "agent_scheduler.py" in " ".join(cmd)
    assert "--once" in cmd
    assert "--dry-run" in cmd
    assert "--skip-maintenance" in cmd
    assert "--skip-alerts" in cmd
    assert "--skip-notifier" in cmd

    unknown_resp = client.post(
        "/api/agent/interact",
        json={"mode": "operation", "message": "do something impossible", "confirm": True},
    )
    assert unknown_resp.status_code == 200
    assert unknown_resp.json()["ok"] is False

    history_resp = client.get("/api/agent/operations/history?limit=20")
    assert history_resp.status_code == 200
    history_items = history_resp.json()["items"]
    assert len(history_items) >= 2
    assert any(item.get("operation_id") == "refresh_alerts" for item in history_items)
    assert any(item.get("operation_id") == "scheduler_once" for item in history_items)
    assert all("exit_code" in item for item in history_items)

    audit_path = settings.state_root / "webui_audit_log.jsonl"
    rows = [
        json.loads(line)
        for line in audit_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(row.get("action") == "agent_interact" for row in rows)


def test_real_command_runner_flow(tmp_path: Path) -> None:
    settings = _build_test_workspace(tmp_path)
    repo_root = Path(__file__).resolve().parents[1]
    _copy_runtime_scripts(repo_root, settings.root_dir)

    client = TestClient(create_app(settings=settings))

    pending_review = client.get("/api/proposals/pending").json()["items"]
    assert len(pending_review) > 0
    review_run_id = pending_review[0]["run_id"]
    review_resp = client.post(
        f"/api/reviews/{review_run_id}",
        json={"decision": "hold", "reviewer": "integration", "note": "integration-check"},
    )
    assert review_resp.status_code == 200

    pending_execution = client.get("/api/executions/pending").json()["items"]
    assert len(pending_execution) > 0
    exec_run_id = pending_execution[0]["run_id"]
    exec_resp = client.post(
        f"/api/executions/{exec_run_id}",
        json={"executor": "integration", "dry_run": True, "force": False},
    )
    assert exec_resp.status_code == 200

    scheduler_resp = client.post(
        "/api/scheduler/once",
        json={"dry_run": True, "skip_maintenance": True},
    )
    assert scheduler_resp.status_code == 200
