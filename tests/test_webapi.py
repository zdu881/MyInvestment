from __future__ import annotations

import json
import shutil
from pathlib import Path

from fastapi.testclient import TestClient

from webapi.main import create_app
from webapi.services.command_runner import CommandResult
from webapi.settings import AppSettings


def _build_test_workspace(tmp_path: Path) -> AppSettings:
    repo_root = Path(__file__).resolve().parents[1]
    shutil.copytree(repo_root / "runs", tmp_path / "runs")
    shutil.copytree(repo_root / "state", tmp_path / "state")
    shutil.copytree(repo_root / "knowledge", tmp_path / "knowledge")
    shutil.copytree(repo_root / "webui", tmp_path / "webui")
    shutil.copy2(repo_root / "agent_config.json", tmp_path / "agent_config.json")
    return AppSettings(
        root_dir=tmp_path,
        runs_root=tmp_path / "runs",
        state_root=tmp_path / "state",
        knowledge_root=tmp_path / "knowledge",
        config_path=tmp_path / "agent_config.json",
        command_timeout_sec=30,
        api_token="",
    )


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
