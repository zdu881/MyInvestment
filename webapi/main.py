from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .services.command_runner import CommandResult, run_command
from .services.file_repo import FileRepo
from .settings import AppSettings


class ReviewSubmitRequest(BaseModel):
    decision: str = Field(pattern="^(approve|hold|reject)$")
    reviewer: str = Field(min_length=1)
    note: str = Field(min_length=1)


class ExecutionSubmitRequest(BaseModel):
    executor: str = Field(min_length=1)
    dry_run: bool = False
    force: bool = False


class SchedulerOnceRequest(BaseModel):
    dry_run: bool = False
    skip_maintenance: bool = False
    skip_ops_report: bool = False
    skip_feedback: bool = False
    skip_skill_promotion: bool = False
    skip_alerts: bool = False
    skip_action_center: bool = False
    ops_on_idle: bool = False
    ops_days: int = Field(default=7, ge=1)
    feedback_days: int = Field(default=30, ge=1)


CommandRunner = Callable[[list[str], Path, int], CommandResult]


def _error(code: str, message: str, status: int = 400, details: Any | None = None) -> HTTPException:
    payload: dict[str, Any] = {"error_code": code, "message": message}
    if details is not None:
        payload["details"] = details
    return HTTPException(status_code=status, detail=payload)


def _deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _audit(
    repo: FileRepo,
    action: str,
    payload: dict[str, Any],
    user: str,
    command_result: CommandResult | None = None,
) -> None:
    row: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "action": action,
        "user": user,
        "payload": payload,
    }
    if command_result is not None:
        row["command"] = command_result.command
        row["exit_code"] = command_result.exit_code
        row["stdout_tail"] = command_result.stdout_tail
        row["stderr_tail"] = command_result.stderr_tail
    repo.append_jsonl(repo.state_root / "webui_audit_log.jsonl", row)


def create_app(
    settings: AppSettings | None = None,
    command_runner: CommandRunner | None = None,
) -> FastAPI:
    app = FastAPI(title="MyInvestment Web API", version="0.1.0")
    cfg = settings or AppSettings.from_env()
    repo = FileRepo(
        root_dir=cfg.root_dir,
        runs_root=cfg.runs_root,
        state_root=cfg.state_root,
        knowledge_root=cfg.knowledge_root,
        config_path=cfg.config_path,
    )
    runner = command_runner or run_command

    app.state.settings = cfg
    app.state.repo = repo
    app.state.runner = runner

    static_dir = cfg.root_dir / "webui" / "static"
    if static_dir.exists():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def require_auth(x_api_token: str | None = Header(default=None)) -> str:
        configured = str(cfg.api_token or "").strip()
        if not configured:
            return "anonymous"
        if x_api_token != configured:
            raise _error("unauthorized", "invalid or missing API token", status=401)
        return "token_user"

    @app.get("/health")
    def health() -> dict[str, str]:
        return {
            "status": "ok",
            "server_time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }

    @app.get("/")
    def web_ui_root() -> Any:
        index = static_dir / "index.html"
        if not index.exists():
            raise _error("not_found", "web UI index not found", status=404)
        return FileResponse(index)

    @app.get("/api/action-center")
    def get_action_center() -> dict[str, Any]:
        path = repo.runs_root / "ops" / "action_center_latest.json"
        data = repo.read_json(path, default={})
        if not data:
            raise _error("not_found", "action center report not found", status=404)
        return data

    @app.get("/api/ops/report")
    def get_ops_report() -> dict[str, Any]:
        path = repo.runs_root / "ops" / "ops_report_latest.json"
        data = repo.read_json(path, default={})
        if not data:
            raise _error("not_found", "ops report not found", status=404)
        return data

    @app.get("/api/alerts")
    def get_alerts() -> dict[str, Any]:
        path = repo.runs_root / "ops" / "alerts_latest.json"
        data = repo.read_json(path, default={})
        if not data:
            raise _error("not_found", "alerts report not found", status=404)
        return data

    @app.get("/api/alerts/events")
    def get_alert_events(limit: int = Query(default=100, ge=1, le=500)) -> dict[str, Any]:
        rows = repo.read_jsonl(repo.state_root / "alerts_events.jsonl")
        rows = sorted(rows, key=lambda x: str(x.get("timestamp", "")), reverse=True)
        return {"items": rows[:limit]}

    @app.get("/api/quality/latest")
    def get_quality_latest() -> dict[str, Any]:
        path = repo.runs_root / "ops" / "proposal_quality_latest.json"
        data = repo.read_json(path, default={})
        if not data:
            raise _error("not_found", "quality report not found", status=404)
        return data

    @app.get("/api/runs")
    def list_runs(
        trading_date: str | None = None,
        phase: str | None = Query(default=None, pattern="^(preopen|intraday|postclose|all)$"),
        status: str | None = Query(default=None, pattern="^(success|failed)$"),
        limit: int = Query(default=50, ge=1, le=200),
    ) -> dict[str, Any]:
        rows = repo.list_run_manifests()
        if trading_date:
            rows = [r for r in rows if str(r.get("trading_date", "")) == trading_date]
        if phase:
            rows = [r for r in rows if str(r.get("phase", "")) == phase]
        if status:
            rows = [r for r in rows if str(r.get("status", "")) == status]
        rows = rows[:limit]
        return {"items": rows}

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str) -> dict[str, Any]:
        run_dir = repo.find_run_dir(run_id)
        if run_dir is None:
            raise _error("not_found", f"run not found: {run_id}", status=404)
        manifest = repo.read_json(run_dir / "run_manifest.json", default={})
        if not manifest:
            raise _error("not_found", f"manifest not found for run: {run_id}", status=404)
        return manifest

    @app.get("/api/runs/{run_id}/artifacts")
    def list_run_artifacts(run_id: str) -> dict[str, Any]:
        run_dir = repo.find_run_dir(run_id)
        if run_dir is None:
            raise _error("not_found", f"run not found: {run_id}", status=404)
        manifest = repo.read_json(run_dir / "run_manifest.json", default={})
        if not manifest:
            raise _error("not_found", f"manifest not found for run: {run_id}", status=404)
        return {"items": repo.list_run_artifacts(run_dir, manifest)}

    @app.get("/api/runs/{run_id}/artifact-content")
    def get_artifact_content(run_id: str, artifact: str = Query(...)) -> dict[str, Any]:
        run_dir = repo.find_run_dir(run_id)
        if run_dir is None:
            raise _error("not_found", f"run not found: {run_id}", status=404)
        try:
            kind, content = repo.read_artifact_content(run_dir, artifact)
        except ValueError as exc:
            raise _error("bad_request", str(exc), status=400) from exc
        except FileNotFoundError as exc:
            raise _error("not_found", f"artifact not found: {artifact}", status=404) from exc
        return {"artifact": artifact, "kind": kind, "content": content}

    @app.get("/api/proposals/pending")
    def list_pending_proposals() -> dict[str, Any]:
        return {"items": repo.pending_review_items()}

    @app.get("/api/proposals/{run_id}")
    def get_proposal_detail(run_id: str) -> dict[str, Any]:
        payload = repo.load_proposal_bundle(run_id)
        if payload is None:
            raise _error("not_found", f"proposal not found for run: {run_id}", status=404)
        return payload

    @app.post("/api/reviews/{run_id}")
    def submit_review(
        run_id: str,
        body: ReviewSubmitRequest,
        user: str = Depends(require_auth),
    ) -> dict[str, Any]:
        if not repo.has_pending_review(run_id):
            raise _error("conflict", f"run {run_id} is not pending review", status=409)
        command = [
            sys.executable,
            "agent_review.py",
            "--decision",
            body.decision,
            "--run-id",
            run_id,
            "--reviewer",
            body.reviewer,
            "--note",
            body.note,
        ]
        result = runner(command, cfg.root_dir, cfg.command_timeout_sec)
        _audit(
            repo=repo,
            action="submit_review",
            payload=body.model_dump() | {"run_id": run_id},
            user=user,
            command_result=result,
        )
        output = {
            "ok": result.ok,
            "command": result.command,
            "exit_code": result.exit_code,
            "stdout_tail": result.stdout_tail,
            "stderr_tail": result.stderr_tail,
            "affected_run_id": run_id,
        }
        if not result.ok:
            raise _error("command_failed", "review command failed", status=500, details=output)
        return output

    @app.get("/api/executions/pending")
    def list_pending_executions() -> dict[str, Any]:
        return {"items": repo.pending_execution_items()}

    @app.post("/api/executions/{run_id}")
    def execute_run(
        run_id: str,
        body: ExecutionSubmitRequest,
        user: str = Depends(require_auth),
    ) -> dict[str, Any]:
        if not repo.has_pending_execution(run_id):
            raise _error("conflict", f"run {run_id} is not pending execution", status=409)
        command = [
            sys.executable,
            "agent_execute.py",
            "--run-id",
            run_id,
            "--executor",
            body.executor,
        ]
        if body.dry_run:
            command.append("--dry-run")
        if body.force:
            command.append("--force")
        result = runner(command, cfg.root_dir, cfg.command_timeout_sec)
        _audit(
            repo=repo,
            action="execute_run",
            payload=body.model_dump() | {"run_id": run_id},
            user=user,
            command_result=result,
        )
        output = {
            "ok": result.ok,
            "command": result.command,
            "exit_code": result.exit_code,
            "stdout_tail": result.stdout_tail,
            "stderr_tail": result.stderr_tail,
            "affected_run_id": run_id,
        }
        if not result.ok:
            raise _error("command_failed", "execution command failed", status=500, details=output)
        return output

    @app.post("/api/scheduler/once")
    def scheduler_once(
        body: SchedulerOnceRequest | None = None,
        user: str = Depends(require_auth),
    ) -> dict[str, Any]:
        req = body or SchedulerOnceRequest()
        command = [sys.executable, "agent_scheduler.py", "--once"]
        if req.dry_run:
            command.append("--dry-run")
        if req.skip_maintenance:
            command.append("--skip-maintenance")
        if req.skip_ops_report:
            command.append("--skip-ops-report")
        if req.skip_feedback:
            command.append("--skip-feedback")
        if req.skip_skill_promotion:
            command.append("--skip-skill-promotion")
        if req.skip_alerts:
            command.append("--skip-alerts")
        if req.skip_action_center:
            command.append("--skip-action-center")
        if req.ops_on_idle:
            command.append("--ops-on-idle")
        command.extend(["--ops-days", str(req.ops_days)])
        command.extend(["--feedback-days", str(req.feedback_days)])

        result = runner(command, cfg.root_dir, cfg.command_timeout_sec)
        _audit(
            repo=repo,
            action="scheduler_once",
            payload=req.model_dump(),
            user=user,
            command_result=result,
        )
        output = {
            "ok": result.ok,
            "command": result.command,
            "exit_code": result.exit_code,
            "stdout_tail": result.stdout_tail,
            "stderr_tail": result.stderr_tail,
        }
        if not result.ok:
            raise _error("command_failed", "scheduler command failed", status=500, details=output)
        return output

    @app.get("/api/config")
    def get_config() -> dict[str, Any]:
        data = repo.read_json(repo.config_path, default={})
        if not data:
            raise _error("not_found", "config not found", status=404)
        return data

    @app.patch("/api/config")
    def patch_config(
        patch: dict[str, Any],
        user: str = Depends(require_auth),
    ) -> dict[str, Any]:
        if not isinstance(patch, dict):
            raise _error("bad_request", "patch body must be object", status=400)
        current = repo.read_json(repo.config_path, default={})
        merged = _deep_merge(current, patch)
        repo.write_json(repo.config_path, merged)
        _audit(repo=repo, action="patch_config", payload=patch, user=user, command_result=None)
        return {
            "ok": True,
            "command": ["config_patch"],
            "exit_code": 0,
            "stdout_tail": json.dumps({"updated": True}, ensure_ascii=False),
            "stderr_tail": "",
        }

    @app.exception_handler(HTTPException)
    async def http_exception_handler(_, exc: HTTPException) -> JSONResponse:
        if isinstance(exc.detail, dict) and "error_code" in exc.detail:
            payload = exc.detail
        else:
            payload = {
                "error_code": "http_error",
                "message": str(exc.detail),
                "details": {"status_code": exc.status_code},
            }
        return JSONResponse(status_code=exc.status_code, content=payload)

    return app


app = create_app()
