from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

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
    skip_notifier: bool = False
    skip_action_center: bool = False
    ops_on_idle: bool = False
    ops_days: int = Field(default=7, ge=1)
    feedback_days: int = Field(default=30, ge=1)


class OnboardingInitRequest(BaseModel):
    initial_capital: float = Field(default=100000.0, gt=0)
    risk_profile: str = Field(default="defensive", min_length=1, max_length=64)
    max_single_weight: float | None = Field(default=None, gt=0, le=1)
    max_industry_weight: float | None = Field(default=None, gt=0, le=1)
    min_cash_ratio: float | None = Field(default=None, ge=0, lt=1)
    reset_runtime: bool = False
    reset_knowledge: bool = False
    reset_watchlist: bool = False
    seed_watchlist: str = Field(default="", max_length=4000)
    force: bool = False
    dry_run: bool = True


class AgentInteractRequest(BaseModel):
    mode: str = Field(pattern="^(ask|plan|operation)$")
    message: str = Field(default="", max_length=4000)
    confirm: bool = False
    operation_id: str | None = Field(default=None, max_length=80)
    operation_options: dict[str, Any] = Field(default_factory=dict)
    confirmation_id: str | None = Field(default=None, max_length=80)


CommandRunner = Callable[[list[str], Path, int], CommandResult]


ROLE_PERMISSIONS: dict[str, set[str]] = {
    "viewer": {"read", "agent_read"},
    "reviewer": {"read", "agent_read", "review"},
    "executor": {"read", "agent_read", "execute"},
    "admin": {
        "read",
        "agent_read",
        "review",
        "execute",
        "schedule",
        "config",
        "onboarding",
        "operation",
    },
}
ROLE_PRIORITY = {"viewer": 0, "reviewer": 1, "executor": 2, "admin": 3}
CONFIG_AWARE_SCRIPTS = {
    "agent_review.py",
    "agent_execute.py",
    "agent_scheduler.py",
    "agent_init_state.py",
}


@dataclass(frozen=True)
class AuthContext:
    role: str
    permissions: frozenset[str]


def _merge_auth_tokens(cfg: AppSettings) -> dict[str, str]:
    tokens = {role: token for role, token in (cfg.auth_tokens or {}).items() if str(token).strip()}
    legacy = str(cfg.api_token or "").strip()
    if legacy and "admin" not in tokens:
        tokens["admin"] = legacy
    return tokens


def _build_token_contexts(cfg: AppSettings) -> dict[str, AuthContext]:
    contexts: dict[str, AuthContext] = {}
    grouped: dict[str, set[str]] = {}
    for role, token in _merge_auth_tokens(cfg).items():
        grouped.setdefault(token, set()).add(role)

    for token, roles in grouped.items():
        permissions: set[str] = set()
        for role in roles:
            permissions.update(ROLE_PERMISSIONS.get(role, set()))
        primary_role = max(roles, key=lambda item: ROLE_PRIORITY.get(item, -1))
        contexts[token] = AuthContext(role=primary_role, permissions=frozenset(permissions))
    return contexts


def _inject_config_path(command: list[str], config_path: Path) -> list[str]:
    if len(command) < 2 or "--config" in command:
        return list(command)
    script_name = Path(command[1]).name
    if script_name not in CONFIG_AWARE_SCRIPTS:
        return list(command)
    return [command[0], command[1], "--config", str(config_path), *command[2:]]


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


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _agent_snapshot(repo: FileRepo) -> dict[str, Any]:
    action_center = repo.read_json(repo.runs_root / "ops" / "action_center_latest.json", default={})
    alerts = repo.read_json(repo.runs_root / "ops" / "alerts_latest.json", default={})
    ops_report = repo.read_json(repo.runs_root / "ops" / "ops_report_latest.json", default={})
    overview = action_center.get("overview", {}) if isinstance(action_center.get("overview"), dict) else {}
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "overview": {
            "health_score": _to_float(overview.get("health_score"), 0.0),
            "health_label": str(overview.get("health_label", "unknown")),
            "alert_status": str(overview.get("alert_status", "unknown")),
            "active_alert_count": _to_int(overview.get("active_alert_count"), 0),
            "pending_review_count": _to_int(overview.get("pending_review_count"), 0),
            "pending_execution_count": _to_int(overview.get("pending_execution_count"), 0),
        },
        "alerts": {
            "status": str(alerts.get("status", "unknown")),
            "active_alert_count": _to_int(alerts.get("active_alert_count"), 0),
            "active_alerts": alerts.get("active_alerts", []) if isinstance(alerts.get("active_alerts"), list) else [],
        },
        "ops_report": {
            "health_score": _to_float(ops_report.get("health_score"), 0.0),
            "health_label": str(ops_report.get("health_label", "unknown")),
            "window_start": str(ops_report.get("window_start", "")),
            "window_end": str(ops_report.get("window_end", "")),
        },
    }


def _top_alert_lines(snapshot: dict[str, Any], limit: int = 3) -> list[str]:
    alerts = snapshot.get("alerts", {})
    rows = alerts.get("active_alerts", []) if isinstance(alerts, dict) else []
    items: list[str] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        level = str(row.get("level", "warn"))
        check_id = str(row.get("check_id", "unknown_check"))
        message = str(row.get("message", ""))
        items.append(f"- [{level}] {check_id}: {message}")
    return items


def _build_ask_reply(message: str, snapshot: dict[str, Any]) -> str:
    overview = snapshot.get("overview", {})
    alerts = snapshot.get("alerts", {})
    lines = [
        f"问题: {message.strip()}",
        "",
        "系统当前快照:",
        (
            f"- health={overview.get('health_score', 0):.1f} "
            f"({overview.get('health_label', 'unknown')})"
        ),
        f"- alert_status={overview.get('alert_status', 'unknown')}, active_alerts={alerts.get('active_alert_count', 0)}",
        f"- pending_review={overview.get('pending_review_count', 0)}",
        f"- pending_execution={overview.get('pending_execution_count', 0)}",
        "",
    ]
    alert_lines = _top_alert_lines(snapshot)
    if alert_lines:
        lines.append("重点告警:")
        lines.extend(alert_lines)
    else:
        lines.append("重点告警: 无")
    lines.extend(
        [
            "",
            "可继续追问示例:",
            "- 请解释最高优先级告警",
            "- 给我今天先做哪三件事",
            "- 帮我准备 operation 模式命令",
        ]
    )
    return "\n".join(lines)


def _build_plan_reply(message: str, snapshot: dict[str, Any]) -> str:
    overview = snapshot.get("overview", {})
    pending_review = _to_int(overview.get("pending_review_count"), 0)
    pending_execution = _to_int(overview.get("pending_execution_count"), 0)
    active_alerts = _to_int(snapshot.get("alerts", {}).get("active_alert_count"), 0)

    lines = [
        f"规划目标: {message.strip()}",
        "",
        "建议执行顺序:",
    ]
    step = 1
    if active_alerts > 0:
        lines.append(f"{step}. 先处理告警（当前 {active_alerts} 条），优先级按 critical > warn。")
        step += 1
    if pending_review > 0:
        lines.append(f"{step}. 处理人工审核队列（当前 {pending_review} 条 pending review）。")
        step += 1
    if pending_execution > 0:
        lines.append(f"{step}. 检查并执行 execution 队列（当前 {pending_execution} 条 pending execution）。")
        step += 1
    lines.append(f"{step}. 运行一次 dry-run 调度确认状态收敛。")
    lines.extend(
        [
            "",
            "operation 模式建议输入:",
            "- run scheduler once",
            "- refresh alerts",
            "- refresh action center",
        ]
    )
    return "\n".join(lines)


def _to_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _agent_operation_specs() -> list[dict[str, Any]]:
    return [
        {
            "id": "scheduler_once",
            "label": "Run scheduler once",
            "i18n_key": "agent.operation.scheduler_once",
            "keywords": ["scheduler", "run once", "调度", "全流程"],
            "options": [
                {
                    "name": "dry_run",
                    "type": "bool",
                    "default": True,
                    "i18n_key": "agent.option.dry_run",
                },
                {
                    "name": "skip_maintenance",
                    "type": "bool",
                    "default": False,
                    "i18n_key": "agent.option.skip_maintenance",
                },
                {
                    "name": "skip_alerts",
                    "type": "bool",
                    "default": False,
                    "i18n_key": "agent.option.skip_alerts",
                },
                {
                    "name": "skip_notifier",
                    "type": "bool",
                    "default": False,
                    "i18n_key": "agent.option.skip_notifier",
                },
            ],
        },
        {
            "id": "refresh_alerts",
            "label": "Refresh alerts",
            "i18n_key": "agent.operation.refresh_alerts",
            "keywords": ["refresh alerts", "alerts", "告警"],
            "options": [],
        },
        {
            "id": "refresh_action_center",
            "label": "Refresh action center",
            "i18n_key": "agent.operation.refresh_action_center",
            "keywords": ["action center", "行动中心"],
            "options": [],
        },
        {
            "id": "generate_ops_report",
            "label": "Generate ops report",
            "i18n_key": "agent.operation.generate_ops_report",
            "keywords": ["ops report", "运维报告", "health report"],
            "options": [
                {
                    "name": "days",
                    "type": "int",
                    "default": 7,
                    "min": 1,
                    "max": 30,
                    "i18n_key": "agent.option.days",
                }
            ],
        },
        {
            "id": "run_notifier",
            "label": "Run notifier",
            "i18n_key": "agent.operation.run_notifier",
            "keywords": ["notifier", "ntfy", "推送"],
            "options": [
                {
                    "name": "dry_run",
                    "type": "bool",
                    "default": True,
                    "i18n_key": "agent.option.dry_run",
                }
            ],
        },
        {
            "id": "queue_maintenance",
            "label": "Run queue maintenance",
            "i18n_key": "agent.operation.queue_maintenance",
            "keywords": ["queue", "队列维护"],
            "options": [
                {
                    "name": "dry_run",
                    "type": "bool",
                    "default": True,
                    "i18n_key": "agent.option.dry_run",
                }
            ],
        },
    ]


def _agent_operation_public_specs() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for spec in _agent_operation_specs():
        items.append(
            {
                "id": spec["id"],
                "label": spec["label"],
                "i18n_key": spec.get("i18n_key", ""),
                "options": spec.get("options", []),
            }
        )
    return items


def _find_operation_spec(operation_id: str) -> dict[str, Any] | None:
    op_id = str(operation_id or "").strip()
    for spec in _agent_operation_specs():
        if spec["id"] == op_id:
            return spec
    return None


def _normalize_operation_options(spec: dict[str, Any], raw_options: dict[str, Any] | None) -> dict[str, Any]:
    values = raw_options if isinstance(raw_options, dict) else {}
    normalized: dict[str, Any] = {}
    for item in spec.get("options", []):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        typ = str(item.get("type", "")).strip().lower()
        default_value = item.get("default")
        raw_value = values.get(name, default_value)
        if typ == "bool":
            normalized[name] = _to_bool(raw_value, bool(default_value))
            continue
        if typ == "int":
            parsed = _to_int(raw_value, _to_int(default_value, 0))
            if "min" in item:
                parsed = max(parsed, _to_int(item.get("min"), parsed))
            if "max" in item:
                parsed = min(parsed, _to_int(item.get("max"), parsed))
            normalized[name] = parsed
            continue
        normalized[name] = str(raw_value if raw_value is not None else default_value or "")
    return normalized


def _build_operation_command(operation_id: str, raw_options: dict[str, Any] | None) -> dict[str, Any] | None:
    spec = _find_operation_spec(operation_id)
    if spec is None:
        return None
    options = _normalize_operation_options(spec, raw_options)
    op_id = spec["id"]
    command: list[str]

    if op_id == "scheduler_once":
        command = [sys.executable, "agent_scheduler.py", "--once"]
        if _to_bool(options.get("dry_run"), True):
            command.append("--dry-run")
        if _to_bool(options.get("skip_maintenance"), False):
            command.append("--skip-maintenance")
        if _to_bool(options.get("skip_alerts"), False):
            command.append("--skip-alerts")
        if _to_bool(options.get("skip_notifier"), False):
            command.append("--skip-notifier")
    elif op_id == "refresh_alerts":
        command = [sys.executable, "agent_alerts.py"]
    elif op_id == "refresh_action_center":
        command = [sys.executable, "agent_action_center.py"]
    elif op_id == "generate_ops_report":
        command = [sys.executable, "agent_ops_report.py", "--days", str(_to_int(options.get("days"), 7))]
    elif op_id == "run_notifier":
        command = [sys.executable, "agent_notifier.py", "--enabled"]
        if _to_bool(options.get("dry_run"), True):
            command.append("--dry-run")
    elif op_id == "queue_maintenance":
        command = [sys.executable, "agent_queue_maintenance.py"]
        if _to_bool(options.get("dry_run"), True):
            command.append("--dry-run")
    else:
        return None

    return {
        "id": spec["id"],
        "label": spec["label"],
        "i18n_key": spec.get("i18n_key", ""),
        "command": command,
        "options": options,
    }


def _detect_operation(message: str) -> dict[str, Any] | None:
    text = message.lower().strip()
    if not text:
        return None
    for spec in _agent_operation_specs():
        if any(str(keyword) in text for keyword in spec.get("keywords", [])):
            return _build_operation_command(spec["id"], {})
    return None


def _operation_help_text() -> str:
    names = [f"- {spec['id']}" for spec in _agent_operation_public_specs()]
    return "\n".join(
        [
            "未识别到可执行操作。",
            "可用 operation_id:",
            *names,
            "",
            "也可继续输入自然语言关键词，例如: refresh alerts",
        ]
    )


_OPERATION_CONFIRM_TTL_SEC = 300
_OPERATION_EXEC_COOLDOWN_SEC = 30


def _safe_parse_ts(text: str) -> datetime | None:
    value = str(text or "").strip()
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _operation_guard_path(repo: FileRepo) -> Path:
    return repo.state_root / "agent_operation_guard.json"


def _read_operation_guard(repo: FileRepo) -> dict[str, Any]:
    data = repo.read_json(_operation_guard_path(repo), default={})
    pending = data.get("pending", {})
    last_exec = data.get("last_exec", {})
    if not isinstance(pending, dict):
        pending = {}
    if not isinstance(last_exec, dict):
        last_exec = {}
    return {"pending": pending, "last_exec": last_exec}


def _write_operation_guard(repo: FileRepo, guard: dict[str, Any]) -> None:
    repo.write_json(_operation_guard_path(repo), guard)


def _operation_fingerprint(operation_id: str, options: dict[str, Any] | None) -> str:
    payload = json.dumps(options or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return f"{operation_id}|{payload}"


def _cleanup_operation_guard(guard: dict[str, Any], now_utc: datetime) -> None:
    pending = guard.get("pending", {})
    to_delete: list[str] = []
    for key, row in pending.items():
        if not isinstance(row, dict):
            to_delete.append(str(key))
            continue
        expires_at = _safe_parse_ts(str(row.get("expires_at", "")))
        if expires_at is None or expires_at < now_utc:
            to_delete.append(str(key))
    for key in to_delete:
        pending.pop(key, None)


def _issue_operation_confirmation(
    repo: FileRepo,
    *,
    operation_id: str,
    options: dict[str, Any],
    user: str,
) -> dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    guard = _read_operation_guard(repo)
    _cleanup_operation_guard(guard, now_utc)

    confirmation_id = uuid4().hex
    expires_at = now_utc + timedelta(seconds=_OPERATION_CONFIRM_TTL_SEC)
    guard["pending"][confirmation_id] = {
        "operation_id": operation_id,
        "fingerprint": _operation_fingerprint(operation_id, options),
        "created_at": now_utc.isoformat(timespec="seconds"),
        "expires_at": expires_at.isoformat(timespec="seconds"),
        "user": user,
    }
    _write_operation_guard(repo, guard)
    return {
        "required": True,
        "confirmation_id": confirmation_id,
        "expires_at": expires_at.isoformat(timespec="seconds"),
        "cooldown_sec": _OPERATION_EXEC_COOLDOWN_SEC,
    }


def _validate_and_consume_operation_confirmation(
    repo: FileRepo,
    *,
    confirmation_id: str,
    operation_id: str,
    options: dict[str, Any],
) -> tuple[bool, str]:
    now_utc = datetime.now(timezone.utc)
    guard = _read_operation_guard(repo)
    _cleanup_operation_guard(guard, now_utc)
    pending = guard.get("pending", {})
    row = pending.get(confirmation_id)
    if not isinstance(row, dict):
        _write_operation_guard(repo, guard)
        return False, "confirmation id is missing or expired; please preview again"

    expected_op = str(row.get("operation_id", ""))
    if expected_op != operation_id:
        pending.pop(confirmation_id, None)
        _write_operation_guard(repo, guard)
        return False, "confirmation does not match selected operation"

    expected_fingerprint = str(row.get("fingerprint", ""))
    actual_fingerprint = _operation_fingerprint(operation_id, options)
    if expected_fingerprint != actual_fingerprint:
        pending.pop(confirmation_id, None)
        _write_operation_guard(repo, guard)
        return False, "confirmation does not match selected operation options"

    pending.pop(confirmation_id, None)
    _write_operation_guard(repo, guard)
    return True, ""


def _check_operation_cooldown(
    repo: FileRepo,
    *,
    operation_id: str,
    options: dict[str, Any],
) -> tuple[bool, int]:
    now_utc = datetime.now(timezone.utc)
    guard = _read_operation_guard(repo)
    _cleanup_operation_guard(guard, now_utc)
    key = _operation_fingerprint(operation_id, options)
    last_exec = _safe_parse_ts(str(guard.get("last_exec", {}).get(key, "")))
    if last_exec is None:
        _write_operation_guard(repo, guard)
        return True, 0

    elapsed = int((now_utc - last_exec).total_seconds())
    remain = _OPERATION_EXEC_COOLDOWN_SEC - elapsed
    if remain > 0:
        _write_operation_guard(repo, guard)
        return False, remain
    _write_operation_guard(repo, guard)
    return True, 0


def _mark_operation_executed(
    repo: FileRepo,
    *,
    operation_id: str,
    options: dict[str, Any],
) -> None:
    guard = _read_operation_guard(repo)
    _cleanup_operation_guard(guard, datetime.now(timezone.utc))
    key = _operation_fingerprint(operation_id, options)
    guard["last_exec"][key] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _write_operation_guard(repo, guard)


def _operation_history_path(repo: FileRepo) -> Path:
    return repo.state_root / "agent_operation_history.jsonl"


def _append_operation_history(
    repo: FileRepo,
    *,
    user: str,
    message: str,
    operation_payload: dict[str, Any],
    command_result: CommandResult,
) -> None:
    row: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "user": user,
        "message": message,
        "operation_id": operation_payload.get("id", ""),
        "operation_label": operation_payload.get("label", ""),
        "operation_i18n_key": operation_payload.get("i18n_key", ""),
        "operation_options": operation_payload.get("options", {}),
        "command": command_result.command,
        "exit_code": command_result.exit_code,
        "ok": command_result.ok,
        "stdout_tail": command_result.stdout_tail,
        "stderr_tail": command_result.stderr_tail,
    }
    repo.append_jsonl(_operation_history_path(repo), row)


def create_app(
    settings: AppSettings | None = None,
    command_runner: CommandRunner | None = None,
) -> FastAPI:
    app = FastAPI(title="MyInvestment Web API", version="0.1.0")
    cfg = settings or AppSettings.from_env()
    token_contexts = _build_token_contexts(cfg)
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

    def _authenticate(x_api_token: str | None = Header(default=None)) -> AuthContext:
        if not cfg.auth_required:
            return AuthContext(role="admin", permissions=frozenset(ROLE_PERMISSIONS["admin"]))
        if not token_contexts:
            raise _error(
                "auth_not_configured",
                "API authentication is enabled but no tokens are configured",
                status=503,
            )
        token = str(x_api_token or "").strip()
        ctx = token_contexts.get(token)
        if ctx is None:
            raise _error("unauthorized", "invalid or missing API token", status=401)
        return ctx

    def require_permission(permission: str) -> Callable[[AuthContext], str]:
        def dependency(ctx: AuthContext = Depends(_authenticate)) -> str:
            if permission not in ctx.permissions:
                raise _error("forbidden", f"permission denied for {permission}", status=403)
            return ctx.role

        return dependency

    require_reader = require_permission("read")
    require_reviewer = require_permission("review")
    require_executor = require_permission("execute")
    require_scheduler = require_permission("schedule")
    require_config_admin = require_permission("config")
    require_onboarding_admin = require_permission("onboarding")

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
    def get_action_center(user: str = Depends(require_reader)) -> dict[str, Any]:
        path = repo.runs_root / "ops" / "action_center_latest.json"
        data = repo.read_json(path, default={})
        if not data:
            raise _error("not_found", "action center report not found", status=404)
        return data

    @app.get("/api/ops/report")
    def get_ops_report(user: str = Depends(require_reader)) -> dict[str, Any]:
        path = repo.runs_root / "ops" / "ops_report_latest.json"
        data = repo.read_json(path, default={})
        if not data:
            raise _error("not_found", "ops report not found", status=404)
        return data

    @app.get("/api/alerts")
    def get_alerts(user: str = Depends(require_reader)) -> dict[str, Any]:
        path = repo.runs_root / "ops" / "alerts_latest.json"
        data = repo.read_json(path, default={})
        if not data:
            raise _error("not_found", "alerts report not found", status=404)
        return data

    @app.get("/api/alerts/events")
    def get_alert_events(
        limit: int = Query(default=100, ge=1, le=500),
        user: str = Depends(require_reader),
    ) -> dict[str, Any]:
        rows = repo.read_jsonl(repo.state_root / "alerts_events.jsonl")
        rows = sorted(rows, key=lambda x: str(x.get("timestamp", "")), reverse=True)
        return {"items": rows[:limit]}

    @app.get("/api/quality/latest")
    def get_quality_latest(user: str = Depends(require_reader)) -> dict[str, Any]:
        path = repo.runs_root / "ops" / "proposal_quality_latest.json"
        data = repo.read_json(path, default={})
        if not data:
            raise _error("not_found", "quality report not found", status=404)
        return data

    @app.get("/api/agent/operations")
    def list_agent_operations(user: str = Depends(require_reader)) -> dict[str, Any]:
        return {"items": _agent_operation_public_specs()}

    @app.get("/api/agent/operations/history")
    def list_agent_operation_history(
        limit: int = Query(default=100, ge=1, le=500),
        user: str = Depends(require_reader),
    ) -> dict[str, Any]:
        rows = repo.read_jsonl(_operation_history_path(repo))
        rows = sorted(rows, key=lambda x: str(x.get("timestamp", "")), reverse=True)
        return {"items": rows[:limit]}

    @app.get("/api/runs")
    def list_runs(
        trading_date: str | None = None,
        phase: str | None = Query(default=None, pattern="^(preopen|intraday|postclose|all)$"),
        status: str | None = Query(default=None, pattern="^(success|failed)$"),
        limit: int = Query(default=50, ge=1, le=200),
        user: str = Depends(require_reader),
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
    def get_run(run_id: str, user: str = Depends(require_reader)) -> dict[str, Any]:
        run_dir = repo.find_run_dir(run_id)
        if run_dir is None:
            raise _error("not_found", f"run not found: {run_id}", status=404)
        manifest = repo.read_json(run_dir / "run_manifest.json", default={})
        if not manifest:
            raise _error("not_found", f"manifest not found for run: {run_id}", status=404)
        return manifest

    @app.get("/api/runs/{run_id}/artifacts")
    def list_run_artifacts(run_id: str, user: str = Depends(require_reader)) -> dict[str, Any]:
        run_dir = repo.find_run_dir(run_id)
        if run_dir is None:
            raise _error("not_found", f"run not found: {run_id}", status=404)
        manifest = repo.read_json(run_dir / "run_manifest.json", default={})
        if not manifest:
            raise _error("not_found", f"manifest not found for run: {run_id}", status=404)
        return {"items": repo.list_run_artifacts(run_dir, manifest)}

    @app.get("/api/runs/{run_id}/artifact-content")
    def get_artifact_content(
        run_id: str,
        artifact: str = Query(...),
        user: str = Depends(require_reader),
    ) -> dict[str, Any]:
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
    def list_pending_proposals(user: str = Depends(require_reader)) -> dict[str, Any]:
        return {"items": repo.pending_review_items()}

    @app.get("/api/proposals/{run_id}")
    def get_proposal_detail(run_id: str, user: str = Depends(require_reader)) -> dict[str, Any]:
        payload = repo.load_proposal_bundle(run_id)
        if payload is None:
            raise _error("not_found", f"proposal not found for run: {run_id}", status=404)
        return payload

    @app.post("/api/reviews/{run_id}")
    def submit_review(
        run_id: str,
        body: ReviewSubmitRequest,
        user: str = Depends(require_reviewer),
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
        result = runner(_inject_config_path(command, cfg.config_path), cfg.root_dir, cfg.command_timeout_sec)
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
    def list_pending_executions(user: str = Depends(require_reader)) -> dict[str, Any]:
        return {"items": repo.pending_execution_items()}

    @app.post("/api/executions/{run_id}")
    def execute_run(
        run_id: str,
        body: ExecutionSubmitRequest,
        user: str = Depends(require_executor),
    ) -> dict[str, Any]:
        runtime_cfg = repo.read_json(repo.config_path, default={})
        execution_cfg = (
            runtime_cfg.get("execution", {})
            if isinstance(runtime_cfg.get("execution", {}), dict)
            else {}
        )
        manual_only = _to_bool(execution_cfg.get("manual_only"), False)
        if manual_only and not body.dry_run:
            payload = body.model_dump() | {"run_id": run_id, "blocked_reason": "manual_only"}
            _audit(
                repo=repo,
                action="execute_run_blocked",
                payload=payload,
                user=user,
                command_result=None,
            )
            raise _error(
                "manual_only",
                "execution is manual-only; set dry_run=true and place broker orders manually",
                status=409,
            )

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
        result = runner(_inject_config_path(command, cfg.config_path), cfg.root_dir, cfg.command_timeout_sec)
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
        user: str = Depends(require_scheduler),
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
        if req.skip_notifier:
            command.append("--skip-notifier")
        if req.skip_action_center:
            command.append("--skip-action-center")
        if req.ops_on_idle:
            command.append("--ops-on-idle")
        command.extend(["--ops-days", str(req.ops_days)])
        command.extend(["--feedback-days", str(req.feedback_days)])

        result = runner(_inject_config_path(command, cfg.config_path), cfg.root_dir, cfg.command_timeout_sec)
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

    @app.post("/api/onboarding/init")
    def onboarding_init(
        body: OnboardingInitRequest,
        user: str = Depends(require_onboarding_admin),
    ) -> dict[str, Any]:
        command = [
            sys.executable,
            "agent_init_state.py",
            "--initial-capital",
            str(body.initial_capital),
            "--risk-profile",
            body.risk_profile,
        ]
        if body.max_single_weight is not None:
            command.extend(["--max-single-weight", str(body.max_single_weight)])
        if body.max_industry_weight is not None:
            command.extend(["--max-industry-weight", str(body.max_industry_weight)])
        if body.min_cash_ratio is not None:
            command.extend(["--min-cash-ratio", str(body.min_cash_ratio)])
        if body.reset_runtime:
            command.append("--reset-runtime")
        if body.reset_knowledge:
            command.append("--reset-knowledge")
        if body.reset_watchlist:
            command.append("--reset-watchlist")
        seed_watchlist = str(body.seed_watchlist or "").strip()
        if seed_watchlist:
            command.extend(["--seed-watchlist", seed_watchlist])
        if body.force:
            command.append("--force")
        if body.dry_run:
            command.append("--dry-run")

        result = runner(_inject_config_path(command, cfg.config_path), cfg.root_dir, cfg.command_timeout_sec)
        _audit(
            repo=repo,
            action="onboarding_init",
            payload=body.model_dump(),
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
            raise _error("command_failed", "onboarding init failed", status=500, details=output)
        return output

    @app.post("/api/agent/interact")
    def agent_interact(
        body: AgentInteractRequest,
        auth: AuthContext = Depends(_authenticate),
    ) -> dict[str, Any]:
        mode = str(body.mode).strip().lower()
        user = auth.role
        message = str(body.message or "").strip()
        snapshot = _agent_snapshot(repo)
        command_result: CommandResult | None = None
        operation_payload: dict[str, Any] | None = None
        confirmation_payload: dict[str, Any] | None = None
        ok = True

        if mode == "ask":
            if "agent_read" not in auth.permissions:
                raise _error("forbidden", "permission denied for agent_read", status=403)
            if not message:
                raise _error("bad_request", "message is required in ask mode", status=400)
            reply = _build_ask_reply(message, snapshot)
        elif mode == "plan":
            if "agent_read" not in auth.permissions:
                raise _error("forbidden", "permission denied for agent_read", status=403)
            if not message:
                raise _error("bad_request", "message is required in plan mode", status=400)
            reply = _build_plan_reply(message, snapshot)
        else:
            if "operation" not in auth.permissions:
                raise _error("forbidden", "permission denied for operation", status=403)
            selected_operation_id = str(body.operation_id or "").strip()
            selected_operation_options = body.operation_options if isinstance(body.operation_options, dict) else {}

            operation_spec = None
            if selected_operation_id:
                operation_spec = _build_operation_command(selected_operation_id, selected_operation_options)
            if operation_spec is None:
                operation_spec = _detect_operation(message)

            if operation_spec is None:
                ok = False
                reply = _operation_help_text()
            else:
                command_to_run = _inject_config_path(operation_spec["command"], cfg.config_path)
                operation_payload = {
                    "id": operation_spec["id"],
                    "label": operation_spec["label"],
                    "i18n_key": operation_spec.get("i18n_key", ""),
                    "options": operation_spec.get("options", {}),
                    "command": command_to_run,
                    "executed": False,
                }
                if body.confirm:
                    confirmation_id = str(body.confirmation_id or "").strip()
                    if not confirmation_id:
                        ok = False
                        reply = "operation execute requires confirmation_id; please preview first"
                        confirmation_payload = {
                            "required": True,
                            "missing": True,
                            "cooldown_sec": _OPERATION_EXEC_COOLDOWN_SEC,
                        }
                    else:
                        confirmation_ok, confirmation_msg = _validate_and_consume_operation_confirmation(
                            repo=repo,
                            confirmation_id=confirmation_id,
                            operation_id=operation_spec["id"],
                            options=operation_spec.get("options", {}),
                        )
                        if not confirmation_ok:
                            ok = False
                            reply = confirmation_msg
                            confirmation_payload = {
                                "required": True,
                                "invalid": True,
                                "cooldown_sec": _OPERATION_EXEC_COOLDOWN_SEC,
                            }
                        else:
                            cooldown_ok, cooldown_remain_sec = _check_operation_cooldown(
                                repo=repo,
                                operation_id=operation_spec["id"],
                                options=operation_spec.get("options", {}),
                            )
                            if not cooldown_ok:
                                ok = False
                                reply = f"operation is in cooldown, retry after {cooldown_remain_sec}s"
                                confirmation_payload = {
                                    "required": True,
                                    "cooldown": True,
                                    "retry_after_sec": cooldown_remain_sec,
                                }
                            else:
                                command_result = runner(command_to_run, cfg.root_dir, cfg.command_timeout_sec)
                                operation_payload["executed"] = True
                                operation_payload["exit_code"] = command_result.exit_code
                                operation_payload["stdout_tail"] = command_result.stdout_tail
                                operation_payload["stderr_tail"] = command_result.stderr_tail
                                ok = command_result.ok
                                if command_result.ok:
                                    _mark_operation_executed(
                                        repo=repo,
                                        operation_id=operation_spec["id"],
                                        options=operation_spec.get("options", {}),
                                    )
                                _append_operation_history(
                                    repo=repo,
                                    user=user,
                                    message=message,
                                    operation_payload=operation_payload,
                                    command_result=command_result,
                                )
                                reply_lines = [
                                    f"已执行操作: {operation_spec['label']}",
                                    f"exit_code={command_result.exit_code}",
                                ]
                                if command_result.stdout_tail:
                                    reply_lines.append("")
                                    reply_lines.append("stdout_tail:")
                                    reply_lines.append(command_result.stdout_tail)
                                if command_result.stderr_tail:
                                    reply_lines.append("")
                                    reply_lines.append("stderr_tail:")
                                    reply_lines.append(command_result.stderr_tail)
                                reply = "\n".join(reply_lines)
                else:
                    confirmation_payload = _issue_operation_confirmation(
                        repo=repo,
                        operation_id=operation_spec["id"],
                        options=operation_spec.get("options", {}),
                        user=user,
                    )
                    reply = "\n".join(
                        [
                            f"已识别操作: {operation_spec['label']}",
                            f"command: {' '.join(command_to_run)}",
                            f"confirmation_id: {confirmation_payload['confirmation_id']}",
                            f"expires_at: {confirmation_payload['expires_at']}",
                            "当前为预览模式。勾选 confirm 后携带 confirmation_id 执行。",
                        ]
                    )

        _audit(
            repo=repo,
            action="agent_interact",
            payload={
                "mode": mode,
                "message": message,
                "confirm": body.confirm,
                "confirmation_id": body.confirmation_id,
                "ok": ok,
                "operation_options": body.operation_options,
                "operation_id": operation_payload["id"] if operation_payload else "",
            },
            user=user,
            command_result=command_result,
        )

        return {
            "ok": ok,
            "mode": mode,
            "reply": reply,
            "operation": operation_payload,
            "confirmation": confirmation_payload,
            "snapshot": snapshot,
        }

    @app.get("/api/config")
    def get_config(user: str = Depends(require_reader)) -> dict[str, Any]:
        data = repo.read_json(repo.config_path, default={})
        if not data:
            raise _error("not_found", "config not found", status=404)
        return data

    @app.patch("/api/config")
    def patch_config(
        patch: dict[str, Any],
        user: str = Depends(require_config_admin),
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
