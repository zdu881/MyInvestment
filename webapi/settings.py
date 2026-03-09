from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = raw.strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _load_auth_tokens() -> dict[str, str]:
    tokens = {
        "viewer": os.getenv("MYINVEST_VIEWER_TOKEN", "").strip(),
        "reviewer": os.getenv("MYINVEST_REVIEWER_TOKEN", "").strip(),
        "executor": os.getenv("MYINVEST_EXECUTOR_TOKEN", "").strip(),
        "admin": os.getenv("MYINVEST_ADMIN_TOKEN", "").strip(),
    }
    legacy = os.getenv("MYINVEST_API_TOKEN", "").strip()
    if legacy and not tokens["admin"]:
        tokens["admin"] = legacy
    return {role: token for role, token in tokens.items() if token}


@dataclass(frozen=True)
class AppSettings:
    root_dir: Path
    runs_root: Path
    state_root: Path
    knowledge_root: Path
    config_path: Path
    command_timeout_sec: int = 120
    api_token: str = ""
    auth_required: bool = True
    auth_tokens: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def from_env(root_dir: Path | None = None) -> "AppSettings":
        base = Path(root_dir or os.getenv("MYINVEST_ROOT", ".")).resolve()
        runs = base / os.getenv("MYINVEST_RUNS_ROOT", "runs")
        state = base / os.getenv("MYINVEST_STATE_ROOT", "state")
        knowledge = base / os.getenv("MYINVEST_KNOWLEDGE_ROOT", "knowledge")
        config = base / os.getenv("MYINVEST_CONFIG_PATH", "agent_config.json")
        timeout = int(os.getenv("MYINVEST_COMMAND_TIMEOUT_SEC", "120"))
        token = os.getenv("MYINVEST_API_TOKEN", "")
        auth_required = _env_bool("MYINVEST_AUTH_REQUIRED", True)
        auth_tokens = _load_auth_tokens()
        return AppSettings(
            root_dir=base,
            runs_root=runs,
            state_root=state,
            knowledge_root=knowledge,
            config_path=config,
            command_timeout_sec=max(10, timeout),
            api_token=token,
            auth_required=auth_required,
            auth_tokens=auth_tokens,
        )
