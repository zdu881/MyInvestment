from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppSettings:
    root_dir: Path
    runs_root: Path
    state_root: Path
    knowledge_root: Path
    config_path: Path
    command_timeout_sec: int = 120
    api_token: str = ""

    @staticmethod
    def from_env(root_dir: Path | None = None) -> "AppSettings":
        base = Path(root_dir or os.getenv("MYINVEST_ROOT", ".")).resolve()
        runs = base / os.getenv("MYINVEST_RUNS_ROOT", "runs")
        state = base / os.getenv("MYINVEST_STATE_ROOT", "state")
        knowledge = base / os.getenv("MYINVEST_KNOWLEDGE_ROOT", "knowledge")
        config = base / os.getenv("MYINVEST_CONFIG_PATH", "agent_config.json")
        timeout = int(os.getenv("MYINVEST_COMMAND_TIMEOUT_SEC", "120"))
        token = os.getenv("MYINVEST_API_TOKEN", "")
        return AppSettings(
            root_dir=base,
            runs_root=runs,
            state_root=state,
            knowledge_root=knowledge,
            config_path=config,
            command_timeout_sec=max(10, timeout),
            api_token=token,
        )
