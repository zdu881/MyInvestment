from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_RUNTIME_PATHS = {
    "runs_root": "runs",
    "state_root": "state",
    "knowledge_root": "knowledge",
    "decision_log": "state/decision_log.jsonl",
    "step1_csv": "candidates.csv",
    "step2_csv": "candidates_step2.csv",
}


def load_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return {} if default is None else default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def resolve_path(root_dir: Path, raw_value: str | Path | None, default: str) -> Path:
    value = Path(raw_value) if raw_value not in {None, ""} else Path(default)
    if not value.is_absolute():
        value = (root_dir / value).resolve()
    return value


@dataclass(frozen=True)
class RuntimePaths:
    config_path: Path
    root_dir: Path
    runs_root: Path
    state_root: Path
    knowledge_root: Path
    decision_log_path: Path
    step1_csv_path: Path
    step2_csv_path: Path


def resolve_runtime_paths(
    config_path: Path,
    overrides: dict[str, str | Path | None] | None = None,
) -> tuple[dict[str, Any], RuntimePaths]:
    cfg_path = config_path.resolve()
    cfg = load_json(cfg_path, default={})
    root_dir = cfg_path.parent
    paths_cfg = cfg.get("paths", {}) if isinstance(cfg.get("paths", {}), dict) else {}
    override_values = overrides or {}

    def pick(key: str) -> Path:
        override_value = override_values.get(key)
        raw_value = override_value if override_value not in {None, ""} else paths_cfg.get(key)
        default_value = DEFAULT_RUNTIME_PATHS[key]
        return resolve_path(root_dir, raw_value, default_value)

    paths = RuntimePaths(
        config_path=cfg_path,
        root_dir=root_dir,
        runs_root=pick("runs_root"),
        state_root=pick("state_root"),
        knowledge_root=pick("knowledge_root"),
        decision_log_path=pick("decision_log"),
        step1_csv_path=pick("step1_csv"),
        step2_csv_path=pick("step2_csv"),
    )
    return cfg, paths
