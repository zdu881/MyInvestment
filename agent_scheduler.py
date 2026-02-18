#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lightweight scheduler wrapper for agent_system.py.

Usage examples:
- python3 agent_scheduler.py --once
- python3 agent_scheduler.py --once --dry-run

Recommended deployment:
- trigger this script every 5 minutes via cron/systemd timer.
- the script will execute due phases at most once per trading date.
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional


DEFAULT_SCHEDULE = {
    "preopen": "08:30",
    "intraday": "12:30",
    "postclose": "20:30",
}


def load_config(path: Path) -> Dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def parse_hhmm(value: str) -> Optional[tuple]:
    try:
        hh, mm = value.split(":", 1)
        return int(hh), int(mm)
    except Exception:
        return None


def pick_due_phase(now_local: datetime, schedule: Dict[str, str], executed: List[str]) -> Optional[str]:
    order = ["preopen", "intraday", "postclose"]
    due_phases: List[str] = []

    for phase in order:
        hhmm = parse_hhmm(str(schedule.get(phase, "")))
        if hhmm is None:
            continue
        hh, mm = hhmm
        due_time = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now_local >= due_time:
            due_phases.append(phase)

    for phase in order:
        if phase in due_phases and phase not in executed:
            return phase
    return None


def run_phase(phase: str, config_path: str, dry_run: bool) -> int:
    cmd = [sys.executable, "agent_system.py", "--phase", phase, "--config", config_path]
    if dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(cmd)
    return proc.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run due phase for agent system")
    parser.add_argument("--config", default="agent_config.json")
    parser.add_argument("--once", action="store_true", help="run at most one due phase")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = load_config(Path(args.config))
    tz_hours = int(cfg.get("timezone_offset_hours", 8))
    now_local = datetime.now(timezone(timedelta(hours=tz_hours)))
    trading_date = now_local.date().isoformat()

    runs_root = Path(cfg.get("paths", {}).get("runs_root", "runs"))
    lock_path = runs_root / "scheduler.lock"

    # Prevent concurrent scheduler executions.
    lock_fd = None
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(lock_fd, str(os.getpid()).encode("utf-8"))
    except FileExistsError:
        print("[INFO] scheduler is already running, skip this trigger")
        return 0

    try:
        state_path = runs_root / trading_date / "scheduler_state.json"
        state = load_config(state_path)
        executed = list(state.get("executed_phases", []))

        schedule = dict(DEFAULT_SCHEDULE)
        schedule.update(cfg.get("schedule", {}))

        due_phase = pick_due_phase(now_local, schedule, executed)
        if due_phase is None:
            print("[INFO] no due phase")
            return 0

        ret = run_phase(due_phase, args.config, args.dry_run)
        if ret != 0:
            print(f"[ERROR] phase failed: {due_phase}")
            return ret

        executed.append(due_phase)
        executed = [p for p in ["preopen", "intraday", "postclose"] if p in executed]

        write_json(
            state_path,
            {
                "trading_date": trading_date,
                "updated_at": now_local.isoformat(timespec="seconds"),
                "executed_phases": executed,
                "schedule": schedule,
            },
        )

        print(f"[INFO] executed phase: {due_phase}")
        return 0
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
        if lock_path.exists():
            lock_path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
