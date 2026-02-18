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


def run_queue_maintenance(cfg: Dict, dry_run: bool) -> int:
    tz_hours = int(cfg.get("timezone_offset_hours", 8))
    maintenance_cfg = cfg.get("maintenance", {}) if isinstance(cfg.get("maintenance", {}), dict) else {}

    cmd = [
        sys.executable,
        "agent_queue_maintenance.py",
        "--timezone-offset-hours",
        str(tz_hours),
        "--review-stale-hours",
        str(float(maintenance_cfg.get("review_stale_hours", 24.0))),
        "--execution-stale-hours",
        str(float(maintenance_cfg.get("execution_stale_hours", 24.0))),
        "--retain-days",
        str(float(maintenance_cfg.get("retain_days", 14.0))),
    ]
    if dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(cmd)
    return proc.returncode


def run_ops_report(days: int) -> int:
    cmd = [sys.executable, "agent_ops_report.py", "--days", str(days)]
    proc = subprocess.run(cmd)
    return proc.returncode


def run_quality_feedback(cfg: Dict, days: int, dry_run: bool) -> int:
    paths = cfg.get("paths", {}) if isinstance(cfg.get("paths", {}), dict) else {}
    cmd = [
        sys.executable,
        "agent_feedback.py",
        "--days",
        str(max(1, int(days))),
        "--runs-root",
        str(paths.get("runs_root", "runs")),
        "--state-root",
        str(paths.get("state_root", "state")),
    ]
    if dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(cmd)
    return proc.returncode


def run_skill_promotion(cfg: Dict, dry_run: bool) -> int:
    paths = cfg.get("paths", {}) if isinstance(cfg.get("paths", {}), dict) else {}
    promote_cfg = (
        cfg.get("skill_promotion", {})
        if isinstance(cfg.get("skill_promotion", {}), dict)
        else {}
    )
    cmd = [
        sys.executable,
        "agent_skill_manager.py",
        "--knowledge-root",
        str(paths.get("knowledge_root", "knowledge")),
        "--min-occurrences",
        str(int(promote_cfg.get("min_occurrences", 3))),
        "--quality-threshold",
        str(float(promote_cfg.get("quality_threshold", 0.62))),
        "--promote-delta",
        str(float(promote_cfg.get("promote_delta", 0.08))),
        "--owner",
        str(promote_cfg.get("owner", "auto_agent")),
    ]
    if dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(cmd)
    return proc.returncode


def run_alerts(cfg: Dict, dry_run: bool) -> int:
    paths = cfg.get("paths", {}) if isinstance(cfg.get("paths", {}), dict) else {}
    alert_cfg = cfg.get("ops_alerts", {}) if isinstance(cfg.get("ops_alerts", {}), dict) else {}

    runs_root = str(paths.get("runs_root", "runs"))
    state_root = str(paths.get("state_root", "state"))
    ops_report_json = f"{runs_root}/ops/ops_report_latest.json"
    quality_report_json = f"{runs_root}/ops/proposal_quality_latest.json"
    model_feedback_json = f"{state_root}/model_feedback.json"

    cmd = [
        sys.executable,
        "agent_alerts.py",
        "--ops-report-json",
        ops_report_json,
        "--quality-report-json",
        quality_report_json,
        "--model-feedback-json",
        model_feedback_json,
        "--state-root",
        state_root,
        "--health-score-warn",
        str(float(alert_cfg.get("health_score_warn", 80.0))),
        "--health-score-critical",
        str(float(alert_cfg.get("health_score_critical", 70.0))),
        "--stale-review-warn",
        str(int(alert_cfg.get("stale_review_warn", 1))),
        "--stale-execution-warn",
        str(int(alert_cfg.get("stale_execution_warn", 1))),
        "--oldest-review-hours-warn",
        str(float(alert_cfg.get("oldest_review_hours_warn", 24.0))),
        "--oldest-review-hours-critical",
        str(float(alert_cfg.get("oldest_review_hours_critical", 48.0))),
        "--oldest-execution-hours-warn",
        str(float(alert_cfg.get("oldest_execution_hours_warn", 24.0))),
        "--oldest-execution-hours-critical",
        str(float(alert_cfg.get("oldest_execution_hours_critical", 48.0))),
        "--quality-score-warn",
        str(float(alert_cfg.get("quality_score_warn", 0.6))),
        "--quality-score-critical",
        str(float(alert_cfg.get("quality_score_critical", 0.5))),
        "--quality-min-sample-size",
        str(int(alert_cfg.get("quality_min_sample_size", 3))),
        "--feedback-min-confidence-warn",
        str(float(alert_cfg.get("feedback_min_confidence_warn", 0.78))),
        "--report-stale-hours-warn",
        str(float(alert_cfg.get("report_stale_hours_warn", 8.0))),
        "--report-stale-hours-critical",
        str(float(alert_cfg.get("report_stale_hours_critical", 24.0))),
        "--reminder-hours",
        str(float(alert_cfg.get("reminder_hours", 24.0))),
    ]
    if dry_run:
        cmd.append("--dry-run")
    proc = subprocess.run(cmd)
    return proc.returncode


def run_action_center(cfg: Dict) -> int:
    paths = cfg.get("paths", {}) if isinstance(cfg.get("paths", {}), dict) else {}
    center_cfg = cfg.get("action_center", {}) if isinstance(cfg.get("action_center", {}), dict) else {}
    cmd = [
        sys.executable,
        "agent_action_center.py",
        "--state-root",
        str(paths.get("state_root", "state")),
        "--runs-root",
        str(paths.get("runs_root", "runs")),
        "--max-review-items",
        str(int(center_cfg.get("max_review_items", 8))),
        "--max-execution-items",
        str(int(center_cfg.get("max_execution_items", 8))),
        "--max-alerts",
        str(int(center_cfg.get("max_alerts", 8))),
    ]
    proc = subprocess.run(cmd)
    return proc.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="Run due phase for agent system")
    parser.add_argument("--config", default="agent_config.json")
    parser.add_argument("--once", action="store_true", help="run at most one due phase")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-maintenance", action="store_true")
    parser.add_argument("--skip-ops-report", action="store_true")
    parser.add_argument("--skip-feedback", action="store_true")
    parser.add_argument("--skip-skill-promotion", action="store_true")
    parser.add_argument("--skip-alerts", action="store_true")
    parser.add_argument("--skip-action-center", action="store_true")
    parser.add_argument("--ops-on-idle", action="store_true")
    parser.add_argument("--ops-days", type=int, default=7)
    parser.add_argument("--feedback-days", type=int, default=30)
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
        phase_executed = False
        if due_phase is None:
            print("[INFO] no due phase")
        else:
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
            phase_executed = True

            print(f"[INFO] executed phase: {due_phase}")

        if not args.skip_maintenance:
            ret = run_queue_maintenance(cfg, args.dry_run)
            if ret != 0:
                print("[ERROR] queue maintenance failed")
                return ret
            print("[INFO] queue maintenance completed")

        if not args.skip_feedback:
            feedback_cfg = cfg.get("feedback", {}) if isinstance(cfg.get("feedback", {}), dict) else {}
            feedback_days = int(feedback_cfg.get("quality_window_days", args.feedback_days))
            ret = run_quality_feedback(cfg, feedback_days, args.dry_run)
            if ret != 0:
                print("[ERROR] quality feedback generation failed")
                return ret
            print("[INFO] quality feedback refreshed")

        if not args.skip_skill_promotion:
            ret = run_skill_promotion(cfg, args.dry_run)
            if ret != 0:
                print("[ERROR] skill promotion failed")
                return ret
            print("[INFO] skill promotion refreshed")

        if not args.skip_ops_report and (phase_executed or args.ops_on_idle):
            ret = run_ops_report(max(1, int(args.ops_days)))
            if ret != 0:
                print("[ERROR] ops report generation failed")
                return ret
            print("[INFO] ops report refreshed")

        if not args.skip_alerts:
            ret = run_alerts(cfg, args.dry_run)
            if ret != 0:
                print("[ERROR] alert channel refresh failed")
                return ret
            print("[INFO] alert channel refreshed")

        if not args.skip_action_center:
            ret = run_action_center(cfg)
            if ret != 0:
                print("[ERROR] action center refresh failed")
                return ret
            print("[INFO] action center refreshed")

        return 0
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
        if lock_path.exists():
            lock_path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
