#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate an operational health report for the investment agent system.

The report summarizes:
- run reliability (success/failed by phase)
- proposal and review pipeline status
- queue backlog (review / execution)
- execution quality (cost and constraint compliance)
- skill candidate growth
"""

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Window:
    start_date: date
    end_date: date


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).strip().replace(",", "")
        if text in {"", "-", "--", "None", "nan", "NaN"}:
            return default
        return float(text)
    except Exception:
        return default


def parse_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def parse_iso_datetime(value: str) -> Optional[datetime]:
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def in_window_by_date(value: str, window: Window) -> bool:
    d = parse_date(value)
    if d is None:
        return False
    return window.start_date <= d <= window.end_date


def in_window_by_ts(value: str, window: Window) -> bool:
    dt = parse_iso_datetime(value)
    if dt is None:
        return False
    d = dt.date()
    return window.start_date <= d <= window.end_date


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def collect_run_manifests(runs_root: Path, window: Window) -> List[Tuple[Path, Dict[str, Any]]]:
    manifests: List[Tuple[Path, Dict[str, Any]]] = []
    if not runs_root.exists():
        return manifests

    for date_dir in sorted(runs_root.glob("*")):
        if not date_dir.is_dir():
            continue
        if date_dir.name == "ops":
            continue
        if not in_window_by_date(date_dir.name, window):
            continue

        for run_dir in sorted(date_dir.glob("*")):
            if not run_dir.is_dir():
                continue
            manifest_path = run_dir / "run_manifest.json"
            if not manifest_path.exists():
                continue
            manifest = read_json(manifest_path)
            manifests.append((run_dir, manifest))

    return manifests


def compute_health_score(
    run_success_rate: float,
    failed_runs: int,
    pending_reviews: int,
    pending_execs: int,
    constraint_violation_execs: int,
) -> float:
    score = 100.0

    if run_success_rate < 0.95:
        score -= min(30.0, (0.95 - run_success_rate) / 0.95 * 30.0)
    score -= min(20.0, pending_reviews * 2.0)
    score -= min(15.0, pending_execs * 3.0)
    score -= min(20.0, failed_runs * 4.0)
    score -= min(15.0, constraint_violation_execs * 3.0)

    if score < 0:
        score = 0.0
    if score > 100:
        score = 100.0
    return round(score, 2)


def score_label(score: float) -> str:
    if score >= 85:
        return "healthy"
    if score >= 70:
        return "watch"
    return "risk"


def build_markdown(summary: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Agent Ops Health Report")
    lines.append("")
    lines.append(f"- generated_at: {summary['generated_at']}")
    lines.append(f"- window: {summary['window_start']} to {summary['window_end']}")
    lines.append(f"- health_score: {summary['health_score']} ({summary['health_label']})")
    lines.append("")

    lines.append("## Run Reliability")
    lines.append("")
    lines.append(
        f"- total_runs: {summary['run_stats']['total_runs']} | success: {summary['run_stats']['success_runs']} | failed: {summary['run_stats']['failed_runs']}"
    )
    lines.append(
        f"- success_rate: {summary['run_stats']['success_rate']:.2%}"
    )
    for phase, stats in summary["phase_stats"].items():
        lines.append(
            f"- phase={phase}: total={stats['total']}, failed={stats['failed']}, success_rate={stats['success_rate']:.2%}"
        )

    lines.append("")
    lines.append("## Proposal Pipeline")
    lines.append("")
    lines.append(
        f"- proposals: {summary['proposal_stats']['total']} | rebalance={summary['proposal_stats']['rebalance']} | hold={summary['proposal_stats']['hold']} | watch={summary['proposal_stats']['watch']}"
    )
    lines.append(
        f"- reviewed: {summary['proposal_stats']['reviewed']} | pending_review: {summary['queue_stats']['pending_review']}"
    )
    lines.append(
        f"- pending_execution: {summary['queue_stats']['pending_execution']}"
    )

    lines.append("")
    lines.append("## Execution Quality")
    lines.append("")
    lines.append(
        f"- executions: {summary['execution_stats']['total']} | avg_cost_ratio={summary['execution_stats']['avg_cost_ratio']:.4%} | total_cost={summary['execution_stats']['total_cost']:.2f}"
    )
    lines.append(
        f"- constraint_violation_executions: {summary['execution_stats']['constraint_violations']}"
    )

    lines.append("")
    lines.append("## Skill Growth")
    lines.append("")
    lines.append(
        f"- new_skill_candidates: {summary['skill_stats']['new_candidates']}"
    )

    lines.append("")
    lines.append("## Recent Failures")
    lines.append("")
    failures = summary.get("recent_failures", [])
    if not failures:
        lines.append("- none")
    else:
        for row in failures[:10]:
            lines.append(
                f"- {row['trading_date']} run={row['run_id']} phase={row['phase']} error={row['error_summary']}"
            )

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate ops health report")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--runs-root", default="runs")
    parser.add_argument("--state-root", default="state")
    parser.add_argument("--knowledge-root", default="knowledge")
    parser.add_argument("--output-md", default="runs/ops/ops_report_latest.md")
    parser.add_argument("--output-json", default="runs/ops/ops_report_latest.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    today = date.today()
    start = today - timedelta(days=max(0, args.days - 1))
    window = Window(start_date=start, end_date=today)

    runs_root = Path(args.runs_root)
    state_root = Path(args.state_root)
    knowledge_root = Path(args.knowledge_root)

    manifests = collect_run_manifests(runs_root, window)

    run_total = len(manifests)
    run_failed = 0
    phase_stats: Dict[str, Dict[str, Any]] = {
        "preopen": {"total": 0, "failed": 0},
        "intraday": {"total": 0, "failed": 0},
        "postclose": {"total": 0, "failed": 0},
    }

    proposals_total = 0
    proposal_decisions = {"rebalance": 0, "hold": 0, "watch": 0}
    reviewed_count = 0

    recent_failures: List[Dict[str, Any]] = []

    for run_dir, manifest in manifests:
        status = str(manifest.get("status", ""))
        if status != "success":
            run_failed += 1
            recent_failures.append(
                {
                    "trading_date": str(manifest.get("trading_date", "")),
                    "run_id": str(manifest.get("run_id", run_dir.name)),
                    "phase": str(manifest.get("phase", "")),
                    "error_summary": str(manifest.get("error_summary", ""))[:240],
                }
            )

        phase = str(manifest.get("phase", ""))
        if phase == "all":
            for st in manifest.get("steps", []):
                ph = str(st.get("phase", ""))
                if ph not in phase_stats:
                    continue
                phase_stats[ph]["total"] += 1
                if str(st.get("status", "")) != "success":
                    phase_stats[ph]["failed"] += 1
        elif phase in phase_stats:
            phase_stats[phase]["total"] += 1
            if status != "success":
                phase_stats[phase]["failed"] += 1

        proposal_path = run_dir / "allocation_proposal.json"
        if proposal_path.exists():
            proposal = read_json(proposal_path)
            proposals_total += 1
            decision = str(proposal.get("decision", "watch"))
            if decision in proposal_decisions:
                proposal_decisions[decision] += 1
            if str(proposal.get("review_status", "")).strip().lower() in {
                "approved",
                "hold",
                "reject",
                "reviewed",
            }:
                reviewed_count += 1

    run_success = run_total - run_failed
    run_success_rate = (run_success / run_total) if run_total > 0 else 1.0

    # Queue stats
    review_queue = read_jsonl(state_root / "review_queue.jsonl")
    pending_review = sum(1 for r in review_queue if str(r.get("status", "")).strip().lower() == "pending")

    execution_queue = read_jsonl(state_root / "execution_queue.jsonl")
    pending_execution = sum(
        1 for r in execution_queue if str(r.get("status", "")).strip().lower() == "pending"
    )

    # Execution quality in window
    execution_history = read_jsonl(state_root / "execution_history.jsonl")
    execution_rows = [
        r for r in execution_history if in_window_by_ts(str(r.get("timestamp", "")), window)
    ]

    total_exec_cost = 0.0
    total_exec_cost_ratio = 0.0
    cost_count = 0
    constraint_violation_execs = 0

    for row in execution_rows:
        costs = row.get("execution_costs", {}) if isinstance(row.get("execution_costs"), dict) else {}
        total_exec_cost += safe_float(costs.get("total_execution_cost"), 0.0)
        total_exec_cost_ratio += safe_float(costs.get("cost_ratio_total_asset"), 0.0)
        cost_count += 1

        cv = row.get("constraint_validation", {}) if isinstance(row.get("constraint_validation"), dict) else {}
        if cv and not bool(cv.get("compliant", True)):
            constraint_violation_execs += 1

    avg_cost_ratio = (total_exec_cost_ratio / cost_count) if cost_count > 0 else 0.0

    # Skill growth in window
    skill_candidates = read_jsonl(knowledge_root / "skill_candidates.jsonl")
    skill_rows = [r for r in skill_candidates if in_window_by_ts(str(r.get("created_at", "")), window)]

    for ph in phase_stats:
        total = phase_stats[ph]["total"]
        failed = phase_stats[ph]["failed"]
        phase_stats[ph]["success_rate"] = ((total - failed) / total) if total > 0 else 1.0

    health = compute_health_score(
        run_success_rate=run_success_rate,
        failed_runs=run_failed,
        pending_reviews=pending_review,
        pending_execs=pending_execution,
        constraint_violation_execs=constraint_violation_execs,
    )

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "window_start": window.start_date.isoformat(),
        "window_end": window.end_date.isoformat(),
        "health_score": health,
        "health_label": score_label(health),
        "run_stats": {
            "total_runs": run_total,
            "success_runs": run_success,
            "failed_runs": run_failed,
            "success_rate": run_success_rate,
        },
        "phase_stats": phase_stats,
        "proposal_stats": {
            "total": proposals_total,
            "rebalance": proposal_decisions["rebalance"],
            "hold": proposal_decisions["hold"],
            "watch": proposal_decisions["watch"],
            "reviewed": reviewed_count,
        },
        "queue_stats": {
            "pending_review": pending_review,
            "pending_execution": pending_execution,
        },
        "execution_stats": {
            "total": len(execution_rows),
            "avg_cost_ratio": avg_cost_ratio,
            "total_cost": round(total_exec_cost, 4),
            "constraint_violations": constraint_violation_execs,
        },
        "skill_stats": {
            "new_candidates": len(skill_rows),
        },
        "recent_failures": recent_failures,
    }

    md_text = build_markdown(summary)
    output_md = Path(args.output_md)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text(md_text, encoding="utf-8")

    output_json = Path(args.output_json)
    write_json(output_json, summary)

    print(f"[INFO] health_score={summary['health_score']} ({summary['health_label']})")
    print(f"[INFO] report_md={output_md}")
    print(f"[INFO] report_json={output_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
