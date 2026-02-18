#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate a single actionable operations dashboard for manual decision-making.
"""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List


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


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except Exception:
        return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build operations action center view")
    parser.add_argument("--state-root", default="state")
    parser.add_argument("--runs-root", default="runs")
    parser.add_argument("--max-review-items", type=int, default=8)
    parser.add_argument("--max-execution-items", type=int, default=8)
    parser.add_argument("--max-alerts", type=int, default=8)
    parser.add_argument("--output-md", default="runs/ops/action_center_latest.md")
    parser.add_argument("--output-json", default="runs/ops/action_center_latest.json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state_root = Path(args.state_root)
    runs_root = Path(args.runs_root)
    ops_root = runs_root / "ops"

    review_queue = read_jsonl(state_root / "review_queue.jsonl")
    execution_queue = read_jsonl(state_root / "execution_queue.jsonl")
    alerts = read_json(ops_root / "alerts_latest.json")
    ops_report = read_json(ops_root / "ops_report_latest.json")
    quality_report = read_json(ops_root / "proposal_quality_latest.json")

    pending_review = [
        r for r in review_queue if str(r.get("status", "")).strip().lower() == "pending"
    ]
    pending_execution = [
        r for r in execution_queue if str(r.get("status", "")).strip().lower() == "pending"
    ]
    pending_review = sorted(pending_review, key=lambda x: str(x.get("timestamp", "")))
    pending_execution = sorted(pending_execution, key=lambda x: str(x.get("created_at", "")))

    active_alerts = alerts.get("active_alerts", []) if isinstance(alerts.get("active_alerts"), list) else []
    active_alerts = sorted(
        active_alerts,
        key=lambda x: (0 if str(x.get("level", "")) == "critical" else 1, str(x.get("check_id", ""))),
    )

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "overview": {
            "health_score": ops_report.get("health_score"),
            "health_label": ops_report.get("health_label"),
            "alert_status": alerts.get("status", "unknown"),
            "active_alert_count": safe_int(alerts.get("active_alert_count"), 0),
            "pending_review_count": len(pending_review),
            "pending_execution_count": len(pending_execution),
            "quality_sample_size": safe_int(quality_report.get("sample_size"), 0),
            "quality_avg_score": quality_report.get("avg_quality_score"),
        },
        "pending_review": pending_review[: max(1, int(args.max_review_items))],
        "pending_execution": pending_execution[: max(1, int(args.max_execution_items))],
        "active_alerts": active_alerts[: max(1, int(args.max_alerts))],
    }

    lines: List[str] = []
    lines.append("# Agent Action Center")
    lines.append("")
    lines.append(f"- generated_at: {summary['generated_at']}")
    lines.append(
        f"- health: {summary['overview']['health_score']} ({summary['overview']['health_label']})"
    )
    lines.append(
        f"- alerts: {summary['overview']['alert_status']} / active={summary['overview']['active_alert_count']}"
    )
    lines.append(
        f"- pending_review={summary['overview']['pending_review_count']} | pending_execution={summary['overview']['pending_execution_count']}"
    )
    lines.append(
        f"- proposal_quality: sample={summary['overview']['quality_sample_size']} avg={summary['overview']['quality_avg_score']}"
    )
    lines.append("")
    lines.append("## Priority Alerts")
    lines.append("")
    if not summary["active_alerts"]:
        lines.append("- none")
    else:
        for row in summary["active_alerts"]:
            lines.append(
                f"- [{row.get('level', 'warn')}] {row.get('check_id', '')}: {row.get('message', '')}"
            )
    lines.append("")
    lines.append("## Pending Manual Reviews")
    lines.append("")
    if not summary["pending_review"]:
        lines.append("- none")
    else:
        for row in summary["pending_review"]:
            lines.append(
                f"- run={row.get('run_id', '')} proposal={row.get('proposal_id', '')} suggested={row.get('suggested_decision', '')} advice={row.get('advice_report_path', '')}"
            )
    lines.append("")
    lines.append("## Pending Executions")
    lines.append("")
    if not summary["pending_execution"]:
        lines.append("- none")
    else:
        for row in summary["pending_execution"]:
            lines.append(
                f"- queue_id={row.get('queue_id', '')} run={row.get('run_id', '')} orders={row.get('order_count', 0)} path={row.get('execution_orders_path', '')}"
            )
    lines.append("")
    lines.append("## Action Commands")
    lines.append("")
    lines.append("- Review proposal: python3 agent_review.py --decision approve --run-id <RUN_ID> --reviewer <YOU> --note \"...\"")
    lines.append("- Hold proposal: python3 agent_review.py --decision hold --run-id <RUN_ID> --reviewer <YOU> --note \"...\"")
    lines.append("- Execute approved: python3 agent_execute.py --run-id <RUN_ID> --executor <YOU>")
    lines.append("- Refresh all hooks: python3 agent_scheduler.py --once --ops-on-idle")

    output_md = Path(args.output_md)
    output_json = Path(args.output_json)
    output_md.parent.mkdir(parents=True, exist_ok=True)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_md.write_text("\n".join(lines), encoding="utf-8")
    write_json(output_json, summary)

    print(
        f"[INFO] action_center pending_review={len(pending_review)} pending_execution={len(pending_execution)} alerts={safe_int(alerts.get('active_alert_count'), 0)}"
    )
    print(f"[INFO] output_md={output_md}")
    print(f"[INFO] output_json={output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
