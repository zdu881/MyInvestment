#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Operational alert channel for the investment agent system.

The script evaluates the latest ops/quality artifacts and emits:
- current active alerts
- transition events (opened/escalated/deescalated/resolved/reminder)
- markdown/json summaries
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


LEVEL_ORDER = {"ok": 0, "warn": 1, "critical": 2}


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


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except Exception:
        return default


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def age_hours_from_iso(value: Any, now_utc: datetime) -> Optional[float]:
    dt = parse_iso_datetime(value)
    if dt is None:
        return None
    delta = now_utc - dt.astimezone(timezone.utc)
    return delta.total_seconds() / 3600.0


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def evaluate_level(
    value: float,
    warn_threshold: float,
    critical_threshold: float,
    lower_is_worse: bool = True,
) -> str:
    if lower_is_worse:
        if value <= critical_threshold:
            return "critical"
        if value <= warn_threshold:
            return "warn"
        return "ok"
    if value >= critical_threshold:
        return "critical"
    if value >= warn_threshold:
        return "warn"
    return "ok"


def build_markdown(summary: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Agent Alert Summary")
    lines.append("")
    lines.append(f"- generated_at: {summary['generated_at']}")
    lines.append(f"- status: {summary['status']}")
    lines.append(
        f"- active_alerts: {summary['active_alert_count']} (critical={summary['active_critical_count']}, warn={summary['active_warn_count']})"
    )
    lines.append(f"- transition_events: {summary['event_count']}")
    lines.append("")
    lines.append("## Active Alerts")
    lines.append("")

    active = summary.get("active_alerts", [])
    if not active:
        lines.append("- none")
    else:
        for row in active:
            lines.append(
                f"- [{row['level']}] {row['check_id']} | value={row.get('value')} | {row.get('message', '')}"
            )

    lines.append("")
    lines.append("## Recent Events")
    lines.append("")
    events = summary.get("events", [])
    if not events:
        lines.append("- none")
    else:
        for row in events[:20]:
            lines.append(
                f"- {row['timestamp']} event={row['event']} level={row.get('level', '')} check={row.get('check_id', '')} message={row.get('message', '')}"
            )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate operational alerts")
    parser.add_argument("--ops-report-json", default="runs/ops/ops_report_latest.json")
    parser.add_argument("--quality-report-json", default="runs/ops/proposal_quality_latest.json")
    parser.add_argument("--model-feedback-json", default="state/model_feedback.json")
    parser.add_argument("--state-root", default="state")
    parser.add_argument("--output-md", default="runs/ops/alerts_latest.md")
    parser.add_argument("--output-json", default="runs/ops/alerts_latest.json")
    parser.add_argument("--health-score-warn", type=float, default=80.0)
    parser.add_argument("--health-score-critical", type=float, default=70.0)
    parser.add_argument("--stale-review-warn", type=int, default=1)
    parser.add_argument("--stale-execution-warn", type=int, default=1)
    parser.add_argument("--oldest-review-hours-warn", type=float, default=24.0)
    parser.add_argument("--oldest-review-hours-critical", type=float, default=48.0)
    parser.add_argument("--oldest-execution-hours-warn", type=float, default=24.0)
    parser.add_argument("--oldest-execution-hours-critical", type=float, default=48.0)
    parser.add_argument("--quality-score-warn", type=float, default=0.6)
    parser.add_argument("--quality-score-critical", type=float, default=0.5)
    parser.add_argument("--quality-min-sample-size", type=int, default=3)
    parser.add_argument("--feedback-min-confidence-warn", type=float, default=0.78)
    parser.add_argument("--report-stale-hours-warn", type=float, default=8.0)
    parser.add_argument("--report-stale-hours-critical", type=float, default=24.0)
    parser.add_argument("--reminder-hours", type=float, default=24.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now_utc = datetime.now(timezone.utc)
    now_local = datetime.now().astimezone()
    now_ts = now_local.isoformat(timespec="seconds")

    ops_report = read_json(Path(args.ops_report_json))
    quality_report = read_json(Path(args.quality_report_json))
    model_feedback = read_json(Path(args.model_feedback_json))

    checks: List[Dict[str, Any]] = []

    def add_check(
        check_id: str,
        level: str,
        value: Any,
        message: str,
        source: str,
    ) -> None:
        checks.append(
            {
                "check_id": check_id,
                "level": level,
                "value": value,
                "message": message,
                "source": source,
            }
        )

    # Source freshness checks.
    ops_age = age_hours_from_iso(ops_report.get("generated_at"), now_utc) if ops_report else None
    if ops_age is None:
        add_check(
            "ops_report_missing",
            "critical",
            None,
            "ops report missing or unreadable",
            str(args.ops_report_json),
        )
    else:
        level = evaluate_level(
            ops_age,
            warn_threshold=args.report_stale_hours_warn,
            critical_threshold=args.report_stale_hours_critical,
            lower_is_worse=False,
        )
        if level != "ok":
            add_check(
                "ops_report_stale",
                level,
                round(ops_age, 2),
                f"ops report is stale ({ops_age:.2f}h old)",
                str(args.ops_report_json),
            )

    quality_age = age_hours_from_iso(quality_report.get("generated_at"), now_utc) if quality_report else None
    if quality_age is None:
        add_check(
            "quality_report_missing",
            "warn",
            None,
            "proposal quality report missing or unreadable",
            str(args.quality_report_json),
        )
    else:
        level = evaluate_level(
            quality_age,
            warn_threshold=args.report_stale_hours_warn,
            critical_threshold=args.report_stale_hours_critical,
            lower_is_worse=False,
        )
        if level != "ok":
            add_check(
                "quality_report_stale",
                level,
                round(quality_age, 2),
                f"proposal quality report is stale ({quality_age:.2f}h old)",
                str(args.quality_report_json),
            )

    # Ops health checks.
    if ops_report:
        health_score = safe_float(ops_report.get("health_score"), 100.0)
        level = evaluate_level(
            health_score,
            warn_threshold=args.health_score_warn,
            critical_threshold=args.health_score_critical,
            lower_is_worse=True,
        )
        if level != "ok":
            add_check(
                "health_score_low",
                level,
                round(health_score, 2),
                f"health_score={health_score:.2f} below threshold",
                str(args.ops_report_json),
            )

        queue_stats = ops_report.get("queue_stats", {}) if isinstance(ops_report.get("queue_stats"), dict) else {}
        stale_review = safe_int(queue_stats.get("stale_review"), 0)
        stale_execution = safe_int(queue_stats.get("stale_execution"), 0)
        oldest_review = safe_float(queue_stats.get("oldest_pending_review_hours"), 0.0)
        oldest_execution = safe_float(queue_stats.get("oldest_pending_execution_hours"), 0.0)

        if stale_review >= int(args.stale_review_warn):
            level = "critical" if stale_review >= max(2, int(args.stale_review_warn) * 2) else "warn"
            add_check(
                "stale_review_backlog",
                level,
                stale_review,
                f"stale review backlog count={stale_review}",
                str(args.ops_report_json),
            )
        if stale_execution >= int(args.stale_execution_warn):
            level = "critical" if stale_execution >= max(2, int(args.stale_execution_warn) * 2) else "warn"
            add_check(
                "stale_execution_backlog",
                level,
                stale_execution,
                f"stale execution backlog count={stale_execution}",
                str(args.ops_report_json),
            )

        level_review_age = evaluate_level(
            oldest_review,
            warn_threshold=args.oldest_review_hours_warn,
            critical_threshold=args.oldest_review_hours_critical,
            lower_is_worse=False,
        )
        if level_review_age != "ok":
            add_check(
                "oldest_review_wait_too_long",
                level_review_age,
                round(oldest_review, 2),
                f"oldest pending review wait={oldest_review:.2f}h",
                str(args.ops_report_json),
            )

        level_execution_age = evaluate_level(
            oldest_execution,
            warn_threshold=args.oldest_execution_hours_warn,
            critical_threshold=args.oldest_execution_hours_critical,
            lower_is_worse=False,
        )
        if level_execution_age != "ok":
            add_check(
                "oldest_execution_wait_too_long",
                level_execution_age,
                round(oldest_execution, 2),
                f"oldest pending execution wait={oldest_execution:.2f}h",
                str(args.ops_report_json),
            )

    # Quality checks.
    if quality_report:
        sample_size = safe_int(quality_report.get("sample_size"), 0)
        avg_quality = safe_float(quality_report.get("avg_quality_score"), 0.0)
        if sample_size >= int(args.quality_min_sample_size):
            level = evaluate_level(
                avg_quality,
                warn_threshold=args.quality_score_warn,
                critical_threshold=args.quality_score_critical,
                lower_is_worse=True,
            )
            if level != "ok":
                add_check(
                    "proposal_quality_low",
                    level,
                    round(avg_quality, 4),
                    f"avg quality score={avg_quality:.4f} with sample_size={sample_size}",
                    str(args.quality_report_json),
                )
        else:
            add_check(
                "proposal_quality_sample_small",
                "warn",
                sample_size,
                f"quality sample too small ({sample_size}<{args.quality_min_sample_size})",
                str(args.quality_report_json),
            )

    # Feedback posture checks.
    if model_feedback:
        min_conf = safe_float(model_feedback.get("min_confidence_buy"), 0.0)
        if min_conf >= float(args.feedback_min_confidence_warn):
            add_check(
                "feedback_over_conservative",
                "warn",
                round(min_conf, 4),
                f"min_confidence_buy={min_conf:.2f}, model is in conservative mode",
                str(args.model_feedback_json),
            )

    active_now = [c for c in checks if c["level"] in {"warn", "critical"}]
    active_now = sorted(active_now, key=lambda x: (-LEVEL_ORDER.get(x["level"], 0), x["check_id"]))

    state_root = Path(args.state_root)
    state_path = state_root / "alerts_state.json"
    events_path = state_root / "alerts_events.jsonl"
    prev_state = read_json(state_path)
    prev_active = prev_state.get("active", {}) if isinstance(prev_state.get("active"), dict) else {}
    reminder_hours = max(1.0, float(args.reminder_hours))

    events: List[Dict[str, Any]] = []
    next_active: Dict[str, Dict[str, Any]] = {}

    for alert in active_now:
        check_id = str(alert["check_id"])
        prev = prev_active.get(check_id, {})
        prev_level = str(prev.get("level", "ok"))
        current_level = str(alert["level"])

        current_entry = dict(alert)
        current_entry["first_seen"] = str(prev.get("first_seen", now_ts))
        current_entry["last_seen"] = now_ts
        current_entry["last_event_at"] = str(prev.get("last_event_at", ""))

        event_type = ""
        if not prev:
            event_type = "opened"
        else:
            prev_order = LEVEL_ORDER.get(prev_level, 0)
            cur_order = LEVEL_ORDER.get(current_level, 0)
            if cur_order > prev_order:
                event_type = "escalated"
            elif cur_order < prev_order:
                event_type = "deescalated"
            else:
                last_event_dt = parse_iso_datetime(prev.get("last_event_at"))
                if last_event_dt is None:
                    event_type = "reminder"
                else:
                    elapsed = now_utc - last_event_dt.astimezone(timezone.utc)
                    if elapsed.total_seconds() / 3600.0 >= reminder_hours:
                        event_type = "reminder"

        if event_type:
            event = {
                "timestamp": now_ts,
                "event": event_type,
                "check_id": check_id,
                "level": current_level,
                "value": alert.get("value"),
                "message": alert.get("message", ""),
                "source": alert.get("source", ""),
            }
            if prev_level and prev_level != "ok":
                event["prev_level"] = prev_level
            events.append(event)
            current_entry["last_event_at"] = now_ts

        next_active[check_id] = current_entry

    for check_id, prev in prev_active.items():
        if check_id in next_active:
            continue
        events.append(
            {
                "timestamp": now_ts,
                "event": "resolved",
                "check_id": check_id,
                "level": str(prev.get("level", "ok")),
                "value": prev.get("value"),
                "message": str(prev.get("message", "")),
                "source": str(prev.get("source", "")),
            }
        )

    critical_count = sum(1 for x in active_now if x["level"] == "critical")
    warn_count = sum(1 for x in active_now if x["level"] == "warn")
    status = "critical" if critical_count > 0 else ("warn" if warn_count > 0 else "ok")

    summary = {
        "generated_at": now_ts,
        "status": status,
        "active_alert_count": len(active_now),
        "active_critical_count": critical_count,
        "active_warn_count": warn_count,
        "active_alerts": active_now,
        "event_count": len(events),
        "events": events[:100],
        "sources": {
            "ops_report_json": args.ops_report_json,
            "quality_report_json": args.quality_report_json,
            "model_feedback_json": args.model_feedback_json,
        },
    }

    if not args.dry_run:
        state_payload = {
            "updated_at": now_ts,
            "status": status,
            "active": next_active,
        }
        write_json(state_path, state_payload)
        append_jsonl(events_path, events)
        write_json(Path(args.output_json), summary)
        md_text = build_markdown(summary)
        Path(args.output_md).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_md).write_text(md_text, encoding="utf-8")

    print(f"[INFO] status={status}")
    print(
        f"[INFO] active_alerts={summary['active_alert_count']} (critical={critical_count}, warn={warn_count})"
    )
    print(f"[INFO] event_count={summary['event_count']}")
    if not args.dry_run:
        print(f"[INFO] state={state_path}")
        print(f"[INFO] events={events_path}")
        print(f"[INFO] output_md={args.output_md}")
        print(f"[INFO] output_json={args.output_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
