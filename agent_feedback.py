#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Proposal quality scoring and feedback synthesis.

This script closes the loop between execution outcomes and future proposal
generation by:
1) scoring recent executed proposals
2) persisting quality history
3) generating model feedback for the next postclose run

Learning policy:
- objective execution artifacts only
- exclude human review decisions, notes, and reviewer preferences
"""

import argparse
import csv
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional


FEEDBACK_LEARNING_POLICY = "objective_execution_only"
FEEDBACK_SOURCES_USED = [
    "execution_history",
    "allocation_proposal",
    "execution_orders",
    "stock_research",
]
FEEDBACK_SOURCES_EXCLUDED = [
    "review_history",
    "human_decision",
    "review_note",
]


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


def clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def in_window_by_ts(value: Any, start_date: date, end_date: date) -> bool:
    dt = parse_iso_datetime(value)
    if dt is None:
        return False
    d = dt.date()
    return start_date <= d <= end_date


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


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return sum(1 for _ in reader)


def load_execution_tickers(path: Path) -> List[str]:
    if not path.exists():
        return []
    tickers: List[str] = []
    with path.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = str(row.get("ticker", "")).strip()
            if ticker:
                tickers.append(ticker)
    return sorted(set(tickers))


def load_risk_flags(path: Path) -> List[str]:
    flags: List[str] = []
    for row in read_jsonl(path):
        for flag in list(row.get("risk_flags", [])):
            text = str(flag).strip()
            if text:
                flags.append(text)
    return sorted(set(flags))


def find_run_dir(run_id: str, runs_root: Path) -> Optional[Path]:
    matches = sorted(runs_root.glob(f"*/{run_id}"))
    if not matches:
        return None
    return matches[-1]


def quality_label(score: float) -> str:
    if score >= 0.8:
        return "high"
    if score >= 0.6:
        return "medium"
    return "low"


def compute_quality_row(
    execution_row: Dict[str, Any],
    proposal: Dict[str, Any],
    action_count: int,
) -> Dict[str, Any]:
    evidence_score = clamp(safe_float(proposal.get("evidence_completeness"), 0.0))
    gate_failures = proposal.get("gate_failures", [])
    gate_failure_count = len(gate_failures) if isinstance(gate_failures, list) else 0
    gate_score = clamp(1.0 - min(0.8, gate_failure_count * 0.25))

    costs = (
        execution_row.get("execution_costs", {})
        if isinstance(execution_row.get("execution_costs"), dict)
        else {}
    )
    cost_ratio = safe_float(costs.get("cost_ratio_total_asset"), 0.0)
    max_cost_ratio_guard = safe_float(execution_row.get("max_cost_ratio_guard"), 0.005)
    if max_cost_ratio_guard <= 0:
        cost_score = 1.0
    else:
        cost_score = clamp(1.0 - (cost_ratio / max_cost_ratio_guard))

    validation = (
        execution_row.get("constraint_validation", {})
        if isinstance(execution_row.get("constraint_validation"), dict)
        else {}
    )
    constraint_compliant = bool(validation.get("compliant", True))
    constraint_score = 1.0 if constraint_compliant else 0.2

    warnings = execution_row.get("warnings", [])
    warning_count = len(warnings) if isinstance(warnings, list) else 0
    warning_score = clamp(1.0 - min(0.7, warning_count * 0.1), 0.2, 1.0)

    force_override = bool(execution_row.get("force_override", False))
    force_score = 0.6 if force_override else 1.0

    action_score = 0.75 if action_count > 0 else 1.0

    quality = (
        0.28 * evidence_score
        + 0.14 * gate_score
        + 0.20 * cost_score
        + 0.20 * constraint_score
        + 0.10 * warning_score
        + 0.08 * action_score
    ) * force_score
    quality = clamp(quality)

    return {
        "quality_score": round(quality, 4),
        "quality_label": quality_label(quality),
        "evidence_score": round(evidence_score, 4),
        "gate_score": round(gate_score, 4),
        "cost_score": round(cost_score, 4),
        "constraint_score": round(constraint_score, 4),
        "warning_score": round(warning_score, 4),
        "action_score": round(action_score, 4),
        "force_score": round(force_score, 4),
        "gate_failure_count": gate_failure_count,
        "warning_count": warning_count,
        "cost_ratio": round(cost_ratio, 8),
        "max_cost_ratio_guard": round(max_cost_ratio_guard, 8),
        "constraint_compliant": constraint_compliant,
        "force_override": force_override,
        "action_count": action_count,
    }


def synthesize_feedback(
    quality_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    rows = sorted(
        quality_rows,
        key=lambda x: str(x.get("timestamp", "")),
        reverse=True,
    )
    sample_size = len(rows)
    avg_quality = mean([safe_float(r.get("quality_score"), 0.0) for r in rows]) if rows else 0.0
    avg_cost_ratio = mean([safe_float(r.get("cost_ratio"), 0.0) for r in rows]) if rows else 0.0

    if avg_quality >= 0.8:
        min_confidence_buy = 0.65
        max_new_positions_override = 2
    elif avg_quality >= 0.7:
        min_confidence_buy = 0.72
        max_new_positions_override = 2
    elif avg_quality >= 0.6:
        min_confidence_buy = 0.78
        max_new_positions_override = 2
    elif avg_quality >= 0.5:
        min_confidence_buy = 0.84
        max_new_positions_override = 1
    else:
        min_confidence_buy = 0.9
        max_new_positions_override = 1

    ticker_penalties: Dict[str, float] = {}
    risk_flag_penalties: Dict[str, float] = {}
    for row in rows:
        score = safe_float(row.get("quality_score"), 0.0)
        if score >= 0.65:
            continue
        severity = 0.12 if score < 0.5 else 0.08
        for ticker in row.get("tickers", []):
            key = str(ticker).strip()
            if not key:
                continue
            ticker_penalties[key] = min(0.25, ticker_penalties.get(key, 0.0) + severity)
        for flag in row.get("risk_flags", []):
            key = str(flag).strip()
            if not key:
                continue
            risk_flag_penalties[key] = min(0.2, risk_flag_penalties.get(key, 0.0) + 0.03)

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "learning_policy": FEEDBACK_LEARNING_POLICY,
        "human_review_signals_included": False,
        "sources_used": list(FEEDBACK_SOURCES_USED),
        "sources_excluded": list(FEEDBACK_SOURCES_EXCLUDED),
        "quality_sample_size": sample_size,
        "average_quality_score": round(avg_quality, 4),
        "average_cost_ratio": round(avg_cost_ratio, 8),
        "min_confidence_buy": round(min_confidence_buy, 4),
        "max_new_positions_override": int(max_new_positions_override),
        "ticker_penalties": ticker_penalties,
        "risk_flag_penalties": risk_flag_penalties,
    }


def build_markdown(summary: Dict[str, Any], top_n: int = 10) -> str:
    lines: List[str] = []
    lines.append("# Proposal Quality Feedback Report")
    lines.append("")
    lines.append(f"- generated_at: {summary['generated_at']}")
    lines.append(f"- window: {summary['window_start']} to {summary['window_end']}")
    lines.append(f"- sample_size: {summary['sample_size']}")
    lines.append(f"- avg_quality_score: {summary['avg_quality_score']:.4f}")
    lines.append(f"- avg_cost_ratio: {summary['avg_cost_ratio']:.4%}")
    lines.append("")
    lines.append("## Learning Policy")
    lines.append("")
    lines.append(f"- learning_policy: {summary['model_feedback']['learning_policy']}")
    lines.append(
        f"- human_review_signals_included: {summary['model_feedback']['human_review_signals_included']}"
    )
    lines.append(
        "- sources_used: " + ", ".join(summary["model_feedback"].get("sources_used", []))
    )
    lines.append(
        "- sources_excluded: " + ", ".join(summary["model_feedback"].get("sources_excluded", []))
    )
    lines.append("")
    lines.append("## Next-Round Feedback")
    lines.append("")
    lines.append(
        f"- min_confidence_buy: {summary['model_feedback']['min_confidence_buy']:.2f}"
    )
    lines.append(
        f"- max_new_positions_override: {summary['model_feedback']['max_new_positions_override']}"
    )
    lines.append(
        f"- ticker_penalties: {len(summary['model_feedback']['ticker_penalties'])}"
    )
    lines.append(
        f"- risk_flag_penalties: {len(summary['model_feedback']['risk_flag_penalties'])}"
    )
    lines.append("")
    lines.append("## Low Quality Cases")
    lines.append("")

    low_rows = [
        r for r in summary.get("quality_rows", []) if safe_float(r.get("quality_score"), 0.0) < 0.6
    ]
    if not low_rows:
        lines.append("- none")
    else:
        for row in low_rows[:top_n]:
            lines.append(
                "- "
                + f"{row['timestamp']} run={row['run_id']} score={safe_float(row['quality_score']):.4f} "
                + f"cost_ratio={safe_float(row['cost_ratio']):.4%} "
                + f"gate_failures={safe_int(row['gate_failure_count'])} "
                + f"warnings={safe_int(row['warning_count'])}"
            )

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate proposal quality feedback")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--runs-root", default="runs")
    parser.add_argument("--state-root", default="state")
    parser.add_argument("--output-md", default="runs/ops/proposal_quality_latest.md")
    parser.add_argument("--output-json", default="runs/ops/proposal_quality_latest.json")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    today = date.today()
    window_start = today - timedelta(days=max(0, args.days - 1))
    window_end = today

    runs_root = Path(args.runs_root)
    state_root = Path(args.state_root)

    execution_history = read_jsonl(state_root / "execution_history.jsonl")
    rows_in_window = [
        r
        for r in execution_history
        if not bool(r.get("dry_run", False))
        and in_window_by_ts(r.get("timestamp", ""), window_start, window_end)
    ]

    quality_rows: List[Dict[str, Any]] = []
    for row in rows_in_window:
        run_id = str(row.get("run_id", ""))
        proposal_id = str(row.get("proposal_id", ""))
        if not run_id or not proposal_id:
            continue

        run_dir = find_run_dir(run_id, runs_root)
        if run_dir is None:
            continue

        proposal = read_json(run_dir / "allocation_proposal.json")
        orders_path = run_dir / "execution_orders.csv"
        action_count = load_csv_row_count(orders_path)
        tickers = load_execution_tickers(orders_path)
        risk_flags = load_risk_flags(run_dir / "stock_research.jsonl")
        quality = compute_quality_row(row, proposal, action_count)
        quality_row = {
            "timestamp": str(row.get("timestamp", "")),
            "trading_date": str(proposal.get("trading_date", "")),
            "run_id": run_id,
            "proposal_id": proposal_id,
            "decision": str(proposal.get("decision", "")),
            "tickers": tickers,
            "risk_flags": risk_flags,
            **quality,
        }
        quality_rows.append(quality_row)

    # Merge with existing history for stable reruns.
    history_path = state_root / "proposal_quality_history.jsonl"
    existing_history = read_jsonl(history_path)
    merged: Dict[str, Dict[str, Any]] = {}
    for row in existing_history:
        key = f"{row.get('run_id', '')}:{row.get('proposal_id', '')}"
        if key.strip(":"):
            merged[key] = row
    for row in quality_rows:
        key = f"{row.get('run_id', '')}:{row.get('proposal_id', '')}"
        if key.strip(":"):
            merged[key] = row
    merged_rows = sorted(merged.values(), key=lambda x: str(x.get("timestamp", "")))

    model_feedback = synthesize_feedback(quality_rows)
    model_feedback["window_days"] = max(1, int(args.days))

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "sample_size": len(quality_rows),
        "avg_quality_score": mean([safe_float(r.get("quality_score"), 0.0) for r in quality_rows])
        if quality_rows
        else 0.0,
        "avg_cost_ratio": mean([safe_float(r.get("cost_ratio"), 0.0) for r in quality_rows])
        if quality_rows
        else 0.0,
        "model_feedback": model_feedback,
        "quality_rows": sorted(
            quality_rows,
            key=lambda x: str(x.get("timestamp", "")),
            reverse=True,
        )[:50],
    }

    if not args.dry_run:
        write_jsonl(history_path, merged_rows)
        write_json(state_root / "model_feedback.json", model_feedback)
        write_json(Path(args.output_json), summary)
        md_text = build_markdown(summary)
        Path(args.output_md).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output_md).write_text(md_text, encoding="utf-8")

    print(f"[INFO] sample_size={summary['sample_size']}")
    print(f"[INFO] avg_quality_score={summary['avg_quality_score']:.4f}")
    print(
        "[INFO] feedback="
        + json.dumps(
            {
                "min_confidence_buy": model_feedback["min_confidence_buy"],
                "max_new_positions_override": model_feedback["max_new_positions_override"],
                "ticker_penalties": len(model_feedback["ticker_penalties"]),
                "risk_flag_penalties": len(model_feedback["risk_flag_penalties"]),
            },
            ensure_ascii=False,
        )
    )
    if not args.dry_run:
        print(f"[INFO] history={history_path}")
        print(f"[INFO] model_feedback={state_root / 'model_feedback.json'}")
        print(f"[INFO] output_md={args.output_md}")
        print(f"[INFO] output_json={args.output_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
