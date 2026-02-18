#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Manual review entrypoint for postclose proposals.

This script records human approval decisions and updates run artifacts.
It does not execute trades; it only finalizes review outcomes.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


def load_json(path: Path) -> Dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def append_jsonl(path: Path, row: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def find_run_dir(run_id: str, runs_root: Path) -> Optional[Path]:
    pattern = f"*/{run_id}"
    matches = list(runs_root.glob(pattern))
    if not matches:
        return None
    # There should be one run_id, but keep deterministic behavior.
    matches = sorted(matches)
    return matches[-1]


def now_local_iso(tz_hours: int = 8) -> str:
    now = datetime.now(timezone(timedelta(hours=tz_hours)))
    return now.isoformat(timespec="seconds")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual review for agent proposals")
    parser.add_argument("--decision", required=True, choices=["approve", "hold", "reject"])
    parser.add_argument("--reviewer", default="manual_user")
    parser.add_argument("--note", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--run-dir", default="")
    parser.add_argument("--runs-root", default="runs")
    parser.add_argument("--timezone-offset-hours", type=int, default=8)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    run_dir: Optional[Path] = None
    if args.run_dir:
        run_dir = Path(args.run_dir)
    elif args.run_id:
        run_dir = find_run_dir(args.run_id, Path(args.runs_root))

    if run_dir is None or not run_dir.exists():
        raise SystemExit("run dir not found, provide --run-dir or valid --run-id")

    proposal_path = run_dir / "allocation_proposal.json"
    if not proposal_path.exists():
        raise SystemExit(f"proposal not found: {proposal_path}")

    proposal = load_json(proposal_path)
    run_id = str(proposal.get("run_id") or run_dir.name)
    proposal_id = str(proposal.get("proposal_id", f"proposal-{run_id[:8]}"))
    proposal_decision = str(proposal.get("decision", "watch"))

    timestamp = now_local_iso(args.timezone_offset_hours)

    if args.decision == "approve":
        if proposal_decision == "rebalance":
            final_action = "approved_rebalance"
        elif proposal_decision == "hold":
            final_action = "approved_hold"
        else:
            final_action = "approved_watch"
    elif args.decision == "hold":
        final_action = "hold"
    else:
        final_action = "reject"

    review_record = {
        "timestamp": timestamp,
        "run_id": run_id,
        "decision_id": proposal_id,
        "reviewer": args.reviewer,
        "human_decision": args.decision,
        "final_action": final_action,
        "proposal_decision": proposal_decision,
        "note": args.note,
    }

    # Update proposal with review state.
    proposal["review_status"] = "approved" if args.decision == "approve" else args.decision
    proposal["reviewed_by"] = args.reviewer
    proposal["reviewed_at"] = timestamp
    proposal["human_decision"] = args.decision
    proposal["review_note"] = args.note
    write_json(proposal_path, proposal)

    run_review_path = run_dir / "review_decision.json"
    write_json(run_review_path, review_record)

    # Append to run-level and global decision logs.
    append_jsonl(run_dir / "decision_log.jsonl", review_record)
    append_jsonl(Path("decision_log.jsonl"), review_record)

    # Persist review history in state area.
    append_jsonl(Path("state") / "review_history.jsonl", review_record)

    # Optional execution preview for approved rebalance only.
    if final_action == "approved_rebalance":
        actions_path = run_dir / "rebalance_actions.csv"
        preview_path = run_dir / "execution_plan.md"
        lines = [
            "# Execution Plan Preview",
            "",
            f"- run_id: {run_id}",
            f"- proposal_id: {proposal_id}",
            f"- approved_by: {args.reviewer}",
            f"- approved_at: {timestamp}",
            "",
            "## Actions",
            "",
        ]
        if actions_path.exists():
            df = pd.read_csv(actions_path)
            if not df.empty:
                df = df[df["action"] != "HOLD"].copy()
            if df.empty:
                lines.append("- No actionable trades after threshold filters.")
            else:
                for _, row in df.iterrows():
                    lines.append(
                        f"- {row['action']} {row['ticker']} {row.get('name', 'N/A')} | "
                        f"{float(row.get('current_weight', 0)):.2%} -> {float(row.get('target_weight', 0)):.2%}"
                    )
        else:
            lines.append("- rebalance_actions.csv not found.")
        preview_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"[INFO] reviewed run_id={run_id}")
    print(f"[INFO] human_decision={args.decision}")
    print(f"[INFO] final_action={final_action}")
    print(f"[INFO] proposal_path={proposal_path}")
    print(f"[INFO] review_file={run_review_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
