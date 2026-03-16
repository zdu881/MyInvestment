#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Manual review entrypoint for postclose proposals.

This script records human approval decisions and updates run artifacts.
It does not execute trades; it only finalizes review outcomes.
"""

import argparse
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from runtime_paths import resolve_runtime_paths
from state_io import LockTimeoutError, advisory_lock, write_jsonl_atomic


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


def read_jsonl(path: Path) -> List[Dict]:
    if not path.exists():
        return []
    rows: List[Dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def find_run_dir(run_id: str, runs_root: Path) -> Optional[Path]:
    pattern = f"*/{run_id}"
    matches = list(runs_root.glob(pattern))
    if not matches:
        return None
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
    parser.add_argument("--config", default="agent_config.json")
    parser.add_argument("--runs-root", default="")
    parser.add_argument("--state-root", default="")
    parser.add_argument("--timezone-offset-hours", type=int, default=8)
    parser.add_argument("--lock-timeout-sec", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    _, runtime_paths = resolve_runtime_paths(
        Path(args.config),
        overrides={
            "runs_root": args.runs_root,
            "state_root": args.state_root,
        },
    )
    runs_root = runtime_paths.runs_root
    state_root = runtime_paths.state_root
    decision_log_path = runtime_paths.decision_log_path
    queue_lock_path = state_root / "queues.lock"

    run_dir: Optional[Path] = None
    if args.run_dir:
        run_dir = Path(args.run_dir).resolve()
    elif args.run_id:
        run_dir = find_run_dir(args.run_id, runs_root)

    if run_dir is None or not run_dir.exists():
        raise SystemExit("run dir not found, provide --run-dir or valid --run-id")

    proposal_path = run_dir / "allocation_proposal.json"
    if not proposal_path.exists():
        raise SystemExit(f"proposal not found: {proposal_path}")

    timestamp = now_local_iso(args.timezone_offset_hours)
    run_review_path = run_dir / "review_decision.json"
    review_queue_path = state_root / "review_queue.jsonl"
    execution_queue_path = state_root / "execution_queue.jsonl"
    review_history_path = state_root / "review_history.jsonl"

    try:
        with advisory_lock(queue_lock_path, timeout_sec=args.lock_timeout_sec):
            proposal = load_json(proposal_path)
            run_id = str(proposal.get("run_id") or run_dir.name)
            proposal_id = str(proposal.get("proposal_id", f"proposal-{run_id[:8]}"))
            proposal_decision = str(proposal.get("decision", "watch"))

            if args.decision == "approve":
                if proposal_decision == "rebalance":
                    final_action = "approved_rebalance"
                elif proposal_decision == "stay_in_cash":
                    final_action = "approved_stay_in_cash"
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

            queue_rows = read_jsonl(review_queue_path)
            queue_updated = False
            for row in queue_rows:
                if (
                    str(row.get("run_id", "")) == run_id
                    and str(row.get("proposal_id", "")) == proposal_id
                    and str(row.get("status", "")).strip().lower() == "pending"
                ):
                    row["status"] = "reviewed"
                    row["reviewed_at"] = timestamp
                    row["reviewed_by"] = args.reviewer
                    row["human_decision"] = args.decision
                    row["final_action"] = final_action
                    row["review_note"] = args.note
                    queue_updated = True
                    break
            if not queue_updated:
                raise SystemExit("review queue item is not pending")

            proposal["review_status"] = "approved" if args.decision == "approve" else args.decision
            proposal["reviewed_by"] = args.reviewer
            proposal["reviewed_at"] = timestamp
            proposal["human_decision"] = args.decision
            proposal["review_note"] = args.note

            write_json(proposal_path, proposal)
            write_json(run_review_path, review_record)
            write_jsonl_atomic(review_queue_path, queue_rows)

            if final_action == "approved_rebalance":
                actions_path = run_dir / "rebalance_actions.csv"
                preview_path = run_dir / "execution_plan.md"
                execution_orders_path = run_dir / "execution_orders.csv"
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
                        orders = []
                        for _, row in df.iterrows():
                            ticker = str(row.get("ticker", "")).strip().zfill(6)
                            action = str(row.get("action", "HOLD")).strip()
                            current_weight = float(row.get("current_weight", 0))
                            target_weight = float(row.get("target_weight", 0))
                            delta_weight = float(row.get("delta_weight", 0))
                            lines.append(
                                f"- {action} {ticker} {row.get('name', 'N/A')} | "
                                f"{current_weight:.2%} -> {target_weight:.2%}"
                            )
                            orders.append(
                                {
                                    "order_id": str(uuid.uuid4()),
                                    "run_id": run_id,
                                    "proposal_id": proposal_id,
                                    "ticker": ticker,
                                    "name": str(row.get("name", "N/A")),
                                    "action": action,
                                    "current_weight": round(current_weight, 4),
                                    "target_weight": round(target_weight, 4),
                                    "delta_weight": round(delta_weight, 4),
                                    "status": "pending",
                                    "created_at": timestamp,
                                }
                            )
                        if orders:
                            existing_exec_queue = read_jsonl(execution_queue_path)
                            already_queued = any(
                                str(x.get("run_id", "")) == run_id
                                and str(x.get("proposal_id", "")) == proposal_id
                                and str(x.get("status", "")).strip().lower() in {"pending", "executed"}
                                for x in existing_exec_queue
                            )
                            if already_queued:
                                lines.append("- Execution queue already contains this proposal, skip duplicate enqueue.")
                            else:
                                pd.DataFrame(orders).to_csv(execution_orders_path, index=False, encoding="utf-8-sig")
                                queue_item = {
                                    "queue_id": str(uuid.uuid4()),
                                    "run_id": run_id,
                                    "proposal_id": proposal_id,
                                    "status": "pending",
                                    "created_at": timestamp,
                                    "created_by": args.reviewer,
                                    "order_count": len(orders),
                                    "execution_orders_path": str(execution_orders_path),
                                }
                                existing_exec_queue.append(queue_item)
                                write_jsonl_atomic(execution_queue_path, existing_exec_queue)
                                proposal["execution_status"] = "queued"
                                proposal["execution_queue_id"] = queue_item["queue_id"]
                                write_json(proposal_path, proposal)
                                review_record["execution_queue_id"] = queue_item["queue_id"]
                                review_record["execution_orders_path"] = str(execution_orders_path)
                else:
                    lines.append("- rebalance_actions.csv not found.")
                preview_path.write_text("\n".join(lines), encoding="utf-8")

            append_jsonl(run_dir / "decision_log.jsonl", review_record)
            append_jsonl(decision_log_path, review_record)
            append_jsonl(review_history_path, review_record)
    except LockTimeoutError as exc:
        raise SystemExit(f"review queue is busy: {exc}") from exc

    print(f"[INFO] reviewed run_id={run_id}")
    print(f"[INFO] human_decision={args.decision}")
    print(f"[INFO] final_action={final_action}")
    print(f"[INFO] proposal_path={proposal_path}")
    print(f"[INFO] review_file={run_review_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
