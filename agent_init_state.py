#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Initialize MyInvestment runtime state for first capital deployment.

Use cases:
1) first-time onboarding before any real trade
2) reset test/demo artifacts and rebuild a clean baseline
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def load_json(path: Path, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not path.exists():
        return {} if default is None else default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def touch_empty(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("", encoding="utf-8")


def truncate(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def normalize_ticker(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(6)[-6:]


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


def now_local_iso(tz_hours: int) -> str:
    now = datetime.now(timezone(timedelta(hours=tz_hours)))
    return now.isoformat(timespec="seconds")


def count_nonempty_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return sum(1 for _ in reader)


def count_pending_jsonl(path: Path) -> int:
    if not path.exists():
        return 0
    pending = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                row = json.loads(text)
            except Exception:
                continue
            if str(row.get("status", "")).strip().lower() == "pending":
                pending += 1
    return pending


def is_trading_date_dir(name: str) -> bool:
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", name))


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            out = {k: row.get(k, "") for k in fieldnames}
            writer.writerow(out)


def load_seed_watchlist(path: Path, default_added_at: str) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    out: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        ticker_raw = (
            row.get("ticker")
            or row.get("code")
            or row.get("symbol")
            or row.get("股票代码")
            or ""
        )
        ticker = normalize_ticker(ticker_raw)
        if not ticker or ticker in seen:
            continue
        seen.add(ticker)

        name = str(row.get("name") or row.get("名称") or "").strip()
        reason = str(row.get("reason") or "seed").strip() or "seed"
        added_at = str(row.get("added_at") or default_added_at).strip() or default_added_at
        priority = str(row.get("priority") or "normal").strip() or "normal"
        status = str(row.get("status") or "active").strip() or "active"
        out.append(
            {
                "ticker": ticker,
                "name": name,
                "reason": reason,
                "added_at": added_at,
                "priority": priority,
                "status": status,
            }
        )
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize onboarding state for first market entry")
    parser.add_argument("--config", default="agent_config.json")
    parser.add_argument("--initial-capital", type=float, default=100000.0)
    parser.add_argument("--risk-profile", default="defensive")
    parser.add_argument("--max-single-weight", type=float, default=None)
    parser.add_argument("--max-industry-weight", type=float, default=None)
    parser.add_argument("--min-cash-ratio", type=float, default=None)
    parser.add_argument("--reset-runtime", action="store_true")
    parser.add_argument("--reset-knowledge", action="store_true")
    parser.add_argument("--reset-watchlist", action="store_true")
    parser.add_argument("--seed-watchlist", default="")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    cfg = load_json(Path(args.config), default={})
    paths = cfg.get("paths", {}) if isinstance(cfg.get("paths", {}), dict) else {}
    constraints_cfg = (
        cfg.get("constraints", {})
        if isinstance(cfg.get("constraints", {}), dict)
        else {}
    )
    tz_hours = int(cfg.get("timezone_offset_hours", 8))

    runs_root = Path(paths.get("runs_root", "runs"))
    state_root = Path(paths.get("state_root", "state"))
    knowledge_root = Path(paths.get("knowledge_root", "knowledge"))

    account_path = state_root / "account_snapshot.json"
    positions_path = state_root / "current_positions.csv"
    watchlist_path = state_root / "watchlist.csv"

    max_single_weight = safe_float(
        args.max_single_weight,
        safe_float(constraints_cfg.get("max_single_weight"), 0.3),
    )
    max_industry_weight = safe_float(
        args.max_industry_weight,
        safe_float(constraints_cfg.get("max_industry_weight"), 0.5),
    )
    min_cash_ratio = safe_float(
        args.min_cash_ratio,
        safe_float(constraints_cfg.get("min_cash_ratio"), 0.1),
    )

    if args.initial_capital <= 0:
        raise SystemExit("--initial-capital must be > 0")
    if not (0 < max_single_weight <= 1):
        raise SystemExit("--max-single-weight must be in (0, 1]")
    if not (0 < max_industry_weight <= 1):
        raise SystemExit("--max-industry-weight must be in (0, 1]")
    if not (0 <= min_cash_ratio < 1):
        raise SystemExit("--min-cash-ratio must be in [0, 1)")
    if max_industry_weight + 1e-12 < max_single_weight:
        raise SystemExit("--max-industry-weight cannot be smaller than --max-single-weight")

    seed_watchlist_path = Path(args.seed_watchlist).resolve() if args.seed_watchlist else None
    if seed_watchlist_path and not seed_watchlist_path.exists():
        raise SystemExit(f"seed watchlist file not found: {seed_watchlist_path}")

    # Preflight checks to avoid accidental overwrite on running accounts.
    existing_position_rows = count_csv_rows(positions_path)
    pending_review = count_pending_jsonl(state_root / "review_queue.jsonl")
    pending_execution = count_pending_jsonl(state_root / "execution_queue.jsonl")
    history_execution_lines = count_nonempty_lines(state_root / "execution_history.jsonl")
    history_decision_lines = count_nonempty_lines(Path("decision_log.jsonl"))

    run_artifact_files = 0
    if runs_root.exists():
        for item in runs_root.iterdir():
            if not item.is_dir() or not is_trading_date_dir(item.name):
                continue
            run_artifact_files += sum(1 for _ in item.rglob("*") if _.is_file())

    existing_runtime_signals = (
        existing_position_rows > 0
        or pending_review > 0
        or pending_execution > 0
        or history_execution_lines > 0
        or history_decision_lines > 0
        or run_artifact_files > 0
    )
    if existing_runtime_signals and not (args.force or args.reset_runtime):
        raise SystemExit(
            "detected existing runtime data; use --reset-runtime to clean test history or --force to continue"
        )

    ts = now_local_iso(tz_hours)
    account_payload = {
        "cash": round(float(args.initial_capital), 4),
        "total_asset": round(float(args.initial_capital), 4),
        "stock_asset": 0.0,
        "cash_ratio": 1.0,
        "max_single_weight": round(max_single_weight, 6),
        "max_industry_weight": round(max_industry_weight, 6),
        "min_cash_ratio": round(min_cash_ratio, 6),
        "risk_profile": str(args.risk_profile).strip() or "defensive",
        "updated_at": ts,
    }

    runtime_truncate_files = [
        Path("decision_log.jsonl"),
        state_root / "review_queue.jsonl",
        state_root / "execution_queue.jsonl",
        state_root / "review_history.jsonl",
        state_root / "alerts_events.jsonl",
        state_root / "proposal_quality_history.jsonl",
    ]
    runtime_remove_files = [
        state_root / "execution_history.jsonl",
        state_root / "webui_audit_log.jsonl",
        state_root / "agent_operation_history.jsonl",
        state_root / "agent_operation_guard.json",
        state_root / "alerts_state.json",
        state_root / "model_feedback.json",
        state_root / "maintenance_last_run.json",
        state_root / "notify_cursor.json",
        runs_root / "scheduler.lock",
    ]
    knowledge_truncate_files = [
        knowledge_root / "skill_candidates.jsonl",
        knowledge_root / "skills_registry_history.jsonl",
    ]
    knowledge_remove_files = [knowledge_root / "skill_promotion_last_run.json"]

    reset_date_dirs: List[Path] = []
    reset_ops_files: List[Path] = []
    if runs_root.exists():
        for item in sorted(runs_root.iterdir()):
            if item.is_dir() and is_trading_date_dir(item.name):
                reset_date_dirs.append(item)
        ops_dir = runs_root / "ops"
        if ops_dir.exists():
            reset_ops_files = [p for p in sorted(ops_dir.iterdir()) if p.is_file()]

    if not args.dry_run:
        runs_root.mkdir(parents=True, exist_ok=True)
        (runs_root / "ops").mkdir(parents=True, exist_ok=True)
        state_root.mkdir(parents=True, exist_ok=True)
        knowledge_root.mkdir(parents=True, exist_ok=True)

        if args.reset_runtime:
            for d in reset_date_dirs:
                shutil.rmtree(d, ignore_errors=True)
            for f in reset_ops_files:
                f.unlink(missing_ok=True)
            for p in runtime_truncate_files:
                truncate(p)
            for p in runtime_remove_files:
                if p.exists():
                    p.unlink()
        else:
            # Ensure core queue/history files exist for a predictable first run.
            for p in runtime_truncate_files:
                touch_empty(p)

        if args.reset_knowledge:
            for p in knowledge_truncate_files:
                truncate(p)
            for p in knowledge_remove_files:
                if p.exists():
                    p.unlink()
        else:
            for p in knowledge_truncate_files:
                touch_empty(p)

        write_json(account_path, account_payload)

        write_csv(
            positions_path,
            fieldnames=[
                "ticker",
                "name",
                "shares",
                "avg_cost",
                "last_price",
                "market_value",
                "weight",
                "industry",
                "updated_at",
            ],
            rows=[],
        )

        watchlist_rows: List[Dict[str, Any]] = []
        if seed_watchlist_path:
            watchlist_rows = load_seed_watchlist(seed_watchlist_path, default_added_at=ts)
            write_csv(
                watchlist_path,
                fieldnames=["ticker", "name", "reason", "added_at", "priority", "status"],
                rows=watchlist_rows,
            )
        elif args.reset_watchlist or not watchlist_path.exists():
            write_csv(
                watchlist_path,
                fieldnames=["ticker", "name", "reason", "added_at", "priority", "status"],
                rows=[],
            )

    print("[INFO] onboarding init summary")
    print(f"[INFO] dry_run={args.dry_run}")
    print(f"[INFO] reset_runtime={args.reset_runtime}")
    print(f"[INFO] reset_knowledge={args.reset_knowledge}")
    print(f"[INFO] runs_root={runs_root}")
    print(f"[INFO] state_root={state_root}")
    print(f"[INFO] knowledge_root={knowledge_root}")
    print(f"[INFO] initial_capital={float(args.initial_capital):.2f}")
    print(f"[INFO] risk_profile={account_payload['risk_profile']}")
    print(
        "[INFO] constraints="
        f"max_single={account_payload['max_single_weight']:.2%}, "
        f"max_industry={account_payload['max_industry_weight']:.2%}, "
        f"min_cash={account_payload['min_cash_ratio']:.2%}"
    )
    print(
        "[INFO] preflight="
        f"position_rows={existing_position_rows}, "
        f"pending_review={pending_review}, "
        f"pending_execution={pending_execution}, "
        f"execution_history_lines={history_execution_lines}, "
        f"decision_log_lines={history_decision_lines}, "
        f"run_artifact_files={run_artifact_files}"
    )
    print(f"[INFO] account_snapshot={account_path}")
    print(f"[INFO] current_positions={positions_path}")
    print(f"[INFO] watchlist={watchlist_path}")
    if seed_watchlist_path:
        count = len(load_seed_watchlist(seed_watchlist_path, default_added_at=ts))
        print(f"[INFO] seeded_watchlist_rows={count}")

    if args.dry_run:
        if args.reset_runtime:
            print(
                "[INFO] dry-run plan: "
                f"remove_date_dirs={len(reset_date_dirs)}, "
                f"remove_ops_files={len(reset_ops_files)}, "
                f"truncate_files={len(runtime_truncate_files)}"
            )
        if args.reset_knowledge:
            print(
                "[INFO] dry-run plan: "
                f"truncate_knowledge_files={len(knowledge_truncate_files)}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
