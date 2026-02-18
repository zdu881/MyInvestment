#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Execution stage for approved rebalance proposals.

This script consumes pending tasks in state/execution_queue.jsonl,
updates state/current_positions.csv and state/account_snapshot.json,
and records execution audit artifacts.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def now_local_iso(tz_hours: int = 8) -> str:
    now = datetime.now(timezone(timedelta(hours=tz_hours)))
    return now.isoformat(timespec="seconds")


def normalize_ticker(value: Any) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
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


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


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


def find_run_dir(run_id: str, runs_root: Path) -> Optional[Path]:
    matches = sorted(runs_root.glob(f"*/{run_id}"))
    if not matches:
        return None
    return matches[-1]


def pick_queue_item(
    queue_rows: List[Dict[str, Any]], queue_id: str, run_id: str
) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    candidates = []
    for idx, row in enumerate(queue_rows):
        if str(row.get("status", "")) != "pending":
            continue
        if queue_id and str(row.get("queue_id", "")) != queue_id:
            continue
        if run_id and str(row.get("run_id", "")) != run_id:
            continue
        candidates.append((idx, row))

    if not candidates:
        return None, None
    return candidates[-1][1], candidates[-1][0]


def load_price_map(run_dir: Path) -> Dict[str, float]:
    price_map: Dict[str, float] = {}
    for filename in ["candidates_step2.csv", "candidates_step1.csv"]:
        path = run_dir / filename
        if not path.exists():
            continue

        df = pd.read_csv(path, dtype={"股票代码": str, "ticker": str})
        if df.empty:
            continue

        ticker_col = "股票代码" if "股票代码" in df.columns else "ticker"
        price_col = None
        for c in ["现价", "current_price"]:
            if c in df.columns:
                price_col = c
                break

        if ticker_col is None or price_col is None:
            continue

        for _, row in df.iterrows():
            ticker = normalize_ticker(row[ticker_col])
            price = safe_float(row.get(price_col), 0.0)
            if price > 0:
                price_map[ticker] = price

    return price_map


def load_name_map(run_dir: Path) -> Dict[str, str]:
    name_map: Dict[str, str] = {}
    research_path = run_dir / "stock_research.jsonl"
    if research_path.exists():
        for row in read_jsonl(research_path):
            ticker = normalize_ticker(row.get("ticker", ""))
            name = str(row.get("name", "N/A"))
            if ticker:
                name_map[ticker] = name

    for filename in ["candidates_step2.csv", "candidates_step1.csv"]:
        path = run_dir / filename
        if not path.exists():
            continue
        df = pd.read_csv(path, dtype={"股票代码": str, "ticker": str})
        if df.empty:
            continue

        ticker_col = "股票代码" if "股票代码" in df.columns else "ticker"
        name_col = "名称" if "名称" in df.columns else "name"
        if ticker_col not in df.columns or name_col not in df.columns:
            continue

        for _, row in df.iterrows():
            ticker = normalize_ticker(row[ticker_col])
            if ticker and ticker not in name_map:
                name_map[ticker] = str(row.get(name_col, "N/A"))

    return name_map


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute approved rebalance task")
    parser.add_argument("--queue-id", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--executor", default="manual_executor")
    parser.add_argument("--state-root", default="state")
    parser.add_argument("--runs-root", default="runs")
    parser.add_argument("--timezone-offset-hours", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    state_root = Path(args.state_root)
    runs_root = Path(args.runs_root)
    queue_path = state_root / "execution_queue.jsonl"

    queue_rows = read_jsonl(queue_path)
    if not queue_rows:
        raise SystemExit("execution queue is empty")

    queue_item, queue_idx = pick_queue_item(queue_rows, args.queue_id, args.run_id)
    if queue_item is None or queue_idx is None:
        raise SystemExit("no pending queue item matched")

    run_id = str(queue_item.get("run_id", ""))
    proposal_id = str(queue_item.get("proposal_id", ""))
    run_dir = find_run_dir(run_id, runs_root)
    if run_dir is None:
        raise SystemExit(f"run dir not found for run_id={run_id}")

    proposal_path = run_dir / "allocation_proposal.json"
    review_path = run_dir / "review_decision.json"
    orders_path = Path(str(queue_item.get("execution_orders_path", run_dir / "execution_orders.csv")))

    proposal = load_json(proposal_path)
    review = load_json(review_path)

    if not proposal:
        raise SystemExit(f"proposal missing: {proposal_path}")
    if not review:
        raise SystemExit(f"review decision missing: {review_path}")
    if str(review.get("human_decision")) != "approve":
        raise SystemExit("review decision is not approve, refuse execution")

    if str(proposal.get("review_status", "")) != "approved":
        raise SystemExit("proposal review_status is not approved")

    if str(queue_item.get("status", "")) != "pending":
        raise SystemExit("queue item is not pending")

    target_weights = proposal.get("target_weights", {})
    if not isinstance(target_weights, dict):
        raise SystemExit("proposal target_weights malformed")

    account_path = state_root / "account_snapshot.json"
    positions_path = state_root / "current_positions.csv"

    account = load_json(account_path)
    total_asset = safe_float(account.get("total_asset"), 0.0)
    if total_asset <= 0:
        raise SystemExit("account total_asset must be > 0")

    price_map = load_price_map(run_dir)
    name_map = load_name_map(run_dir)

    industry_map: Dict[str, str] = {}
    for item in proposal.get("new_portfolio", []):
        ticker = normalize_ticker(item.get("ticker", ""))
        industry_map[ticker] = str(item.get("industry", "未知"))

    warnings: List[str] = []
    new_rows: List[Dict[str, Any]] = []
    executed_at = now_local_iso(args.timezone_offset_hours)

    for t_raw, w_raw in sorted(target_weights.items()):
        ticker = normalize_ticker(t_raw)
        weight = safe_float(w_raw, 0.0)
        if weight <= 0:
            continue

        price = safe_float(price_map.get(ticker), 0.0)
        if price <= 0:
            price = 1.0
            warnings.append(f"missing price for {ticker}, fallback price=1.0")

        market_value = total_asset * weight
        shares = market_value / price if price > 0 else 0.0

        new_rows.append(
            {
                "ticker": ticker,
                "name": name_map.get(ticker, "N/A"),
                "shares": round(shares, 4),
                "avg_cost": round(price, 4),
                "last_price": round(price, 4),
                "market_value": round(market_value, 4),
                "weight": round(weight, 6),
                "industry": industry_map.get(ticker, "未知"),
                "updated_at": executed_at,
            }
        )

    stock_asset = sum(float(r["market_value"]) for r in new_rows)
    cash = max(0.0, total_asset - stock_asset)
    cash_ratio = cash / total_asset if total_asset > 0 else 1.0

    execution_result = {
        "timestamp": executed_at,
        "run_id": run_id,
        "proposal_id": proposal_id,
        "queue_id": str(queue_item.get("queue_id", "")),
        "executor": args.executor,
        "dry_run": args.dry_run,
        "position_count": len(new_rows),
        "stock_asset": round(stock_asset, 4),
        "cash": round(cash, 4),
        "cash_ratio": round(cash_ratio, 6),
        "warnings": warnings,
    }

    if not args.dry_run:
        # 1) update positions
        pd.DataFrame(new_rows).to_csv(positions_path, index=False, encoding="utf-8-sig")

        # 2) update account snapshot
        account["stock_asset"] = round(stock_asset, 4)
        account["cash"] = round(cash, 4)
        account["cash_ratio"] = round(cash_ratio, 6)
        account["updated_at"] = executed_at
        write_json(account_path, account)

        # 3) update queue status
        queue_rows[queue_idx]["status"] = "executed"
        queue_rows[queue_idx]["executed_at"] = executed_at
        queue_rows[queue_idx]["executor"] = args.executor
        write_jsonl(queue_path, queue_rows)

        # 4) update proposal status
        proposal["execution_status"] = "executed"
        proposal["executed_at"] = executed_at
        proposal["executed_by"] = args.executor
        proposal["execution_warnings"] = warnings
        write_json(proposal_path, proposal)

        # 5) audit artifacts
        result_path = run_dir / "execution_result.json"
        write_json(result_path, execution_result)
        append_jsonl(state_root / "execution_history.jsonl", execution_result)
        append_jsonl(Path("decision_log.jsonl"), {
            "timestamp": executed_at,
            "run_id": run_id,
            "decision_id": proposal_id,
            "executor": args.executor,
            "final_action": "executed_rebalance",
            "note": "execution applied to state",
        })
        append_jsonl(run_dir / "decision_log.jsonl", {
            "timestamp": executed_at,
            "run_id": run_id,
            "decision_id": proposal_id,
            "executor": args.executor,
            "final_action": "executed_rebalance",
            "note": "execution applied to state",
        })
    else:
        execution_result["note"] = "dry-run only, state not changed"

    print(f"[INFO] run_id={run_id}")
    print(f"[INFO] proposal_id={proposal_id}")
    print(f"[INFO] queue_id={queue_item.get('queue_id', '')}")
    print(f"[INFO] dry_run={args.dry_run}")
    print(f"[INFO] position_count={len(new_rows)}")
    print(f"[INFO] cash_ratio={cash_ratio:.2%}")
    if warnings:
        print(f"[WARN] warnings={len(warnings)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
