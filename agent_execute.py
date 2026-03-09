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

from runtime_paths import RuntimePaths, resolve_runtime_paths
from state_io import LockTimeoutError, advisory_lock, write_jsonl_atomic

DEFAULT_EXECUTION_SETTINGS = {
    "slippage_bps": 5.0,
    "commission_rate": 0.0003,
    "stamp_duty_sell_rate": 0.001,
}


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


def safe_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
    return default


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_runtime_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    return load_json(path)


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


def load_positions_df(path: Path) -> pd.DataFrame:
    cols = [
        "ticker",
        "name",
        "shares",
        "avg_cost",
        "last_price",
        "market_value",
        "weight",
        "industry",
        "updated_at",
    ]
    if not path.exists():
        return pd.DataFrame(columns=cols)

    df = pd.read_csv(path, dtype={"ticker": str})
    if df.empty:
        return pd.DataFrame(columns=cols)

    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = "" if c in {"ticker", "name", "industry", "updated_at"} else 0.0

    df["ticker"] = df["ticker"].apply(normalize_ticker)
    for c in ["shares", "avg_cost", "last_price", "market_value", "weight"]:
        df[c] = df[c].apply(safe_float)
    df["name"] = df["name"].astype(str)
    df["industry"] = df["industry"].fillna("未知").astype(str)
    df["updated_at"] = df["updated_at"].astype(str)

    return df[cols].copy()


def rows_to_positions_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(
            columns=[
                "ticker",
                "name",
                "shares",
                "avg_cost",
                "last_price",
                "market_value",
                "weight",
                "industry",
                "updated_at",
            ]
        )
    return load_positions_df_from_df(pd.DataFrame(rows))


def load_positions_df_from_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "ticker",
        "name",
        "shares",
        "avg_cost",
        "last_price",
        "market_value",
        "weight",
        "industry",
        "updated_at",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)

    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = "" if c in {"ticker", "name", "industry", "updated_at"} else 0.0

    out["ticker"] = out["ticker"].apply(normalize_ticker)
    for c in ["shares", "avg_cost", "last_price", "market_value", "weight"]:
        out[c] = out[c].apply(safe_float)
    out["name"] = out["name"].astype(str)
    out["industry"] = out["industry"].fillna("未知").astype(str)
    out["updated_at"] = out["updated_at"].astype(str)
    return out[cols].copy()


def industry_exposure(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {}
    grouped = df.groupby("industry", dropna=False)["weight"].sum()
    return {str(k): float(v) for k, v in grouped.to_dict().items()}


def concentration_metrics(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {"top1_weight": 0.0, "top3_weight": 0.0, "hhi": 0.0}
    weights = sorted([safe_float(v, 0.0) for v in df["weight"].tolist()], reverse=True)
    top1 = weights[0] if weights else 0.0
    top3 = sum(weights[:3]) if weights else 0.0
    hhi = sum(w * w for w in weights)
    return {"top1_weight": top1, "top3_weight": top3, "hhi": hhi}


def format_pct(value: float) -> str:
    return f"{value:.2%}"


def load_constraints(account: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, float]:
    cfg_constraints = cfg.get("constraints", {}) if isinstance(cfg, dict) else {}
    return {
        "max_single_weight": safe_float(
            account.get("max_single_weight"), safe_float(cfg_constraints.get("max_single_weight"), 0.3)
        ),
        "max_industry_weight": safe_float(
            account.get("max_industry_weight"), safe_float(cfg_constraints.get("max_industry_weight"), 0.5)
        ),
        "min_cash_ratio": safe_float(
            account.get("min_cash_ratio"), safe_float(cfg_constraints.get("min_cash_ratio"), 0.1)
        ),
    }


def validate_post_execution_constraints(
    positions_df: pd.DataFrame,
    cash_ratio: float,
    constraints: Dict[str, float],
    tolerance: float = 0.001,
) -> Dict[str, Any]:
    max_single_limit = safe_float(constraints.get("max_single_weight"), 0.3)
    max_industry_limit = safe_float(constraints.get("max_industry_weight"), 0.5)
    min_cash_limit = safe_float(constraints.get("min_cash_ratio"), 0.1)

    top1_weight = 0.0
    max_industry_weight = 0.0
    if not positions_df.empty:
        top1_weight = safe_float(positions_df["weight"].max(), 0.0)
        ind_exp = industry_exposure(positions_df)
        max_industry_weight = max(ind_exp.values()) if ind_exp else 0.0

    violations: List[str] = []
    if top1_weight > max_single_limit + tolerance:
        violations.append(
            f"single_weight_exceeded:{top1_weight:.4f}>{max_single_limit:.4f}"
        )
    if max_industry_weight > max_industry_limit + tolerance:
        violations.append(
            f"industry_weight_exceeded:{max_industry_weight:.4f}>{max_industry_limit:.4f}"
        )
    if cash_ratio < min_cash_limit - tolerance:
        violations.append(f"cash_ratio_below_min:{cash_ratio:.4f}<{min_cash_limit:.4f}")

    return {
        "max_single_weight_limit": max_single_limit,
        "max_industry_weight_limit": max_industry_limit,
        "min_cash_ratio_limit": min_cash_limit,
        "tolerance": tolerance,
        "post_top1_weight": round(top1_weight, 8),
        "post_max_industry_weight": round(max_industry_weight, 8),
        "post_cash_ratio": round(cash_ratio, 8),
        "violations": violations,
        "compliant": len(violations) == 0,
    }


def generate_portfolio_change_report(
    run_dir: Path,
    run_id: str,
    proposal_id: str,
    executor: str,
    executed_at: str,
    dry_run: bool,
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    before_cash_ratio: float,
    after_cash_ratio: float,
    warnings: List[str],
    execution_costs: Dict[str, Any],
    constraint_validation: Dict[str, Any],
) -> Path:
    before_map = {
        normalize_ticker(r["ticker"]): safe_float(r.get("weight"), 0.0)
        for _, r in before_df.iterrows()
    }
    after_map = {
        normalize_ticker(r["ticker"]): safe_float(r.get("weight"), 0.0)
        for _, r in after_df.iterrows()
    }
    name_map: Dict[str, str] = {}
    for _, r in before_df.iterrows():
        name_map[normalize_ticker(r["ticker"])] = str(r.get("name", "N/A"))
    for _, r in after_df.iterrows():
        t = normalize_ticker(r["ticker"])
        if t not in name_map:
            name_map[t] = str(r.get("name", "N/A"))

    added: List[str] = []
    removed: List[str] = []
    increased: List[str] = []
    decreased: List[str] = []
    threshold = 1e-9

    all_tickers = sorted(set(before_map.keys()) | set(after_map.keys()))
    for ticker in all_tickers:
        b = before_map.get(ticker, 0.0)
        a = after_map.get(ticker, 0.0)
        delta = a - b
        if b <= threshold and a > threshold:
            added.append(f"{ticker} {name_map.get(ticker, 'N/A')} ({format_pct(a)})")
        elif b > threshold and a <= threshold:
            removed.append(f"{ticker} {name_map.get(ticker, 'N/A')} ({format_pct(b)})")
        elif delta > threshold:
            increased.append(
                f"{ticker} {name_map.get(ticker, 'N/A')} ({format_pct(b)} -> {format_pct(a)})"
            )
        elif delta < -threshold:
            decreased.append(
                f"{ticker} {name_map.get(ticker, 'N/A')} ({format_pct(b)} -> {format_pct(a)})"
            )

    before_ind = industry_exposure(before_df)
    after_ind = industry_exposure(after_df)
    industries = sorted(set(before_ind.keys()) | set(after_ind.keys()))

    before_conc = concentration_metrics(before_df)
    after_conc = concentration_metrics(after_df)

    lines: List[str] = []
    lines.append("# Portfolio Change Report")
    lines.append("")
    lines.append(f"- run_id: {run_id}")
    lines.append(f"- proposal_id: {proposal_id}")
    lines.append(f"- executed_at: {executed_at}")
    lines.append(f"- executor: {executor}")
    lines.append(f"- dry_run: {dry_run}")
    lines.append("")

    lines.append("## Summary")
    lines.append("")
    lines.append(
        f"- holdings_count: {len(before_df)} -> {len(after_df)}"
    )
    lines.append(
        f"- cash_ratio: {format_pct(before_cash_ratio)} -> {format_pct(after_cash_ratio)}"
    )
    lines.append(
        f"- concentration(top1): {format_pct(before_conc['top1_weight'])} -> {format_pct(after_conc['top1_weight'])}"
    )
    lines.append(
        f"- concentration(top3): {format_pct(before_conc['top3_weight'])} -> {format_pct(after_conc['top3_weight'])}"
    )
    lines.append(
        f"- HHI: {before_conc['hhi']:.4f} -> {after_conc['hhi']:.4f}"
    )
    lines.append("")

    lines.append("## Position Changes")
    lines.append("")
    if added:
        lines.append("- Added:")
        for x in added:
            lines.append(f"  - {x}")
    if removed:
        lines.append("- Removed:")
        for x in removed:
            lines.append(f"  - {x}")
    if increased:
        lines.append("- Increased:")
        for x in increased:
            lines.append(f"  - {x}")
    if decreased:
        lines.append("- Decreased:")
        for x in decreased:
            lines.append(f"  - {x}")
    if not any([added, removed, increased, decreased]):
        lines.append("- No material position changes.")
    lines.append("")

    lines.append("## Industry Exposure Delta")
    lines.append("")
    if industries:
        for ind in industries:
            b = before_ind.get(ind, 0.0)
            a = after_ind.get(ind, 0.0)
            lines.append(
                f"- {ind}: {format_pct(b)} -> {format_pct(a)} (delta={format_pct(a - b)})"
            )
    else:
        lines.append("- No industry exposure data.")
    lines.append("")

    lines.append("## Risk Notes")
    lines.append("")
    if warnings:
        for w in warnings:
            lines.append(f"- {w}")
    else:
        lines.append("- No execution warnings.")
    lines.append(
        f"- Max industry exposure after execution: {format_pct(max(after_ind.values()) if after_ind else 0.0)}"
    )
    lines.append(
        f"- Top holding weight after execution: {format_pct(after_conc['top1_weight'])}"
    )
    lines.append("")

    lines.append("## Execution Cost Estimate")
    lines.append("")
    lines.append(
        f"- traded_value: {safe_float(execution_costs.get('traded_value'), 0.0):.2f}"
    )
    lines.append(
        f"- buy_value: {safe_float(execution_costs.get('buy_value'), 0.0):.2f}, sell_value: {safe_float(execution_costs.get('sell_value'), 0.0):.2f}"
    )
    lines.append(
        f"- slippage_cost: {safe_float(execution_costs.get('slippage_cost'), 0.0):.2f}"
    )
    lines.append(
        f"- commission_cost: {safe_float(execution_costs.get('commission_cost'), 0.0):.2f}"
    )
    lines.append(
        f"- stamp_duty_cost: {safe_float(execution_costs.get('stamp_duty_cost'), 0.0):.2f}"
    )
    lines.append(
        f"- total_execution_cost: {safe_float(execution_costs.get('total_execution_cost'), 0.0):.2f} ({format_pct(safe_float(execution_costs.get('cost_ratio_total_asset'), 0.0))})"
    )
    lines.append("")

    lines.append("## Constraint Validation")
    lines.append("")
    lines.append(
        f"- max_single_weight: {format_pct(safe_float(constraint_validation.get('post_top1_weight'), 0.0))} / limit {format_pct(safe_float(constraint_validation.get('max_single_weight_limit'), 0.0))}"
    )
    lines.append(
        f"- max_industry_weight: {format_pct(safe_float(constraint_validation.get('post_max_industry_weight'), 0.0))} / limit {format_pct(safe_float(constraint_validation.get('max_industry_weight_limit'), 0.0))}"
    )
    lines.append(
        f"- cash_ratio: {format_pct(safe_float(constraint_validation.get('post_cash_ratio'), 0.0))} / limit {format_pct(safe_float(constraint_validation.get('min_cash_ratio_limit'), 0.0))}"
    )
    lines.append(
        f"- compliant: {bool(constraint_validation.get('compliant', False))}"
    )
    violations = list(constraint_validation.get("violations", []))
    if violations:
        for v in violations:
            lines.append(f"- violation: {v}")

    report_path = run_dir / "portfolio_change_report.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def estimate_execution_costs(
    total_asset: float,
    orders_path: Path,
    before_df: pd.DataFrame,
    after_df: pd.DataFrame,
    settings: Dict[str, float],
) -> Dict[str, Any]:
    before_map = {
        normalize_ticker(r["ticker"]): safe_float(r.get("weight"), 0.0)
        for _, r in before_df.iterrows()
    }
    after_map = {
        normalize_ticker(r["ticker"]): safe_float(r.get("weight"), 0.0)
        for _, r in after_df.iterrows()
    }

    deltas: Dict[str, float] = {}
    all_tickers = sorted(set(before_map.keys()) | set(after_map.keys()))
    for t in all_tickers:
        deltas[t] = after_map.get(t, 0.0) - before_map.get(t, 0.0)

    if orders_path.exists():
        df = pd.read_csv(orders_path, dtype={"ticker": str})
        if not df.empty:
            deltas = {}
            for _, row in df.iterrows():
                t = normalize_ticker(row.get("ticker", ""))
                deltas[t] = safe_float(row.get("delta_weight"), 0.0)

    buy_value = 0.0
    sell_value = 0.0
    for _, delta_w in deltas.items():
        value = abs(delta_w) * total_asset
        if delta_w > 0:
            buy_value += value
        elif delta_w < 0:
            sell_value += value

    traded_value = buy_value + sell_value
    slippage_bps = safe_float(settings.get("slippage_bps"), DEFAULT_EXECUTION_SETTINGS["slippage_bps"])
    commission_rate = safe_float(settings.get("commission_rate"), DEFAULT_EXECUTION_SETTINGS["commission_rate"])
    stamp_duty_sell_rate = safe_float(
        settings.get("stamp_duty_sell_rate"), DEFAULT_EXECUTION_SETTINGS["stamp_duty_sell_rate"]
    )

    slippage_cost = traded_value * slippage_bps / 10000.0
    commission_cost = traded_value * commission_rate
    stamp_duty_cost = sell_value * stamp_duty_sell_rate
    total_cost = slippage_cost + commission_cost + stamp_duty_cost
    cost_ratio = (total_cost / total_asset) if total_asset > 0 else 0.0

    return {
        "slippage_bps": slippage_bps,
        "commission_rate": commission_rate,
        "stamp_duty_sell_rate": stamp_duty_sell_rate,
        "buy_value": round(buy_value, 4),
        "sell_value": round(sell_value, 4),
        "traded_value": round(traded_value, 4),
        "slippage_cost": round(slippage_cost, 4),
        "commission_cost": round(commission_cost, 4),
        "stamp_duty_cost": round(stamp_duty_cost, 4),
        "total_execution_cost": round(total_cost, 4),
        "cost_ratio_total_asset": round(cost_ratio, 8),
    }



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute approved rebalance task")
    parser.add_argument("--queue-id", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--executor", default="manual_executor")
    parser.add_argument("--config", default="agent_config.json")
    parser.add_argument("--state-root", default="")
    parser.add_argument("--runs-root", default="")
    parser.add_argument("--timezone-offset-hours", type=int, default=8)
    parser.add_argument("--force", action="store_true", help="override execution cost guard")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--lock-timeout-sec", type=float, default=10.0)
    return parser.parse_args()


def _resolve_orders_path(queue_item: Dict[str, Any], run_dir: Path, runtime_paths: RuntimePaths) -> Path:
    raw_value = str(queue_item.get("execution_orders_path", run_dir / "execution_orders.csv"))
    path = Path(raw_value)
    if not path.is_absolute():
        path = (runtime_paths.root_dir / path).resolve()
    return path


def _run_locked_execution(
    args: argparse.Namespace,
    cfg: Dict[str, Any],
    cfg_execution: Dict[str, Any],
    runtime_paths: RuntimePaths,
) -> int:
    state_root = runtime_paths.state_root
    runs_root = runtime_paths.runs_root
    decision_log_path = runtime_paths.decision_log_path
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
    orders_path = _resolve_orders_path(queue_item, run_dir, runtime_paths)

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
    before_positions_df = load_positions_df(positions_path)
    total_asset = safe_float(account.get("total_asset"), 0.0)
    if total_asset <= 0:
        raise SystemExit("account total_asset must be > 0")
    before_cash = safe_float(account.get("cash"), 0.0)
    before_cash_ratio = safe_float(account.get("cash_ratio"), 0.0)

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
    after_positions_df = rows_to_positions_df(new_rows)

    if not orders_path.exists():
        warnings.append(f"execution orders file not found: {orders_path}")

    before_snapshot_path = run_dir / "portfolio_before_snapshot.csv"
    after_snapshot_path = run_dir / "portfolio_after_snapshot.csv"
    before_positions_df.to_csv(before_snapshot_path, index=False, encoding="utf-8-sig")
    after_positions_df.to_csv(after_snapshot_path, index=False, encoding="utf-8-sig")

    execution_settings = {
        "slippage_bps": safe_float(
            cfg_execution.get("slippage_bps"), DEFAULT_EXECUTION_SETTINGS["slippage_bps"]
        ),
        "commission_rate": safe_float(
            cfg_execution.get("commission_rate"), DEFAULT_EXECUTION_SETTINGS["commission_rate"]
        ),
        "stamp_duty_sell_rate": safe_float(
            cfg_execution.get("stamp_duty_sell_rate"),
            DEFAULT_EXECUTION_SETTINGS["stamp_duty_sell_rate"],
        ),
    }
    enforce_constraint_guard = safe_bool(cfg_execution.get("enforce_constraint_guard"), True)
    max_cost_ratio_guard = safe_float(cfg_execution.get("max_cost_ratio_total_asset"), 0.005)
    execution_costs = estimate_execution_costs(
        total_asset=total_asset,
        orders_path=orders_path,
        before_df=before_positions_df,
        after_df=after_positions_df,
        settings=execution_settings,
    )
    cost_ratio = safe_float(execution_costs.get("cost_ratio_total_asset"), 0.0)
    if cost_ratio > max_cost_ratio_guard and not args.force:
        raise SystemExit(
            "execution cost ratio guard blocked: "
            f"{cost_ratio:.4%} > {max_cost_ratio_guard:.4%}. "
            "Use --force to override."
        )
    if cost_ratio > max_cost_ratio_guard and args.force:
        warnings.append(
            f"execution cost ratio exceeds guard but forced: {cost_ratio:.4%} > {max_cost_ratio_guard:.4%}"
        )
    total_execution_cost = safe_float(execution_costs.get("total_execution_cost"), 0.0)
    projected_cash_after_cost = max(0.0, cash - total_execution_cost)
    projected_total_asset_after_cost = stock_asset + projected_cash_after_cost
    projected_cash_ratio_after_cost = (
        (projected_cash_after_cost / projected_total_asset_after_cost)
        if projected_total_asset_after_cost > 0
        else 1.0
    )
    after_positions_for_validation = after_positions_df.copy()
    if not after_positions_for_validation.empty:
        if projected_total_asset_after_cost > 0:
            after_positions_for_validation["weight"] = (
                after_positions_for_validation["market_value"] / projected_total_asset_after_cost
            ).round(6)
        else:
            after_positions_for_validation["weight"] = 0.0

    constraint_validation = validate_post_execution_constraints(
        positions_df=after_positions_for_validation,
        cash_ratio=projected_cash_ratio_after_cost,
        constraints=load_constraints(account, cfg),
        tolerance=safe_float(cfg_execution.get("constraint_tolerance"), 0.001),
    )
    if (
        enforce_constraint_guard
        and not bool(constraint_validation.get("compliant", False))
        and not args.force
    ):
        raise SystemExit(
            "post-execution constraint guard blocked: "
            + ";".join(constraint_validation.get("violations", []))
            + ". Use --force to override."
        )
    if (
        enforce_constraint_guard
        and not bool(constraint_validation.get("compliant", False))
        and args.force
    ):
        warnings.append(
            "constraint violations detected but forced: "
            + ";".join(constraint_validation.get("violations", []))
        )

    report_path = generate_portfolio_change_report(
        run_dir=run_dir,
        run_id=run_id,
        proposal_id=proposal_id,
        executor=args.executor,
        executed_at=executed_at,
        dry_run=args.dry_run,
        before_df=before_positions_df,
        after_df=after_positions_for_validation,
        before_cash_ratio=before_cash_ratio,
        after_cash_ratio=projected_cash_ratio_after_cost,
        warnings=warnings,
        execution_costs=execution_costs,
        constraint_validation=constraint_validation,
    )

    execution_result = {
        "timestamp": executed_at,
        "run_id": run_id,
        "proposal_id": proposal_id,
        "queue_id": str(queue_item.get("queue_id", "")),
        "executor": args.executor,
        "dry_run": args.dry_run,
        "before_total_asset": round(total_asset, 4),
        "before_position_count": len(before_positions_df),
        "position_count": len(new_rows),
        "before_cash": round(before_cash, 4),
        "before_cash_ratio": round(before_cash_ratio, 6),
        "stock_asset": round(stock_asset, 4),
        "cash": round(cash, 4),
        "cash_ratio": round(cash_ratio, 6),
        "before_snapshot_path": str(before_snapshot_path),
        "after_snapshot_path": str(after_snapshot_path),
        "portfolio_change_report_path": str(report_path),
        "execution_costs": execution_costs,
        "constraint_validation": constraint_validation,
        "max_cost_ratio_guard": round(max_cost_ratio_guard, 8),
        "enforce_constraint_guard": enforce_constraint_guard,
        "constraint_tolerance": round(safe_float(cfg_execution.get("constraint_tolerance"), 0.001), 8),
        "force_override": bool(args.force),
        "estimated_total_execution_cost": round(total_execution_cost, 4),
        "projected_cash_after_cost": round(projected_cash_after_cost, 4),
        "projected_total_asset_after_cost": round(projected_total_asset_after_cost, 4),
        "projected_cash_ratio_after_cost": round(projected_cash_ratio_after_cost, 6),
        "warnings": warnings,
    }

    if not args.dry_run:
        cash_after_cost = projected_cash_after_cost
        total_asset_after_cost = projected_total_asset_after_cost

        for row in new_rows:
            if total_asset_after_cost > 0:
                row["weight"] = round(safe_float(row["market_value"], 0.0) / total_asset_after_cost, 6)
            else:
                row["weight"] = 0.0

        pd.DataFrame(new_rows).to_csv(positions_path, index=False, encoding="utf-8-sig")

        account["stock_asset"] = round(stock_asset, 4)
        account["cash"] = round(cash_after_cost, 4)
        account["total_asset"] = round(total_asset_after_cost, 4)
        account["cash_ratio"] = round(
            (cash_after_cost / total_asset_after_cost) if total_asset_after_cost > 0 else 1.0,
            6,
        )
        account["last_execution_cost"] = round(total_execution_cost, 4)
        account["updated_at"] = executed_at
        write_json(account_path, account)

        queue_rows[queue_idx]["status"] = "executed"
        queue_rows[queue_idx]["executed_at"] = executed_at
        queue_rows[queue_idx]["executor"] = args.executor
        write_jsonl_atomic(queue_path, queue_rows)

        proposal["execution_status"] = "executed"
        proposal["executed_at"] = executed_at
        proposal["executed_by"] = args.executor
        proposal["execution_warnings"] = warnings
        proposal["execution_costs"] = execution_costs
        proposal["constraint_validation"] = constraint_validation
        write_json(proposal_path, proposal)

        execution_result["cash_after_cost"] = round(cash_after_cost, 4)
        execution_result["total_asset_after_cost"] = round(total_asset_after_cost, 4)
        result_path = run_dir / "execution_result.json"
        write_json(result_path, execution_result)
        append_jsonl(state_root / "execution_history.jsonl", execution_result)
        append_jsonl(
            decision_log_path,
            {
                "timestamp": executed_at,
                "run_id": run_id,
                "decision_id": proposal_id,
                "executor": args.executor,
                "final_action": "executed_rebalance",
                "note": "execution applied to state",
            },
        )
        append_jsonl(
            run_dir / "decision_log.jsonl",
            {
                "timestamp": executed_at,
                "run_id": run_id,
                "decision_id": proposal_id,
                "executor": args.executor,
                "final_action": "executed_rebalance",
                "note": "execution applied to state",
            },
        )
    else:
        execution_result["note"] = "dry-run only, state not changed"
        result_path = run_dir / "execution_result.json"
        write_json(result_path, execution_result)

    print(f"[INFO] run_id={run_id}")
    print(f"[INFO] proposal_id={proposal_id}")
    print(f"[INFO] queue_id={queue_item.get('queue_id', '')}")
    print(f"[INFO] dry_run={args.dry_run}")
    print(f"[INFO] position_count={len(new_rows)}")
    print(f"[INFO] cash_ratio={cash_ratio:.2%}")
    print(f"[INFO] report={report_path}")
    if warnings:
        print(f"[WARN] warnings={len(warnings)}")

    return 0


def main() -> int:
    args = parse_args()
    cfg, runtime_paths = resolve_runtime_paths(
        Path(args.config),
        overrides={
            "state_root": args.state_root,
            "runs_root": args.runs_root,
        },
    )
    cfg_execution = cfg.get("execution", {}) if isinstance(cfg, dict) else {}
    manual_only = safe_bool(cfg_execution.get("manual_only"), False)

    if manual_only and not args.dry_run:
        raise SystemExit("execution manual_only is enabled; only --dry-run is allowed")

    try:
        with advisory_lock(runtime_paths.state_root / "queues.lock", timeout_sec=args.lock_timeout_sec):
            return _run_locked_execution(args, cfg, cfg_execution, runtime_paths)
    except LockTimeoutError as exc:
        raise SystemExit(f"execution queue is busy: {exc}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
