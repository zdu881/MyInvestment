#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Agent system runtime for daily multi-round investment workflow.

Phases:
- preopen  : morning portfolio check and brief
- intraday : alert scan for existing positions
- postclose: run screening + AI research + allocation proposal + advice report
- all      : execute the three phases in order within one run_id
"""

import argparse
import json
import subprocess
import sys
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Optional tool imports. The runtime still works in degraded mode if unavailable.
try:
    from mcp_tools import (
        calculate_ah_premium,
        get_financial_health_check,
        search_market_sentiment,
    )
except Exception:
    calculate_ah_premium = None
    get_financial_health_check = None
    search_market_sentiment = None


DEFAULT_CONFIG = {
    "timezone_offset_hours": 8,
    "paths": {
        "runs_root": "runs",
        "state_root": "state",
        "step1_csv": "candidates.csv",
        "step2_csv": "candidates_step2.csv",
    },
    "postclose": {
        "max_candidates_for_research": 8,
        "max_new_positions": 3,
        "default_transaction_cost_rate": 0.0015,
    },
    "gates": {
        "min_evidence_completeness": 0.6,
        "min_action_delta": 0.02,
        "max_allowed_constraint_violations": 0,
    },
    "constraints": {
        "max_single_weight": 0.3,
        "max_industry_weight": 0.5,
        "min_cash_ratio": 0.1,
    },
    "alerts": {
        "intraday_loss_alert_pct": -5.0,
        "intraday_profit_alert_pct": 8.0,
    },
}


@dataclass
class RunContext:
    run_id: str
    trading_date: str
    as_of_ts: str
    run_dir: Path


def merge_dict(base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in update.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = merge_dict(out[k], v)
        else:
            out[k] = v
    return out


def load_json(path: Path, default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not path.exists():
        return {} if default is None else default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


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


def now_with_offset(hours: int) -> datetime:
    return datetime.now(timezone(timedelta(hours=hours)))


def score_candidate(row: pd.Series) -> float:
    dy = safe_float(row.get("股息率(%)", 0), 0.0)
    pe = safe_float(row.get("PE(TTM)", 99), 99.0)
    pb = safe_float(row.get("PB", 99), 99.0)
    return dy * 2.0 + max(0.0, 12 - pe) + max(0.0, 1.2 - pb) * 10


class AgentSystem:
    def __init__(self, config_path: str) -> None:
        cfg = load_json(Path(config_path), default={})
        self.config = merge_dict(DEFAULT_CONFIG, cfg)
        self.paths = self.config["paths"]

    def make_context(self) -> RunContext:
        tz_hours = int(self.config.get("timezone_offset_hours", 8))
        now = now_with_offset(tz_hours)
        run_id = str(uuid.uuid4())
        trading_date = now.date().isoformat()
        run_dir = Path(self.paths["runs_root"]) / trading_date / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        return RunContext(
            run_id=run_id,
            trading_date=trading_date,
            as_of_ts=now.isoformat(timespec="seconds"),
            run_dir=run_dir,
        )

    def _load_positions(self) -> pd.DataFrame:
        state_root = Path(self.paths["state_root"])
        path = state_root / "current_positions.csv"
        if not path.exists():
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

        df = pd.read_csv(path, dtype={"ticker": str})
        if df.empty:
            return df

        df = df.copy()
        df["ticker"] = df["ticker"].apply(normalize_ticker)
        for col in ["shares", "avg_cost", "last_price", "market_value", "weight"]:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = df[col].apply(safe_float)

        # Fill market value if missing.
        mv_missing = df["market_value"] <= 0
        df.loc[mv_missing, "market_value"] = (
            df.loc[mv_missing, "shares"] * df.loc[mv_missing, "last_price"]
        )

        total_mv = df["market_value"].sum()
        if total_mv > 0:
            weight_missing = df["weight"] <= 0
            df.loc[weight_missing, "weight"] = df.loc[weight_missing, "market_value"] / total_mv

        if "industry" not in df.columns:
            df["industry"] = "未知"
        if "name" not in df.columns:
            df["name"] = "N/A"
        return df

    def _load_account(self) -> Dict[str, Any]:
        state_root = Path(self.paths["state_root"])
        account = load_json(state_root / "account_snapshot.json", default={})
        constraints = self.config.get("constraints", {})
        return {
            "cash": safe_float(account.get("cash"), 10000.0),
            "total_asset": safe_float(account.get("total_asset"), 10000.0),
            "stock_asset": safe_float(account.get("stock_asset"), 0.0),
            "cash_ratio": safe_float(account.get("cash_ratio"), 1.0),
            "max_single_weight": safe_float(
                account.get("max_single_weight"), constraints.get("max_single_weight", 0.3)
            ),
            "max_industry_weight": safe_float(
                account.get("max_industry_weight"), constraints.get("max_industry_weight", 0.5)
            ),
            "min_cash_ratio": safe_float(
                account.get("min_cash_ratio"), constraints.get("min_cash_ratio", 0.1)
            ),
            "risk_profile": str(account.get("risk_profile", "defensive")),
        }

    def _run_script(self, script_name: str, log_path: Path) -> None:
        cmd = [sys.executable, script_name]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        text = "".join(
            [
                f"$ {' '.join(cmd)}\n",
                "\n[STDOUT]\n",
                proc.stdout,
                "\n[STDERR]\n",
                proc.stderr,
                f"\n[RETURN_CODE] {proc.returncode}\n",
            ]
        )
        log_path.write_text(text, encoding="utf-8")
        if proc.returncode != 0:
            raise RuntimeError(f"script failed: {script_name} (code={proc.returncode})")

    def run_preopen(self, ctx: RunContext, dry_run: bool) -> List[str]:
        positions = self._load_positions()
        account = self._load_account()

        lines: List[str] = []
        lines.append(f"# Preopen Brief ({ctx.trading_date})")
        lines.append("")
        lines.append(f"- run_id: {ctx.run_id}")
        lines.append(f"- as_of: {ctx.as_of_ts}")
        lines.append(f"- total_asset: {account['total_asset']:.2f}")
        lines.append(f"- cash: {account['cash']:.2f}")
        lines.append(f"- cash_ratio: {account['cash_ratio']:.2%}")
        lines.append(f"- holdings_count: {len(positions)}")
        lines.append("")

        if positions.empty:
            lines.append("## Holdings Overview")
            lines.append("")
            lines.append("- 当前无持仓，盘前阶段仅监控候选池与风险事件。")
        else:
            lines.append("## Holdings Overview")
            lines.append("")
            top = positions.sort_values(by="market_value", ascending=False).head(5)
            for _, row in top.iterrows():
                pnl_pct = 0.0
                avg_cost = safe_float(row.get("avg_cost"), 0.0)
                last_price = safe_float(row.get("last_price"), 0.0)
                if avg_cost > 0:
                    pnl_pct = (last_price / avg_cost - 1) * 100
                lines.append(
                    f"- {row['ticker']} {row.get('name','N/A')} | weight={safe_float(row.get('weight')):.2%} | pnl={pnl_pct:.2f}%"
                )

        lines.append("")
        lines.append("## Phase Result")
        lines.append("")
        lines.append("- 盘前阶段仅输出观察摘要，不直接给调仓动作。")
        if dry_run:
            lines.append("- 当前为 dry-run，本轮未调用外部数据源。")

        brief_path = ctx.run_dir / "preopen_brief.md"
        brief_path.write_text("\n".join(lines), encoding="utf-8")
        return [str(brief_path)]

    def run_intraday(self, ctx: RunContext, dry_run: bool) -> List[str]:
        positions = self._load_positions()
        alerts_cfg = self.config.get("alerts", {})
        loss_threshold = safe_float(alerts_cfg.get("intraday_loss_alert_pct"), -5.0)
        profit_threshold = safe_float(alerts_cfg.get("intraday_profit_alert_pct"), 8.0)

        alerts: List[Dict[str, Any]] = []
        if not positions.empty:
            for _, row in positions.iterrows():
                avg_cost = safe_float(row.get("avg_cost"), 0.0)
                last_price = safe_float(row.get("last_price"), 0.0)
                if avg_cost <= 0:
                    continue
                pnl_pct = (last_price / avg_cost - 1) * 100.0

                level = None
                msg = ""
                if pnl_pct <= loss_threshold:
                    level = "high"
                    msg = f"跌幅触发预警（{pnl_pct:.2f}% <= {loss_threshold:.2f}%）"
                elif pnl_pct >= profit_threshold:
                    level = "medium"
                    msg = f"涨幅达到观察阈值（{pnl_pct:.2f}% >= {profit_threshold:.2f}%）"

                if level is not None:
                    alerts.append(
                        {
                            "run_id": ctx.run_id,
                            "trading_date": ctx.trading_date,
                            "as_of_ts": ctx.as_of_ts,
                            "ticker": row["ticker"],
                            "name": row.get("name", "N/A"),
                            "pnl_pct": round(pnl_pct, 4),
                            "severity": level,
                            "message": msg,
                        }
                    )

        alerts_path = ctx.run_dir / "intraday_alerts.jsonl"
        write_jsonl(alerts_path, alerts)

        brief_lines = [
            f"# Intraday Brief ({ctx.trading_date})",
            "",
            f"- run_id: {ctx.run_id}",
            f"- as_of: {ctx.as_of_ts}",
            f"- alerts_count: {len(alerts)}",
            "",
            "## Alerts",
            "",
        ]
        if alerts:
            for alert in alerts:
                brief_lines.append(
                    f"- [{alert['severity']}] {alert['ticker']} {alert['name']}: {alert['message']}"
                )
        else:
            brief_lines.append("- 未触发盘中预警阈值。")

        if dry_run:
            brief_lines.append("")
            brief_lines.append("- 当前为 dry-run，本轮未拉取实时外部行情。")

        brief_path = ctx.run_dir / "intraday_brief.md"
        brief_path.write_text("\n".join(brief_lines), encoding="utf-8")
        return [str(alerts_path), str(brief_path)]

    def _load_candidate_df(self, step1_path: Path, step2_path: Path) -> Tuple[pd.DataFrame, str]:
        if step2_path.exists():
            df2 = pd.read_csv(step2_path, dtype={"股票代码": str, "ticker": str})
            if not df2.empty:
                return df2, "step2"

        if step1_path.exists():
            df1 = pd.read_csv(step1_path, dtype={"股票代码": str, "ticker": str})
            if not df1.empty:
                return df1, "step1"

        return pd.DataFrame(), "none"

    def _run_tools(self, ticker: str, dry_run: bool) -> Dict[str, Any]:
        if dry_run or get_financial_health_check is None:
            return {
                "health": {"ok": False, "message": "dry_run_or_tool_unavailable", "data": {}},
                "sentiment": {
                    "ok": True,
                    "message": "mocked",
                    "data": {
                        "negative_events": [
                            {
                                "type": "placeholder",
                                "severity": "medium",
                                "headline": "dry-run placeholder",
                            }
                        ]
                    },
                },
                "ah": {"ok": False, "message": "dry_run_or_tool_unavailable", "data": {}},
            }

        health = get_financial_health_check(ticker)
        sentiment = search_market_sentiment(ticker)
        ah = calculate_ah_premium(ticker)
        return {"health": health, "sentiment": sentiment, "ah": ah}

    def _derive_research_summary(self, ticker: str, tools: Dict[str, Any]) -> Dict[str, Any]:
        health = tools["health"]
        sentiment = tools["sentiment"]
        ah = tools["ah"]

        risk_flags: List[str] = []
        thesis: List[str] = []

        h_ok = bool(health.get("ok"))
        s_ok = bool(sentiment.get("ok"))
        ah_ok = bool(ah.get("ok"))

        if h_ok:
            h_data = health.get("data", {})
            if h_data.get("big_fluctuation"):
                risk_flags.extend(h_data.get("risk_flags", []))
            else:
                thesis.append("近三期财务波动可控")

            ocf_growth = h_data.get("ocf_growth_pct")
            if isinstance(ocf_growth, (int, float)) and ocf_growth > 0:
                thesis.append("经营现金流趋势向上")
        else:
            risk_flags.append("财务健康检查失败或缺失")

        if s_ok:
            events = sentiment.get("data", {}).get("negative_events", [])
            if events and events[0].get("type") == "placeholder":
                risk_flags.append("舆情工具仍为占位数据")
            elif len(events) >= 3:
                risk_flags.append("近3个月负面事件偏多")
        else:
            risk_flags.append("舆情检查失败或缺失")

        if ah_ok:
            premium = ah.get("data", {}).get("ah_premium_pct")
            if isinstance(premium, (int, float)) and premium > 30:
                risk_flags.append(f"A/H 溢价偏高（{premium:.2f}%）")
            elif isinstance(premium, (int, float)):
                thesis.append(f"A/H 溢价可控（{premium:.2f}%）")
        else:
            thesis.append("A/H 溢价信息不可用，不作为阻断项")

        if not thesis:
            thesis.append("估值处于防御策略可跟踪区间")

        tool_success = int(h_ok) + int(s_ok) + int(ah_ok)
        confidence = round(tool_success / 3.0, 2)
        if "舆情工具仍为占位数据" in risk_flags:
            confidence = round(max(0.0, confidence - 0.2), 2)

        # Conservative verdict policy.
        if confidence < 0.5:
            verdict = "observe"
        elif len(risk_flags) >= 3:
            verdict = "avoid"
        elif len(risk_flags) >= 1:
            verdict = "observe"
        else:
            verdict = "buy"

        return {
            "ticker": ticker,
            "thesis": thesis[:3],
            "risk_flags": risk_flags[:4],
            "confidence": confidence,
            "verdict": verdict,
            "tool_evidence": tools,
        }

    def _build_target_weights(
        self,
        positions: pd.DataFrame,
        candidate_df: pd.DataFrame,
        research_rows: List[Dict[str, Any]],
        account: Dict[str, Any],
    ) -> Dict[str, float]:
        max_single = safe_float(account.get("max_single_weight"), 0.3)
        min_cash = safe_float(account.get("min_cash_ratio"), 0.1)
        max_positions = int(self.config.get("postclose", {}).get("max_new_positions", 3))
        investable = max(0.0, 1.0 - min_cash)

        scored: List[Tuple[str, float]] = []
        df = candidate_df.copy()
        if not df.empty:
            if "股票代码" not in df.columns and "ticker" in df.columns:
                df["股票代码"] = df["ticker"]
            df["__score"] = df.apply(score_candidate, axis=1)
            score_map = {
                normalize_ticker(r["股票代码"]): safe_float(r["__score"])
                for _, r in df.iterrows()
            }
        else:
            score_map = {}

        for item in research_rows:
            if item.get("verdict") == "buy":
                t = normalize_ticker(item["ticker"])
                scored.append((t, score_map.get(t, 0.0) + item.get("confidence", 0.0)))

        scored = sorted(scored, key=lambda x: x[1], reverse=True)
        selected = [t for t, _ in scored[:max_positions]]

        target: Dict[str, float] = {}
        if selected:
            equal_w = min(max_single, investable / len(selected))
            for ticker in selected:
                target[ticker] = round(equal_w, 4)
            return target

        # Fallback: keep existing top holdings if no buy candidates.
        if positions.empty:
            return target

        keep = positions.sort_values(by="weight", ascending=False).head(max_positions)
        if keep.empty:
            return target

        total_keep = keep["weight"].sum()
        if total_keep <= 0:
            equal_w = min(max_single, investable / len(keep))
            for _, row in keep.iterrows():
                target[normalize_ticker(row["ticker"])] = round(equal_w, 4)
        else:
            scale = investable / total_keep
            for _, row in keep.iterrows():
                w = min(max_single, safe_float(row.get("weight"), 0.0) * scale)
                target[normalize_ticker(row["ticker"])] = round(w, 4)

        return target

    def _industry_map(self, positions: pd.DataFrame, candidate_df: pd.DataFrame) -> Dict[str, str]:
        out: Dict[str, str] = {}
        if not positions.empty:
            for _, row in positions.iterrows():
                out[normalize_ticker(row.get("ticker"))] = str(row.get("industry", "未知"))

        if not candidate_df.empty:
            ticker_col = "股票代码" if "股票代码" in candidate_df.columns else "ticker"
            industry_col = None
            for c in ["行业", "industry"]:
                if c in candidate_df.columns:
                    industry_col = c
                    break
            if ticker_col in candidate_df.columns and industry_col:
                for _, row in candidate_df.iterrows():
                    out[normalize_ticker(row[ticker_col])] = str(row[industry_col])
        return out

    def _evaluate_constraints(
        self,
        target_weights: Dict[str, float],
        industry_map: Dict[str, str],
        account: Dict[str, Any],
    ) -> List[str]:
        violations: List[str] = []
        max_single = safe_float(account.get("max_single_weight"), 0.3)
        max_industry = safe_float(account.get("max_industry_weight"), 0.5)
        min_cash = safe_float(account.get("min_cash_ratio"), 0.1)

        for ticker, weight in target_weights.items():
            if weight > max_single + 1e-9:
                violations.append(f"single_weight_exceeded:{ticker}:{weight:.4f}>{max_single:.4f}")

        industry_weights: Dict[str, float] = {}
        for ticker, weight in target_weights.items():
            industry = industry_map.get(ticker, "未知")
            industry_weights[industry] = industry_weights.get(industry, 0.0) + weight
        for industry, weight in industry_weights.items():
            if weight > max_industry + 1e-9:
                violations.append(
                    f"industry_weight_exceeded:{industry}:{weight:.4f}>{max_industry:.4f}"
                )

        invested = sum(target_weights.values())
        cash_ratio = 1.0 - invested
        if cash_ratio < min_cash - 1e-9:
            violations.append(f"cash_ratio_below_min:{cash_ratio:.4f}<{min_cash:.4f}")

        return violations

    def _build_actions(
        self,
        positions: pd.DataFrame,
        target_weights: Dict[str, float],
        min_action_delta: float,
    ) -> pd.DataFrame:
        current_map = {
            normalize_ticker(row["ticker"]): safe_float(row.get("weight"), 0.0)
            for _, row in positions.iterrows()
        }
        names = {
            normalize_ticker(row["ticker"]): str(row.get("name", "N/A"))
            for _, row in positions.iterrows()
        }

        tickers = sorted(set(current_map.keys()) | set(target_weights.keys()))
        rows: List[Dict[str, Any]] = []
        for t in tickers:
            cur = current_map.get(t, 0.0)
            tgt = target_weights.get(t, 0.0)
            delta = tgt - cur

            if abs(delta) < min_action_delta:
                action = "HOLD"
                reason = "delta below min_action_delta"
            elif cur <= 0 and tgt > 0:
                action = "BUY"
                reason = "new target position"
            elif cur > 0 and tgt <= 0:
                action = "SELL"
                reason = "remove from target portfolio"
            elif delta > 0:
                action = "INCREASE"
                reason = "increase weight toward target"
            else:
                action = "DECREASE"
                reason = "decrease weight toward target"

            rows.append(
                {
                    "action": action,
                    "ticker": t,
                    "name": names.get(t, "N/A"),
                    "current_weight": round(cur, 4),
                    "target_weight": round(tgt, 4),
                    "delta_weight": round(delta, 4),
                    "reason": reason,
                    "priority": 1 if action in {"SELL", "DECREASE"} else 2,
                    "trigger_type": "postclose_rebalance",
                }
            )

        if not rows:
            return pd.DataFrame(
                columns=[
                    "action",
                    "ticker",
                    "name",
                    "current_weight",
                    "target_weight",
                    "delta_weight",
                    "reason",
                    "priority",
                    "trigger_type",
                ]
            )
        return pd.DataFrame(rows)

    def _generate_advice_report(
        self,
        ctx: RunContext,
        account: Dict[str, Any],
        source_level: str,
        research_rows: List[Dict[str, Any]],
        actions_df: pd.DataFrame,
        gate_result: Dict[str, Any],
        proposal: Dict[str, Any],
    ) -> Path:
        lines: List[str] = []
        lines.append("# 资产变动建议书")
        lines.append("")
        lines.append(f"- run_id: {ctx.run_id}")
        lines.append(f"- trading_date: {ctx.trading_date}")
        lines.append(f"- as_of: {ctx.as_of_ts}")
        lines.append(f"- data_source_level: {source_level}")
        lines.append("")

        lines.append("## 执行摘要")
        lines.append("")
        lines.append(
            f"- 建议结论：{proposal.get('decision', 'watch')}（evidence={proposal.get('evidence_completeness', 0):.2f}, violations={len(proposal.get('constraint_violations', []))}）"
        )
        lines.append(
            f"- 估算换手：{proposal.get('turnover_est', 0):.2%}，估算交易成本：{proposal.get('transaction_cost_est', 0):.2f}"
        )
        lines.append("")

        lines.append("## 当前组合体检")
        lines.append("")
        lines.append(f"- total_asset: {account['total_asset']:.2f}")
        lines.append(f"- cash_ratio: {account['cash_ratio']:.2%}")
        lines.append(
            f"- 约束：max_single={account['max_single_weight']:.2%}, max_industry={account['max_industry_weight']:.2%}, min_cash={account['min_cash_ratio']:.2%}"
        )
        lines.append("")

        lines.append("## 建议动作清单")
        lines.append("")
        if actions_df.empty:
            lines.append("- 无需动作，建议保持现有仓位。")
        else:
            for _, row in actions_df.iterrows():
                lines.append(
                    f"- {row['action']} {row['ticker']} {row['name']} | {row['current_weight']:.2%} -> {row['target_weight']:.2%} | {row['reason']}"
                )
        lines.append("")

        lines.append("## 证据链")
        lines.append("")
        if not research_rows:
            lines.append("- 本轮无候选研究结果，证据不足。")
        else:
            for item in research_rows[:8]:
                thesis = "；".join(item.get("thesis", [])) or "N/A"
                risks = "；".join(item.get("risk_flags", [])) or "无"
                lines.append(
                    f"- {item['ticker']} | verdict={item['verdict']} | confidence={item['confidence']:.2f} | thesis={thesis} | risk={risks}"
                )
        lines.append("")

        lines.append("## 风险与反证")
        lines.append("")
        if gate_result.get("hard_risk_block"):
            lines.append("- 命中风险硬门：当前存在高风险标的，禁止新增仓位。")
        else:
            lines.append("- 未命中风险硬门。")
        if gate_result.get("constraint_violations"):
            for v in gate_result["constraint_violations"]:
                lines.append(f"- 约束违例：{v}")
        else:
            lines.append("- 未发现仓位约束违例。")
        lines.append("")

        lines.append("## 变动前后对比")
        lines.append("")
        lines.append(f"- decision: {proposal.get('decision', 'watch')}")
        lines.append(
            f"- evidence_completeness: {proposal.get('evidence_completeness', 0):.2f} (threshold={gate_result.get('min_evidence_threshold', 0):.2f})"
        )
        lines.append(f"- actionable_count: {gate_result.get('actionable_count', 0)}")
        lines.append("")

        lines.append("## 执行约束")
        lines.append("")
        lines.append("- 本建议需人工审批后执行。")
        lines.append("- 若下一轮证据完整性下降，结论自动降级为观察。")
        lines.append("")

        lines.append("## 最终结论")
        lines.append("")
        lines.append(f"- {proposal.get('decision', 'watch')}")

        report_path = ctx.run_dir / "advice_report.md"
        report_path.write_text("\n".join(lines), encoding="utf-8")
        return report_path

    def run_postclose(self, ctx: RunContext, dry_run: bool) -> List[str]:
        artifacts: List[str] = []
        logs_dir = ctx.run_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)

        step1_path = Path(self.paths["step1_csv"])
        step2_path = Path(self.paths["step2_csv"])

        # Try to refresh candidates unless dry-run.
        if not dry_run:
            try:
                self._run_script("step1_screener.py", logs_dir / "step1.log")
            except Exception as e:
                (logs_dir / "step1_error.txt").write_text(str(e), encoding="utf-8")
            try:
                self._run_script("step2_financial_cleaner.py", logs_dir / "step2.log")
            except Exception as e:
                (logs_dir / "step2_error.txt").write_text(str(e), encoding="utf-8")

        if step1_path.exists():
            copied_step1 = ctx.run_dir / "candidates_step1.csv"
            copied_step1.write_bytes(step1_path.read_bytes())
            artifacts.append(str(copied_step1))
        if step2_path.exists():
            copied_step2 = ctx.run_dir / "candidates_step2.csv"
            copied_step2.write_bytes(step2_path.read_bytes())
            artifacts.append(str(copied_step2))

        candidate_df, source_level = self._load_candidate_df(step1_path, step2_path)
        if candidate_df.empty:
            raise RuntimeError("no candidate data available for postclose phase")

        ticker_col = "股票代码" if "股票代码" in candidate_df.columns else "ticker"
        candidate_df = candidate_df.copy()
        candidate_df["ticker_norm"] = candidate_df[ticker_col].apply(normalize_ticker)
        if "名称" not in candidate_df.columns:
            candidate_df["名称"] = "N/A"

        top_n = int(self.config.get("postclose", {}).get("max_candidates_for_research", 8))
        candidate_df["__score"] = candidate_df.apply(score_candidate, axis=1)
        top_df = candidate_df.sort_values(by="__score", ascending=False).head(top_n).reset_index(drop=True)

        research_rows: List[Dict[str, Any]] = []
        for _, row in top_df.iterrows():
            ticker = normalize_ticker(row["ticker_norm"])
            tools = self._run_tools(ticker, dry_run=dry_run)
            summary = self._derive_research_summary(ticker, tools)
            summary["run_id"] = ctx.run_id
            summary["trading_date"] = ctx.trading_date
            summary["as_of_ts"] = ctx.as_of_ts
            summary["name"] = str(row.get("名称", "N/A"))
            summary["source"] = source_level
            research_rows.append(summary)

        research_path = ctx.run_dir / "stock_research.jsonl"
        write_jsonl(research_path, research_rows)
        artifacts.append(str(research_path))

        positions = self._load_positions()
        account = self._load_account()
        target_weights = self._build_target_weights(positions, candidate_df, research_rows, account)

        industry_map = self._industry_map(positions, candidate_df)
        violations = self._evaluate_constraints(target_weights, industry_map, account)

        min_action_delta = safe_float(self.config.get("gates", {}).get("min_action_delta"), 0.02)
        actions_df = self._build_actions(positions, target_weights, min_action_delta)
        actions_path = ctx.run_dir / "rebalance_actions.csv"
        actions_df.to_csv(actions_path, index=False, encoding="utf-8-sig")
        artifacts.append(str(actions_path))

        current_weights = {
            normalize_ticker(r["ticker"]): safe_float(r.get("weight"), 0.0)
            for _, r in positions.iterrows()
        }
        positive_delta = 0.0
        for _, row in actions_df.iterrows():
            delta = safe_float(row.get("delta_weight"), 0.0)
            if delta > 0:
                positive_delta += delta

        research_map = {r["ticker"]: r for r in research_rows}
        hard_risk_block = False
        for t in current_weights.keys():
            item = research_map.get(t)
            if item and item.get("verdict") == "avoid":
                hard_risk_block = True
                break

        evidence_completeness = 0.0
        if research_rows:
            evidence_completeness = sum(safe_float(r.get("confidence"), 0.0) for r in research_rows) / len(research_rows)

        actionable_count = 0
        if not actions_df.empty:
            actionable_count = int((actions_df["action"] != "HOLD").sum())

        gate_cfg = self.config.get("gates", {})
        min_evidence = safe_float(gate_cfg.get("min_evidence_completeness"), 0.6)
        max_violations = int(gate_cfg.get("max_allowed_constraint_violations", 0))

        gate_failures: List[str] = []
        if hard_risk_block and positive_delta > 0:
            gate_failures.append("hard_risk_block_new_buy")
        if evidence_completeness < min_evidence:
            gate_failures.append("evidence_below_threshold")
        if len(violations) > max_violations:
            gate_failures.append("constraint_violations")

        if gate_failures:
            decision = "watch"
        elif actionable_count == 0:
            decision = "hold"
        else:
            decision = "rebalance"

        turnover = 0.0
        if not actions_df.empty:
            turnover = actions_df["delta_weight"].abs().sum() / 2.0

        cost_rate = safe_float(
            self.config.get("postclose", {}).get("default_transaction_cost_rate"), 0.0015
        )
        transaction_cost_est = turnover * safe_float(account.get("total_asset"), 0.0) * cost_rate

        proposal = {
            "proposal_id": f"proposal-{ctx.run_id[:8]}",
            "run_id": ctx.run_id,
            "trading_date": ctx.trading_date,
            "as_of_ts": ctx.as_of_ts,
            "base_portfolio": [
                {
                    "ticker": normalize_ticker(r["ticker"]),
                    "name": str(r.get("name", "N/A")),
                    "weight": round(safe_float(r.get("weight"), 0.0), 4),
                }
                for _, r in positions.iterrows()
            ],
            "new_portfolio": [
                {"ticker": t, "weight": round(w, 4), "industry": industry_map.get(t, "未知")}
                for t, w in sorted(target_weights.items())
            ],
            "target_weights": {t: round(w, 4) for t, w in target_weights.items()},
            "turnover_est": round(float(turnover), 6),
            "transaction_cost_est": round(float(transaction_cost_est), 4),
            "risk_delta": {
                "hard_risk_block": hard_risk_block,
                "constraint_violations": violations,
            },
            "expected_return_delta": None,
            "evidence_completeness": round(float(evidence_completeness), 4),
            "constraint_violations": violations,
            "gate_failures": gate_failures,
            "decision": decision,
        }

        proposal_path = ctx.run_dir / "allocation_proposal.json"
        write_json(proposal_path, proposal)
        artifacts.append(str(proposal_path))

        gate_result = {
            "hard_risk_block": hard_risk_block,
            "new_buy_weight_sum": round(positive_delta, 6),
            "evidence_completeness": round(float(evidence_completeness), 4),
            "min_evidence_threshold": min_evidence,
            "constraint_violations": violations,
            "actionable_count": actionable_count,
            "gate_failures": gate_failures,
            "decision": decision,
        }

        decision_row = {
            "timestamp": ctx.as_of_ts,
            "run_id": ctx.run_id,
            "decision_id": proposal["proposal_id"],
            "gate_results": gate_result,
            "approved_by": "pending_manual_review",
            "final_action": decision,
            "notes": "auto-generated by agent_system",
        }

        decision_log_path = ctx.run_dir / "decision_log.jsonl"
        append_jsonl(decision_log_path, decision_row)
        artifacts.append(str(decision_log_path))

        # Global rolling decision log for cross-run lookup.
        append_jsonl(Path("decision_log.jsonl"), decision_row)

        advice_path = self._generate_advice_report(
            ctx=ctx,
            account=account,
            source_level=source_level,
            research_rows=research_rows,
            actions_df=actions_df,
            gate_result=gate_result,
            proposal=proposal,
        )
        artifacts.append(str(advice_path))

        return artifacts

    def run(self, phase: str, dry_run: bool) -> int:
        start_dt = datetime.now(timezone.utc)
        ctx = self.make_context()
        steps: List[Dict[str, Any]] = []
        artifacts: List[str] = []
        status = "success"
        error_summary = ""

        phase_order = [phase]
        if phase == "all":
            phase_order = ["preopen", "intraday", "postclose"]

        try:
            for ph in phase_order:
                step_info = {"phase": ph, "status": "success", "error": ""}
                try:
                    if ph == "preopen":
                        artifacts.extend(self.run_preopen(ctx, dry_run=dry_run))
                    elif ph == "intraday":
                        artifacts.extend(self.run_intraday(ctx, dry_run=dry_run))
                    elif ph == "postclose":
                        artifacts.extend(self.run_postclose(ctx, dry_run=dry_run))
                    else:
                        raise ValueError(f"unsupported phase: {ph}")
                except Exception as e:
                    step_info["status"] = "failed"
                    step_info["error"] = str(e)
                    status = "failed"
                    error_summary = f"{ph}: {e}"
                    # Continue when running all to preserve partial outputs.
                    if phase != "all":
                        raise
                finally:
                    steps.append(step_info)
        except Exception:
            status = "failed"
            if not error_summary:
                error_summary = traceback.format_exc()

        end_dt = datetime.now(timezone.utc)
        duration_sec = (end_dt - start_dt).total_seconds()

        manifest = {
            "run_id": ctx.run_id,
            "trading_date": ctx.trading_date,
            "as_of_ts": ctx.as_of_ts,
            "start_ts_utc": start_dt.isoformat(timespec="seconds") + "Z",
            "end_ts_utc": end_dt.isoformat(timespec="seconds") + "Z",
            "duration_sec": round(duration_sec, 3),
            "phase": phase,
            "dry_run": dry_run,
            "status": status,
            "error_summary": error_summary,
            "steps": steps,
            "artifacts": artifacts,
        }

        manifest_path = ctx.run_dir / "run_manifest.json"
        write_json(manifest_path, manifest)
        print(f"[INFO] run_id={ctx.run_id}")
        print(f"[INFO] manifest={manifest_path}")
        print(f"[INFO] status={status}")

        return 0 if status == "success" else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily agent runtime")
    parser.add_argument(
        "--phase",
        default="all",
        choices=["preopen", "intraday", "postclose", "all"],
        help="run one phase or all phases",
    )
    parser.add_argument(
        "--config",
        default="agent_config.json",
        help="path to runtime config JSON",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="skip external calls and use degraded mock outputs",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    system = AgentSystem(config_path=args.config)
    return system.run(phase=args.phase, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
