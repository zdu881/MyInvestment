from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from agent_system import AgentSystem


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _system(tmp_path: Path, strategy_lines: dict) -> AgentSystem:
    config_path = tmp_path / "agent_config.json"
    _write_json(
        config_path,
        {
            "paths": {
                "runs_root": "runs",
                "state_root": "state",
                "knowledge_root": "knowledge",
            },
            "constraints": {
                "max_single_weight": 0.3,
                "max_industry_weight": 0.6,
                "min_cash_ratio": 0.1,
            },
            "strategy_lines": strategy_lines,
            "feedback": {
                "default_min_confidence_buy": 0.75,
            },
        },
    )
    return AgentSystem(str(config_path))


def test_parallel_strategy_lines_keep_value_core_and_short_satellite(tmp_path: Path) -> None:
    system = _system(
        tmp_path,
        {
            "enabled": True,
            "value": {
                "enabled": True,
                "capital_weight": 0.6,
                "max_positions": 2,
                "max_single_weight": 0.3,
                "min_confidence_buy": 0.75,
                "allow_existing_fallback": False,
            },
            "short": {
                "enabled": True,
                "capital_weight": 0.2,
                "max_positions": 1,
                "max_single_weight": 0.1,
                "min_confidence_buy": 0.8,
                "allow_existing_fallback": False,
                "exclude_value_tickers": True,
            },
        },
    )
    positions = pd.DataFrame()
    candidates = pd.DataFrame(
        [
            {"股票代码": "600001", "名称": "核心一", "PE(TTM)": 5.0, "PB": 0.5, "股息率(%)": 4.0, "行业": "银行"},
            {"股票代码": "600002", "名称": "核心二", "PE(TTM)": 6.0, "PB": 0.6, "股息率(%)": 3.5, "行业": "公用"},
            {"股票代码": "600003", "名称": "短线一", "PE(TTM)": 7.0, "PB": 0.7, "股息率(%)": 3.0, "行业": "交运"},
        ]
    )
    research_rows = [
        {"ticker": "600001", "verdict": "buy", "confidence": 0.95, "risk_flags": []},
        {"ticker": "600002", "verdict": "buy", "confidence": 0.9, "risk_flags": []},
        {"ticker": "600003", "verdict": "buy", "confidence": 0.85, "risk_flags": []},
    ]
    account = {"max_single_weight": 0.3, "min_cash_ratio": 0.1}

    target, feedback_context, plan = system._build_parallel_target_weights(
        positions, candidates, research_rows, account
    )

    assert target == {"600001": 0.3, "600002": 0.3, "600003": 0.1}
    assert feedback_context["strategy_mode"] == "parallel_lines"
    assert feedback_context["selected_count"] == 3
    assert plan["lines"]["value"]["selected_tickers"] == ["600001", "600002"]
    assert plan["lines"]["short"]["selected_tickers"] == ["600003"]
    assert plan["lines"]["short"]["excluded_ticker_count"] == 2


def test_parallel_strategy_lines_cap_duplicate_line_weight(tmp_path: Path) -> None:
    system = _system(
        tmp_path,
        {
            "enabled": True,
            "value": {
                "enabled": True,
                "capital_weight": 0.3,
                "max_positions": 1,
                "max_single_weight": 0.3,
                "min_confidence_buy": 0.75,
                "allow_existing_fallback": False,
            },
            "short": {
                "enabled": True,
                "capital_weight": 0.2,
                "max_positions": 1,
                "max_single_weight": 0.2,
                "min_confidence_buy": 0.75,
                "allow_existing_fallback": False,
                "exclude_value_tickers": False,
            },
        },
    )
    candidates = pd.DataFrame(
        [
            {"股票代码": "600001", "名称": "重复标的", "PE(TTM)": 5.0, "PB": 0.5, "股息率(%)": 4.0, "行业": "银行"},
        ]
    )
    research_rows = [{"ticker": "600001", "verdict": "buy", "confidence": 0.95, "risk_flags": []}]
    account = {"max_single_weight": 0.3, "min_cash_ratio": 0.1}

    target, _, plan = system._build_parallel_target_weights(
        pd.DataFrame(), candidates, research_rows, account
    )

    assert plan["lines"]["value"]["target_weights"] == {"600001": 0.3}
    assert plan["lines"]["short"]["target_weights"] == {"600001": 0.2}
    assert target == {"600001": 0.3}


def test_strategy_line_allocations_are_auditable(tmp_path: Path) -> None:
    system = _system(tmp_path, {"enabled": True})
    plan = {
        "enabled": True,
        "mode": "parallel_lines",
        "merged_target_weights": {"600001": 0.3},
        "lines": {
            "value": {
                "capital_weight": 0.65,
                "selected_tickers": ["600001"],
                "target_weights": {"600001": 0.3},
            }
        },
    }
    candidates = pd.DataFrame([{"股票代码": "600001", "名称": "核心一"}])

    df = system._build_strategy_line_allocations(plan, pd.DataFrame(), candidates)

    assert df.to_dict("records") == [
        {
            "line_id": "value",
            "ticker": "600001",
            "name": "核心一",
            "line_target_weight": 0.3,
            "merged_target_weight": 0.3,
            "selected_by_line": True,
            "line_budget": 0.65,
        }
    ]
