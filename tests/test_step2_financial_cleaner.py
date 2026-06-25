from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import pytest

import step2_financial_cleaner as step2


def _write_candidates(path: Path, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def test_akshare_call_timeout_returns_quickly(monkeypatch) -> None:
    if not step2._alarm_timeout_supported():
        pytest.skip("signal alarm timeout is unavailable in this runtime")

    def slow_api(symbol: str):
        time.sleep(2)
        return pd.DataFrame([{"symbol": symbol}])

    monkeypatch.setattr(step2.ak, "slow_timeout_api", slow_api, raising=False)
    monkeypatch.setattr(step2, "MAX_RETRY", 1)
    monkeypatch.setattr(step2, "AK_CALL_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(step2, "RETRY_SLEEP_SECONDS", 0)

    started = time.monotonic()
    result = step2._call_ak_function_with_retry("slow_timeout_api", "600000")
    elapsed = time.monotonic() - started

    assert result is None
    assert elapsed < 0.5


def test_eastmoney_symbol_prefix() -> None:
    assert step2.to_eastmoney_symbol("600741") == "SH600741"
    assert step2.to_eastmoney_symbol("601668") == "SH601668"
    assert step2.to_eastmoney_symbol("000719") == "SZ000719"
    assert step2.to_eastmoney_symbol("300750") == "SZ300750"


def test_calculate_ratio_uses_eastmoney_cashflow_single_table(monkeypatch) -> None:
    calls: list[str] = []

    def fake_cashflow(symbol: str):
        calls.append(symbol)
        return pd.DataFrame(
            [
                {
                    "REPORT_DATE": "2026-03-31",
                    "NETCASH_OPERATE": 120.0,
                    "NETPROFIT": 100.0,
                }
            ]
        )

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("fallback should not be called")

    monkeypatch.setattr(step2.ak, "stock_cash_flow_sheet_by_report_em", fake_cashflow)
    monkeypatch.setattr(step2, "fetch_cashflow_and_profit", fail_if_called)
    monkeypatch.setattr(step2, "fetch_by_baostock", fail_if_called)
    monkeypatch.setattr(step2, "AK_CALL_TIMEOUT_SECONDS", 2)
    monkeypatch.setattr(step2, "run_with_timeout", lambda func, _timeout, _label: func())

    result = step2.calculate_ocf_net_income_ratio("600741")

    assert calls == ["SH600741"]
    assert result["status"] == "ok"
    assert result["source"] == "eastmoney_cashflow"
    assert result["ocf_net_income_ratio"] == 1.2
    assert result["profit_api"] == ""


def test_eastmoney_cashflow_prefers_latest_annual_report(monkeypatch) -> None:
    def fake_cashflow(symbol: str):
        assert symbol == "SH600741"
        return pd.DataFrame(
            [
                {
                    "REPORT_DATE": "2026-03-31",
                    "REPORT_TYPE": "一季报",
                    "NETCASH_OPERATE": -20.0,
                    "NETPROFIT": 100.0,
                },
                {
                    "REPORT_DATE": "2025-12-31",
                    "REPORT_TYPE": "年报",
                    "NETCASH_OPERATE": 150.0,
                    "NETPROFIT": 100.0,
                },
            ]
        )

    monkeypatch.setattr(step2.ak, "stock_cash_flow_sheet_by_report_em", fake_cashflow)
    monkeypatch.setattr(step2, "run_with_timeout", lambda func, _timeout, _label: func())

    ocf, net_income, report_period, source = step2.fetch_by_eastmoney_cashflow("600741")

    assert ocf == 150.0
    assert net_income == 100.0
    assert report_period == "2025-12-31"
    assert source == "eastmoney_cashflow"


def test_main_removes_stale_output_when_all_financial_fetches_fail(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(step2, "PER_TICKER_SLEEP_SECONDS", 0)
    monkeypatch.setattr(step2, "load_industry_map_from_baostock", lambda: {})
    monkeypatch.setattr(
        step2,
        "calculate_ocf_net_income_ratio",
        lambda ticker: {
            "ticker": ticker,
            "report_period": None,
            "ocf": None,
            "net_income": None,
            "ocf_net_income_ratio": None,
            "cashflow_api": "",
            "profit_api": "",
            "status": "error",
            "source": "timeout",
            "message": "unit-test timeout",
        },
    )
    _write_candidates(
        tmp_path / "candidates.csv",
        [
            {
                "股票代码": "600000",
                "名称": "浦发银行",
                "现价": 10.0,
                "PE(TTM)": 5.0,
                "PB": 0.5,
                "股息率(%)": 4.0,
                "一手成本": 1000.0,
            }
        ],
    )
    stale_output = tmp_path / "candidates_step2.csv"
    stale_output.write_text("stale should be removed\n", encoding="utf-8")

    assert step2.main() == 1
    assert not stale_output.exists()


def test_main_per_ticker_timeout_interrupts_slow_metric_function(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(step2, "PER_TICKER_SLEEP_SECONDS", 0)
    monkeypatch.setattr(step2, "PER_TICKER_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(step2, "load_industry_map_from_baostock", lambda: {})

    def slow_metric(_ticker: str):
        time.sleep(2)
        return {
            "ticker": "600000",
            "report_period": "2024-12-31",
            "ocf": 100.0,
            "net_income": 50.0,
            "ocf_net_income_ratio": 2.0,
            "cashflow_api": "unit",
            "profit_api": "unit",
            "status": "ok",
            "source": "unit",
            "message": "should not return",
        }

    monkeypatch.setattr(step2, "calculate_ocf_net_income_ratio", slow_metric)
    _write_candidates(
        tmp_path / "candidates.csv",
        [
            {
                "股票代码": "600000",
                "名称": "浦发银行",
                "现价": 10.0,
                "PE(TTM)": 5.0,
                "PB": 0.5,
                "股息率(%)": 4.0,
                "一手成本": 1000.0,
            }
        ],
    )

    started = time.monotonic()
    assert step2.main() == 1
    elapsed = time.monotonic() - started

    assert elapsed < 0.7
    assert not (tmp_path / "candidates_step2.csv").exists()


def test_main_success_keeps_industry_and_threshold_columns(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(step2, "PER_TICKER_SLEEP_SECONDS", 0)
    monkeypatch.setattr(step2, "load_industry_map_from_baostock", lambda: {"600000": "银行"})
    monkeypatch.setattr(
        step2,
        "calculate_ocf_net_income_ratio",
        lambda ticker: {
            "ticker": ticker,
            "report_period": "2024-12-31",
            "ocf": 80.0,
            "net_income": 100.0,
            "ocf_net_income_ratio": 0.8,
            "cashflow_api": "unit",
            "profit_api": "unit",
            "status": "ok",
            "source": "unit",
            "message": "success",
        },
    )
    _write_candidates(
        tmp_path / "candidates.csv",
        [
            {
                "股票代码": "600000",
                "名称": "浦发银行",
                "现价": 10.0,
                "PE(TTM)": 5.0,
                "PB": 0.5,
                "股息率(%)": 4.0,
                "一手成本": 1000.0,
            }
        ],
    )

    assert step2.main() == 0

    output = pd.read_csv(tmp_path / "candidates_step2.csv", encoding="utf-8-sig")
    assert output.loc[0, "股票代码"] == 600000
    assert output.loc[0, "行业"] == "银行"
    assert output.loc[0, "OCF阈值"] == 0.5
    assert output.loc[0, "OCF/净利润"] == 0.8


def test_main_uses_name_industry_fallback_when_industry_map_empty(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(step2, "PER_TICKER_SLEEP_SECONDS", 0)
    monkeypatch.setattr(step2, "load_industry_map_from_baostock", lambda: {})
    monkeypatch.setattr(
        step2,
        "calculate_ocf_net_income_ratio",
        lambda ticker: {
            "ticker": ticker,
            "report_period": "2025-12-31",
            "ocf": 40.0,
            "net_income": 100.0,
            "ocf_net_income_ratio": 0.4,
            "cashflow_api": "unit",
            "profit_api": "",
            "status": "ok",
            "source": "unit",
            "message": "success",
        },
    )
    _write_candidates(
        tmp_path / "candidates.csv",
        [
            {
                "股票代码": "601668",
                "名称": "中国建筑",
                "现价": 4.45,
                "PE(TTM)": 4.88,
                "PB": 0.37,
                "股息率(%)": 6.07,
                "一手成本": 445.0,
            }
        ],
    )

    assert step2.main() == 0

    output = pd.read_csv(tmp_path / "candidates_step2.csv", encoding="utf-8-sig")
    assert len(output) == 1
    assert output.loc[0, "名称"] == "中国建筑"
    assert output.loc[0, "行业"] == "建筑工程"
    assert output.loc[0, "OCF阈值"] == 0.3
    assert output.loc[0, "OCF/净利润"] == 0.4


def test_main_preserves_leading_zero_tickers(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(step2, "PER_TICKER_SLEEP_SECONDS", 0)
    monkeypatch.setattr(step2, "load_industry_map_from_baostock", lambda: {})
    monkeypatch.setattr(
        step2,
        "calculate_ocf_net_income_ratio",
        lambda ticker: {
            "ticker": ticker,
            "report_period": "2025-12-31",
            "ocf": 150.0,
            "net_income": 100.0,
            "ocf_net_income_ratio": 1.5,
            "cashflow_api": "unit",
            "profit_api": "",
            "status": "ok",
            "source": "unit",
            "message": "success",
        },
    )
    _write_candidates(
        tmp_path / "candidates.csv",
        [
            {
                "股票代码": "000719",
                "名称": "中原传媒",
                "现价": 11.57,
                "PE(TTM)": 8.64,
                "PB": 0.96,
                "股息率(%)": 5.67,
                "一手成本": 1157.0,
            }
        ],
    )

    assert step2.main() == 0

    raw = (tmp_path / "candidates_step2.csv").read_text(encoding="utf-8-sig")
    assert "\n000719," in raw
