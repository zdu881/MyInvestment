from __future__ import annotations

from pathlib import Path

import pandas as pd

import step1_screener as step1


def test_step1_main_returns_nonzero_when_all_data_sources_fail(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "candidates.csv").write_text("stale\n", encoding="utf-8")

    monkeypatch.setattr(step1, "run_sina_baidu_pipeline", lambda: (_ for _ in ()).throw(RuntimeError("sina_baidu down")))
    monkeypatch.setattr(step1, "run_akshare_pipeline", lambda: (_ for _ in ()).throw(RuntimeError("akshare down")))
    monkeypatch.setattr(step1, "run_lixinger_pipeline", lambda: (_ for _ in ()).throw(RuntimeError("lixinger down")))
    monkeypatch.setattr(step1, "run_baostock_fallback_pipeline", lambda: (_ for _ in ()).throw(RuntimeError("baostock down")))

    assert step1.main() == 1
    assert (tmp_path / "candidates.csv").read_text(encoding="utf-8") == "stale\n"


def test_step1_main_writes_candidates_atomically_on_success(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        step1,
        "run_sina_baidu_pipeline",
        lambda: pd.DataFrame(
            [
                {
                    "ticker": "600000",
                    "name": "浦发银行",
                    "current_price": 10.0,
                    "pe_ttm": 5.0,
                    "pb": 0.5,
                    "dividend_yield": 5.2,
                    "lot_cost": 1000.0,
                    "data_source": "unit",
                    "pass_count_final": 5,
                    "rule_market_cap": True,
                    "rule_pe": True,
                    "rule_pb": True,
                    "rule_lot_cost": True,
                    "rule_dividend": True,
                }
            ]
        ),
    )

    assert step1.main() == 0
    output = pd.read_csv(tmp_path / "candidates.csv", encoding="utf-8-sig")
    assert output.loc[0, "股票代码"] == 600000
    assert output.loc[0, "数据源"] == "unit"


def test_sina_baidu_pipeline_builds_candidates_from_working_sources(monkeypatch) -> None:
    monkeypatch.setattr(step1, "SINA_BAIDU_MAX_VALUATION_REQUESTS", 3)
    monkeypatch.setattr(
        step1.ak,
        "stock_zh_a_spot",
        lambda: pd.DataFrame(
            [
                {"代码": "sh600001", "名称": "高息一", "最新价": 10.0},
                {"代码": "sz000002", "名称": "高息二", "最新价": 20.0},
                {"代码": "bj920000", "名称": "北交所", "最新价": 12.0},
            ]
        ),
    )
    monkeypatch.setattr(
        step1.ak,
        "stock_fhps_em",
        lambda date: pd.DataFrame(
            [
                {
                    "代码": "600001",
                    "现金分红-股息率": 0.061,
                    "最新公告日期": "2026-03-01",
                },
                {
                    "代码": "000002",
                    "现金分红-股息率": 0.052,
                    "最新公告日期": "2026-03-02",
                },
            ]
        ),
    )

    def _stock_value_em(symbol: str) -> pd.DataFrame:
        rows = {
            "600001": {"总市值": 30_000_000_000, "PE(TTM)": 8.0, "市净率": 0.8},
            "000002": {"总市值": 50_000_000_000, "PE(TTM)": 9.0, "市净率": 0.9},
        }
        return pd.DataFrame([{**rows[symbol], "数据日期": "2026-06-22"}])

    monkeypatch.setattr(step1.ak, "stock_value_em", _stock_value_em)

    output = step1.run_sina_baidu_pipeline()

    assert list(output["ticker"]) == ["600001", "000002"]
    assert list(output["data_source"].unique()) == ["sina_baidu"]
    assert output.loc[0, "dividend_yield"] == 6.1
    assert output.loc[0, "pass_count_final"] == 5
