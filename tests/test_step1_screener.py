from __future__ import annotations

from pathlib import Path

import pandas as pd

import step1_screener as step1


def test_step1_main_returns_nonzero_when_all_data_sources_fail(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "candidates.csv").write_text("stale\n", encoding="utf-8")

    monkeypatch.setattr(step1, "run_akshare_pipeline", lambda: (_ for _ in ()).throw(RuntimeError("akshare down")))
    monkeypatch.setattr(step1, "run_lixinger_pipeline", lambda: (_ for _ in ()).throw(RuntimeError("lixinger down")))
    monkeypatch.setattr(step1, "run_baostock_fallback_pipeline", lambda: (_ for _ in ()).throw(RuntimeError("baostock down")))

    assert step1.main() == 1
    assert (tmp_path / "candidates.csv").read_text(encoding="utf-8") == "stale\n"


def test_step1_main_writes_candidates_atomically_on_success(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        step1,
        "run_akshare_pipeline",
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
