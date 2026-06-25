from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

import market_intelligence
import mcp_tools
from step4_generate_report import summarize_tool_outputs


RSS_SAMPLE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Google News</title>
    <item>
      <title>贵州茅台收到监管问询函 - 证券时报</title>
      <link>https://example.com/regulatory-1</link>
      <pubDate>Mon, 03 Mar 2026 08:00:00 GMT</pubDate>
      <source url="https://www.stcn.com">证券时报</source>
      <description>公司披露收到监管问询。</description>
    </item>
    <item>
      <title>贵州茅台重要股东拟减持股份 - 上海证券报</title>
      <link>https://example.com/governance-1</link>
      <pubDate>Sun, 02 Mar 2026 08:00:00 GMT</pubDate>
      <source url="https://www.cnstock.com">上海证券报</source>
      <description>公告显示股东拟减持。</description>
    </item>
    <item>
      <title>贵州茅台新品发布会落地 - 每日经济新闻</title>
      <link>https://example.com/neutral-1</link>
      <pubDate>Sat, 01 Mar 2026 08:00:00 GMT</pubDate>
      <source url="https://www.nbd.com.cn">每日经济新闻</source>
      <description>新品发布带动市场关注。</description>
    </item>
  </channel>
</rss>
"""


def test_build_market_intelligence_report_deduplicates_and_scores(monkeypatch) -> None:
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            fixed = datetime(2026, 3, 10, 12, 0, tzinfo=timezone.utc)
            return fixed if tz is None else fixed.astimezone(tz)

    monkeypatch.setattr(market_intelligence, "fetch_google_news_rss", lambda *args, **kwargs: RSS_SAMPLE)
    monkeypatch.setattr(
        market_intelligence,
        "fetch_eastmoney_stock_news",
        lambda *args, **kwargs: (_ for _ in ()).throw(market_intelligence.MarketIntelligenceError("eastmoney unavailable")),
    )
    monkeypatch.setattr(market_intelligence, "datetime", FixedDatetime)

    report = market_intelligence.build_market_intelligence_report(
        "600519",
        company_name="贵州茅台",
        lookback_days=120,
    )

    events = report["negative_events"]
    assert len(events) == 2
    assert {event["risk_category"] for event in events} == {"regulatory", "governance"}
    assert report["source_count"] == 2
    assert report["risk_score"] >= 30
    assert "监管" in report["conclusion"]
    assert all(event["headline"] for event in events)


def test_build_market_intelligence_report_uses_eastmoney_before_google(monkeypatch) -> None:
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            fixed = datetime(2026, 6, 25, 12, 0, tzinfo=timezone.utc)
            return fixed if tz is None else fixed.astimezone(tz)

    google_calls: list[str] = []

    def google_timeout(query, *args, **kwargs):
        google_calls.append(str(query))
        raise AssertionError("google should not be called when eastmoney succeeds")

    def eastmoney_news(*args, **kwargs):
        return [
            {
                "published_at": "2026-06-24T09:00:00+00:00",
                "title": "中国建筑下属子公司拟参与新加坡房地产开发项目",
                "description": "公司公告披露项目投资计划。",
                "link": "https://example.com/neutral",
                "source": "财联社",
                "query": "601668 中国建筑",
            }
        ]

    monkeypatch.setattr(market_intelligence, "fetch_google_news_rss", google_timeout)
    monkeypatch.setattr(market_intelligence, "fetch_eastmoney_stock_news", eastmoney_news)
    monkeypatch.setattr(market_intelligence, "datetime", FixedDatetime)

    report = market_intelligence.build_market_intelligence_report(
        "601668",
        company_name="中国建筑",
        lookback_days=120,
    )

    assert report["successful_sources"] == ["eastmoney_stock_news"]
    assert report["risk_score"] == 0.0
    assert report["negative_events"] == []
    assert "未检索到明确负面舆情" in report["conclusion"]
    assert "partial_errors" not in report
    assert len(google_calls) == 0


def test_search_market_sentiment_returns_failure_when_sources_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(mcp_tools, "_lookup_a_share_name", lambda ticker: "贵州茅台")

    def _raise_error(*args, **kwargs):
        raise mcp_tools.MarketIntelligenceError("公开新闻源检索失败")

    monkeypatch.setattr(mcp_tools, "build_market_intelligence_report", _raise_error)

    result = mcp_tools.search_market_sentiment("600519")

    assert result["ok"] is False
    assert result["ticker"] == "600519"
    assert result["data"]["negative_events"] == []
    assert "公开新闻源检索失败" in result["message"]


def test_financial_health_check_uses_eastmoney_prefixed_symbol(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_call(func_name: str, **kwargs):
        symbol = str(kwargs.get("symbol", ""))
        calls.append((func_name, symbol))
        assert symbol == "SH600741"
        if "profit" in func_name:
            return pd.DataFrame(
                [
                    {"REPORT_DATE": "2023-12-31", "TOTAL_OPERATE_INCOME": 100.0, "NETPROFIT": 10.0},
                    {"REPORT_DATE": "2024-12-31", "TOTAL_OPERATE_INCOME": 110.0, "NETPROFIT": 12.0},
                    {"REPORT_DATE": "2025-12-31", "TOTAL_OPERATE_INCOME": 120.0, "NETPROFIT": 14.0},
                ]
            )
        return pd.DataFrame(
            [
                {"REPORT_DATE": "2023-12-31", "NETCASH_OPERATE": 11.0},
                {"REPORT_DATE": "2024-12-31", "NETCASH_OPERATE": 13.0},
                {"REPORT_DATE": "2025-12-31", "NETCASH_OPERATE": 15.0},
            ]
        )

    monkeypatch.setattr(mcp_tools, "call_ak_with_retry", fake_call)

    result = mcp_tools.get_financial_health_check("600741")

    assert result["ok"] is True
    assert result["ticker"] == "600741"
    assert calls == [
        ("stock_profit_sheet_by_report_em", "SH600741"),
        ("stock_cash_flow_sheet_by_report_em", "SH600741"),
    ]


def test_ah_premium_not_applicable_is_successful_neutral_result() -> None:
    result = mcp_tools.calculate_ah_premium("600741")

    assert result["ok"] is True
    assert result["ticker"] == "600741"
    assert result["data"]["applicable"] is False
    assert "not_applicable" in result["message"]


def test_summarize_tool_outputs_uses_sentiment_risk_score() -> None:
    summary = summarize_tool_outputs(
        health={
            "ok": True,
            "data": {"big_fluctuation": False, "ocf_growth_pct": 12.5, "risk_flags": []},
        },
        sentiment={
            "ok": True,
            "data": {
                "risk_score": 72.5,
                "categories": ["regulatory"],
                "negative_events": [
                    {
                        "headline": "贵州茅台收到监管问询函",
                        "risk_category": "regulatory",
                        "severity": "high",
                    }
                ],
            },
        },
        ah={"ok": False, "data": {}},
    )

    assert any("72.5/100" in flag for flag in summary["risk_flags"])
    assert any("监管类负面舆情" in flag for flag in summary["risk_flags"])
    assert summary["verdict"] == "观察"
