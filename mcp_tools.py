#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 3 - MCP Tools 核心函数模块
--------------------------------
这个文件定义 3 个可被 MCP Server 暴露的工具函数：
1) get_financial_health_check(ticker)
2) search_market_sentiment(ticker)
3) calculate_ah_premium(ticker)

设计原则：
- 优先可运行：先保证本地可直接调用
- 兼容 AkShare 接口变化：增加候选接口与字段兜底
- 返回结构化 dict：方便 LLM/前端/日志系统消费
"""

import json
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd
import akshare as ak

from market_intelligence import MarketIntelligenceError, build_market_intelligence_report


# =============================
# 全局参数
# =============================
MAX_RETRY = 3
RETRY_SLEEP_SECONDS = 1.0


@dataclass
class ToolResult:
    """统一工具返回包装，便于转 JSON。"""

    ok: bool
    tool: str
    ticker: str
    data: Dict[str, Any]
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ok": self.ok,
            "tool": self.tool,
            "ticker": self.ticker,
            "message": self.message,
            "data": self.data,
        }


# =============================
# 通用工具函数
# =============================
def safe_to_float(value) -> Optional[float]:
    """把值安全转成 float；失败返回 None。"""
    try:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if text in {"", "-", "--", "None", "nan", "NaN"}:
                return None
            text = text.replace(",", "").replace("%", "")
            return float(text)
        return float(value)
    except Exception:
        return None


def normalize_ticker(ticker: str) -> str:
    """标准化 A 股代码为 6 位数字字符串。"""
    digits = "".join(ch for ch in str(ticker) if ch.isdigit())
    return digits.zfill(6)[-6:]


def find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """在候选列里返回第一个存在的列名。"""
    for col in candidates:
        if col in df.columns:
            return col
    return None


def call_ak_with_retry(func_name: str, **kwargs) -> Optional[pd.DataFrame]:
    """按函数名动态调用 akshare，带重试。"""
    if not hasattr(ak, func_name):
        return None
    api_func = getattr(ak, func_name)

    for i in range(1, MAX_RETRY + 1):
        try:
            df = api_func(**kwargs)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df
            return None
        except Exception:
            if i < MAX_RETRY:
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                return None
    return None


def pct_growth(old_value: Optional[float], new_value: Optional[float]) -> Optional[float]:
    """计算同比增速百分比。"""
    if old_value is None or new_value is None:
        return None
    if old_value == 0:
        return None
    return (new_value - old_value) / abs(old_value) * 100.0


def _extract_series_last_n(
    df: pd.DataFrame,
    date_candidates: List[str],
    value_candidates: List[str],
    n: int = 3,
) -> List[Tuple[str, Optional[float]]]:
    """
    从财务表中提取最新 n 期的 (报告期, 数值)。
    """
    if df is None or df.empty:
        return []

    data = df.copy()
    date_col = find_first_existing_column(data, date_candidates)
    value_col = find_first_existing_column(data, value_candidates)
    if date_col is None or value_col is None:
        return []

    data["__date"] = pd.to_datetime(data[date_col], errors="coerce")
    data = data.sort_values(by="__date", ascending=True)

    pairs = []
    for _, row in data.tail(n).iterrows():
        period = str(row[date_col])
        value = safe_to_float(row[value_col])
        pairs.append((period, value))
    return pairs


# =============================
# Tool 1: 财务健康检查
# =============================
def get_financial_health_check(ticker: str) -> Dict[str, Any]:
    """
    获取过去3期（通常是年报/近3年）营收、净利润、经营现金流，
    并计算波动情况与“是否大起大落”的简化判断。
    """
    symbol = normalize_ticker(ticker)

    # 1) 拉利润表（收入、净利润）
    profit_df = call_ak_with_retry("stock_profit_sheet_by_report_em", symbol=symbol)
    if profit_df is None:
        profit_df = call_ak_with_retry("stock_profit_sheet_by_yearly_em", symbol=symbol)

    # 2) 拉现金流量表（经营现金流净额）
    cash_df = call_ak_with_retry("stock_cash_flow_sheet_by_report_em", symbol=symbol)
    if cash_df is None:
        cash_df = call_ak_with_retry("stock_cash_flow_sheet_by_yearly_em", symbol=symbol)

    if profit_df is None or cash_df is None:
        return ToolResult(
            ok=False,
            tool="get_financial_health_check",
            ticker=symbol,
            message="利润表或现金流量表获取失败（可能是接口变化或网络问题）",
            data={},
        ).to_dict()

    # 3) 提取最近3期关键数据
    revenue_series = _extract_series_last_n(
        profit_df,
        date_candidates=["REPORT_DATE", "报告日期", "报告期", "日期"],
        value_candidates=["营业总收入", "营业收入", "TOTAL_OPERATE_INCOME"],
        n=3,
    )
    net_profit_series = _extract_series_last_n(
        profit_df,
        date_candidates=["REPORT_DATE", "报告日期", "报告期", "日期"],
        value_candidates=["净利润", "净利润（含少数股东损益）", "NETPROFIT"],
        n=3,
    )
    ocf_series = _extract_series_last_n(
        cash_df,
        date_candidates=["REPORT_DATE", "报告日期", "报告期", "日期"],
        value_candidates=["经营活动产生的现金流量净额", "经营活动现金流量净额", "NETCASH_OPERATE"],
        n=3,
    )

    if len(revenue_series) < 2 or len(net_profit_series) < 2 or len(ocf_series) < 2:
        return ToolResult(
            ok=False,
            tool="get_financial_health_check",
            ticker=symbol,
            message="关键字段不足，无法完成近3期稳定性分析",
            data={
                "revenue_series": revenue_series,
                "net_profit_series": net_profit_series,
                "ocf_series": ocf_series,
            },
        ).to_dict()

    # 4) 计算首尾增长
    rev_growth = pct_growth(revenue_series[0][1], revenue_series[-1][1])
    profit_growth = pct_growth(net_profit_series[0][1], net_profit_series[-1][1])
    ocf_growth = pct_growth(ocf_series[0][1], ocf_series[-1][1])

    # 5) 计算“波动度”：max/min（绝对值规避负值干扰）
    def volatility_ratio(series: List[Tuple[str, Optional[float]]]) -> Optional[float]:
        values = [abs(v) for _, v in series if v is not None]
        if len(values) < 2:
            return None
        min_v = min(values)
        max_v = max(values)
        if min_v == 0:
            return None
        return max_v / min_v

    rev_vol = volatility_ratio(revenue_series)
    profit_vol = volatility_ratio(net_profit_series)
    ocf_vol = volatility_ratio(ocf_series)

    # 6) 简化风险判定规则（防御型）
    # 若净利润或现金流波动过大（>3倍）或首尾增长<-30%，视为“大起大落”
    big_fluctuation = False
    reasons = []

    if profit_vol is not None and profit_vol > 3:
        big_fluctuation = True
        reasons.append("净利润波动超过3倍")
    if ocf_vol is not None and ocf_vol > 3:
        big_fluctuation = True
        reasons.append("经营现金流波动超过3倍")
    if profit_growth is not None and profit_growth < -30:
        big_fluctuation = True
        reasons.append("净利润3期首尾下滑超过30%")
    if ocf_growth is not None and ocf_growth < -30:
        big_fluctuation = True
        reasons.append("经营现金流3期首尾下滑超过30%")

    return ToolResult(
        ok=True,
        tool="get_financial_health_check",
        ticker=symbol,
        data={
            "revenue_series": revenue_series,
            "net_profit_series": net_profit_series,
            "ocf_series": ocf_series,
            "revenue_growth_pct": rev_growth,
            "net_profit_growth_pct": profit_growth,
            "ocf_growth_pct": ocf_growth,
            "revenue_volatility_ratio": rev_vol,
            "net_profit_volatility_ratio": profit_vol,
            "ocf_volatility_ratio": ocf_vol,
            "big_fluctuation": big_fluctuation,
            "risk_flags": reasons,
        },
        message="success",
    ).to_dict()


# =============================
# Tool 2: 市场情绪/负面信息
# =============================
@lru_cache(maxsize=1)
def _load_a_share_name_mapping() -> Dict[str, str]:
    """按需加载 A 股代码到名称的映射，提升新闻检索召回率。"""
    spot_df = call_ak_with_retry("stock_zh_a_spot_em")
    if spot_df is None:
        return {}

    code_col = find_first_existing_column(spot_df, ["代码", "symbol", "股票代码"])
    name_col = find_first_existing_column(spot_df, ["名称", "name", "股票名称"])
    if code_col is None or name_col is None:
        return {}

    data = spot_df.copy()
    data["__code"] = data[code_col].astype(str).str.zfill(6)
    mapping: Dict[str, str] = {}
    for _, row in data.iterrows():
        code = str(row.get("__code", "")).strip()
        name = str(row.get(name_col, "")).strip()
        if code and name and name != "nan":
            mapping[code] = name
    return mapping


def _lookup_a_share_name(ticker: str) -> Optional[str]:
    return _load_a_share_name_mapping().get(ticker)


def search_market_sentiment(ticker: str) -> Dict[str, Any]:
    """检索公开新闻 RSS，输出标准化负面事件与风险摘要。"""
    symbol = normalize_ticker(ticker)
    company_name = _lookup_a_share_name(symbol)

    try:
        intelligence = build_market_intelligence_report(symbol, company_name=company_name)
    except MarketIntelligenceError as exc:
        return ToolResult(
            ok=False,
            tool="search_market_sentiment",
            ticker=symbol,
            data={"lookback_months": 3, "negative_events": []},
            message=str(exc),
        ).to_dict()
    except Exception as exc:
        return ToolResult(
            ok=False,
            tool="search_market_sentiment",
            ticker=symbol,
            data={"lookback_months": 3, "negative_events": []},
            message=f"舆情检索异常：{exc}",
        ).to_dict()

    return ToolResult(
        ok=True,
        tool="search_market_sentiment",
        ticker=symbol,
        data=intelligence,
        message="success",
    ).to_dict()


# =============================
# Tool 3: A/H 溢价率
# =============================
def _guess_hk_ticker_for_ah(a_ticker: str) -> Optional[str]:
    """
    简化映射：示例级别，不保证覆盖全部 A+H 股票。

    如果你后续希望精确计算，建议维护一份 A-H 对照表 CSV。
    """
    mapping = {
        "600036": "3968",   # 招商银行
        "601398": "1398",   # 工商银行
        "601939": "0939",   # 建设银行
        "601988": "3988",   # 中国银行
        "601857": "0857",   # 中国石油
        "601088": "1088",   # 中国神华
        "601328": "3328",   # 交通银行
    }
    return mapping.get(a_ticker)


def _fetch_a_price(a_ticker: str) -> Optional[float]:
    """获取 A 股现价。"""
    spot_df = call_ak_with_retry("stock_zh_a_spot_em")
    if spot_df is None:
        return None

    code_col = find_first_existing_column(spot_df, ["代码", "symbol", "股票代码"])
    price_col = find_first_existing_column(spot_df, ["最新价", "现价", "close"])
    if code_col is None or price_col is None:
        return None

    data = spot_df.copy()
    data["__code"] = data[code_col].astype(str).str.zfill(6)
    matched = data[data["__code"] == a_ticker]
    if matched.empty:
        return None
    return safe_to_float(matched.iloc[0][price_col])


def _fetch_h_price(hk_ticker: str) -> Optional[float]:
    """
    获取港股现价（akshare 港股实时行情）。
    不同版本字段可能不同，做宽松兼容。
    """
    hk_df = call_ak_with_retry("stock_hk_spot_em")
    if hk_df is None:
        return None

    code_col = find_first_existing_column(hk_df, ["代码", "symbol", "股票代码"])
    price_col = find_first_existing_column(hk_df, ["最新价", "现价", "close"])
    if code_col is None or price_col is None:
        return None

    data = hk_df.copy()
    data["__code"] = data[code_col].astype(str).str.zfill(4)
    target = str(hk_ticker).zfill(4)
    matched = data[data["__code"] == target]
    if matched.empty:
        return None
    return safe_to_float(matched.iloc[0][price_col])


def calculate_ah_premium(ticker: str) -> Dict[str, Any]:
    """
    计算 A/H 溢价率：
    premium = (A股价格 / H股价格 - 1) * 100%

    说明：
    - 这里用“价格比”做简化示范，未做汇率换算与股本差异校正。
    - 实务中建议引入汇率和可比口径。
    """
    a_ticker = normalize_ticker(ticker)
    hk_ticker = _guess_hk_ticker_for_ah(a_ticker)

    if hk_ticker is None:
        return ToolResult(
            ok=False,
            tool="calculate_ah_premium",
            ticker=a_ticker,
            message="未命中 A+H 映射（可在代码 mapping 中补充）",
            data={},
        ).to_dict()

    a_price = _fetch_a_price(a_ticker)
    h_price = _fetch_h_price(hk_ticker)

    if a_price is None or h_price is None or h_price <= 0:
        return ToolResult(
            ok=False,
            tool="calculate_ah_premium",
            ticker=a_ticker,
            message="A 股或 H 股价格获取失败",
            data={"a_price": a_price, "h_price": h_price, "hk_ticker": hk_ticker},
        ).to_dict()

    premium = (a_price / h_price - 1.0) * 100.0

    return ToolResult(
        ok=True,
        tool="calculate_ah_premium",
        ticker=a_ticker,
        message="success",
        data={
            "a_ticker": a_ticker,
            "hk_ticker": hk_ticker,
            "a_price": a_price,
            "h_price": h_price,
            "ah_premium_pct": premium,
            "note": "简化口径：未做汇率与股本口径调整，仅用于初筛风险提示",
        },
    ).to_dict()


def pretty_json(data: Dict[str, Any]) -> str:
    """将字典格式化成中文可读 JSON 字符串。"""
    return json.dumps(data, ensure_ascii=False, indent=2)
