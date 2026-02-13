#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 4: 整合与报告生成
----------------------
本脚本将 Step1 + Step2 + Step3 串联：
1) 读取 candidates_step2.csv（优先）或 candidates.csv（回退）
2) 选取 Top 5
3) 调用 Step3 的三个工具做风险排查
4) 生成 Markdown 投资可行性报告
5) 额外输出一个 LLM Prompt 模板文件，便于后续接入真正的大模型
"""

import os
from datetime import datetime
from typing import Dict, Any, List

import pandas as pd

from mcp_tools import (
    get_financial_health_check,
    search_market_sentiment,
    calculate_ah_premium,
)

INPUT_STEP2 = "candidates_step2.csv"
INPUT_STEP1 = "candidates.csv"
REPORT_FILE = "investment_feasibility_report.md"
PROMPT_FILE = "step4_llm_prompt_template.md"
TOP_N = 5


def pick_input_dataframe() -> pd.DataFrame:
    """优先使用 Step2；若 Step2 为空则回退到 Step1。"""
    if os.path.exists(INPUT_STEP2):
        step2_df = pd.read_csv(INPUT_STEP2)
        if not step2_df.empty:
            step2_df["__source_level"] = "step2"
            return step2_df

    if os.path.exists(INPUT_STEP1):
        step1_df = pd.read_csv(INPUT_STEP1)
        if not step1_df.empty:
            step1_df["__source_level"] = "step1"
            return step1_df

    raise FileNotFoundError("未找到可用候选数据，请先运行 step1_screener.py / step2_financial_cleaner.py")


def find_ticker_col(df: pd.DataFrame) -> str:
    for col in ["股票代码", "ticker", "代码"]:
        if col in df.columns:
            return col
    raise KeyError(f"未找到股票代码列，当前列：{list(df.columns)}")


def normalize_ticker(v: Any) -> str:
    digits = "".join(ch for ch in str(v) if ch.isdigit())
    return digits.zfill(6)[-6:]


def score_row(row: pd.Series) -> float:
    """
    简化排序分数：高股息 + 低PE + 低PB。
    分数越高越靠前。
    """
    dy = float(row.get("股息率(%)", 0) or 0)
    pe = float(row.get("PE(TTM)", 99) or 99)
    pb = float(row.get("PB", 99) or 99)
    return dy * 2.0 + max(0.0, 12 - pe) + max(0.0, 1.2 - pb) * 10


def build_llm_prompt_template(top_df: pd.DataFrame) -> str:
    """生成给 LLM 使用的 Prompt 模板。"""
    stock_lines = []
    for _, r in top_df.iterrows():
        stock_lines.append(
            f"- {r.get('股票代码')} {r.get('名称','')} | 现价={r.get('现价','N/A')} | PE={r.get('PE(TTM)','N/A')} | PB={r.get('PB','N/A')} | 股息率={r.get('股息率(%)','N/A')}%"
        )

    return f"""# 角色
你是首席风险官（Chief Risk Officer），偏好极度保守的防御型价值投资。

# 任务
请对以下候选股票做排雷分析，并输出 Markdown 报告：
{chr(10).join(stock_lines)}

# 可调用工具
1. get_financial_health_check(ticker)
2. search_market_sentiment(ticker)
3. calculate_ah_premium(ticker)

# 输出要求
每只股票必须包含：
- 买入理由（不超过3条）
- 核心风险（不超过3条）
- 结论：通过 / 观察 / 放弃

最后给出组合建议：
- 建议保留股票数量（最多3只）
- 单只仓位上限（基于本金 10,000 RMB）
- 关键风控阈值（止损/估值反转/分红下降）
"""


def summarize_tool_outputs(health: Dict[str, Any], sentiment: Dict[str, Any], ah: Dict[str, Any]) -> Dict[str, Any]:
    """把三个工具输出汇总成简洁结论。"""
    risk_flags = []
    buy_reasons = []

    if health.get("ok"):
        h_data = health.get("data", {})
        if h_data.get("big_fluctuation"):
            risk_flags.extend(h_data.get("risk_flags", []))
        else:
            buy_reasons.append("近三期财务波动相对可控")

        ocf_growth = h_data.get("ocf_growth_pct")
        if isinstance(ocf_growth, (int, float)) and ocf_growth > 0:
            buy_reasons.append("经营现金流呈增长趋势")

    if sentiment.get("ok"):
        negs = sentiment.get("data", {}).get("negative_events", [])
        if len(negs) <= 1 and negs[0].get("type") == "placeholder":
            risk_flags.append("外部舆情源未接入，需人工复核公告")

    if ah.get("ok"):
        p = ah.get("data", {}).get("ah_premium_pct")
        if isinstance(p, (int, float)):
            if p > 30:
                risk_flags.append(f"A/H 溢价较高（{p:.2f}%）")
            else:
                buy_reasons.append(f"A/H 溢价压力可控（{p:.2f}%）")

    if not buy_reasons:
        buy_reasons.append("估值处于防御区间，符合低估值初筛")
    if not risk_flags:
        risk_flags.append("未发现硬性红旗，但仍需跟踪季报现金流")

    # 结论规则（简化）：风险>=2为观察，否则通过
    verdict = "观察" if len(risk_flags) >= 2 else "通过"
    return {
        "buy_reasons": buy_reasons[:3],
        "risk_flags": risk_flags[:3],
        "verdict": verdict,
    }


def main() -> None:
    df = pick_input_dataframe().copy()
    ticker_col = find_ticker_col(df)
    df["股票代码"] = df[ticker_col].apply(normalize_ticker)

    if "名称" not in df.columns:
        df["名称"] = "N/A"

    # 如果关键估值列缺失，补默认值避免排序报错
    for c, default in [("股息率(%)", 0), ("PE(TTM)", 99), ("PB", 99), ("现价", 0)]:
        if c not in df.columns:
            df[c] = default

    df["__score"] = df.apply(score_row, axis=1)
    top_df = df.sort_values(by="__score", ascending=False).head(TOP_N).reset_index(drop=True)

    report_lines: List[str] = []
    report_lines.append("# 投资可行性报告（DeepValue-AI-Agent）")
    report_lines.append("")
    report_lines.append(f"- 生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"- 数据来源层级：{top_df['__source_level'].iloc[0] if not top_df.empty else 'N/A'}")
    report_lines.append("- 策略：高股息 + 低估值 + 现金流质量优先（防御型）")
    report_lines.append("")
    report_lines.append("## 候选股票（Top 5）")
    report_lines.append("")

    for _, r in top_df.iterrows():
        report_lines.append(
            f"- {r['股票代码']} {r['名称']} | 现价={r.get('现价', 'N/A')} | PE={r.get('PE(TTM)', 'N/A')} | PB={r.get('PB', 'N/A')} | 股息率={r.get('股息率(%)', 'N/A')}%"
        )

    report_lines.append("")
    report_lines.append("## 个股排雷结果")
    report_lines.append("")

    passed_list = []

    for _, r in top_df.iterrows():
        ticker = r["股票代码"]
        name = r["名称"]

        health = get_financial_health_check(ticker)
        sentiment = search_market_sentiment(ticker)
        ah = calculate_ah_premium(ticker)
        summary = summarize_tool_outputs(health, sentiment, ah)

        if summary["verdict"] == "通过":
            passed_list.append((ticker, name))

        report_lines.append(f"### {ticker} {name}")
        report_lines.append("")
        report_lines.append("**买入理由**")
        for reason in summary["buy_reasons"]:
            report_lines.append(f"- {reason}")
        report_lines.append("")
        report_lines.append("**核心风险**")
        for risk in summary["risk_flags"]:
            report_lines.append(f"- {risk}")
        report_lines.append("")
        report_lines.append(f"**结论**：{summary['verdict']}")
        report_lines.append("")

    report_lines.append("## 组合建议")
    report_lines.append("")
    if passed_list:
        selected = passed_list[:3]
        names = "、".join([f"{c} {n}" for c, n in selected])
        report_lines.append(f"- 建议保留：{names}")
        report_lines.append("- 单只仓位上限：不超过本金的 30%（约 3000 RMB）")
    else:
        report_lines.append("- 当前无明确“通过”标的，建议空仓等待下一轮筛选")
    report_lines.append("- 风控阈值：季度分红预期下修或 OCF/净利润跌破 0.8 时降仓")
    report_lines.append("- 估值阈值：PE>12 或 PB>1.2 触发再评估")

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(report_lines))

    prompt_text = build_llm_prompt_template(top_df)
    with open(PROMPT_FILE, "w", encoding="utf-8") as f:
        f.write(prompt_text)

    print(f"[SUCCESS] 已生成报告：{REPORT_FILE}")
    print(f"[SUCCESS] 已生成 Prompt 模板：{PROMPT_FILE}")


if __name__ == "__main__":
    main()
