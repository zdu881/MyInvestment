#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 3: 本地 Client 模拟调用（无需真正连接 MCP）
-----------------------------------------------
作用：
- 在不接入外部 Agent 的情况下，先验证 Tool 逻辑是否可用
- 便于你快速看到 3 个工具的返回结构

运行：
python3 step3_mcp_client_demo.py
"""

from mcp_tools import (
    get_financial_health_check,
    search_market_sentiment,
    calculate_ah_premium,
    pretty_json,
)


def main() -> None:
    # 你可以替换成任意 A 股代码测试
    test_ticker = "600036"

    print("\n" + "=" * 80)
    print(f"[DEMO] Tool 1 - get_financial_health_check({test_ticker})")
    result_1 = get_financial_health_check(test_ticker)
    print(pretty_json(result_1))

    print("\n" + "=" * 80)
    print(f"[DEMO] Tool 2 - search_market_sentiment({test_ticker})")
    result_2 = search_market_sentiment(test_ticker)
    print(pretty_json(result_2))

    print("\n" + "=" * 80)
    print(f"[DEMO] Tool 3 - calculate_ah_premium({test_ticker})")
    result_3 = calculate_ah_premium(test_ticker)
    print(pretty_json(result_3))

    print("\n[INFO] 本地 Tool 调用演示完成。")


if __name__ == "__main__":
    main()
