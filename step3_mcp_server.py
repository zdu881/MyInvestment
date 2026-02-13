#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 3: MCP Server 搭建（核心）
------------------------------
这个脚本会把 mcp_tools.py 中的 3 个函数注册为 MCP Tools。

运行方式（建议先安装依赖）：
1) pip install mcp akshare pandas
2) python3 step3_mcp_server.py

如果你的环境没有安装 mcp，本脚本会给出清晰提示，不会直接崩溃。
"""

from typing import Any, Dict

from mcp_tools import (
    get_financial_health_check,
    search_market_sentiment,
    calculate_ah_premium,
)


def run_server() -> None:
    """启动 MCP Server 并注册工具。"""
    try:
        # FastMCP 是 Python MCP SDK 的简洁服务封装
        from mcp.server.fastmcp import FastMCP
    except Exception as e:
        print("[ERROR] 未找到 MCP Python SDK，请先安装：pip install mcp")
        print(f"[ERROR] 详细信息：{e}")
        return

    # 创建 MCP 服务实例
    mcp = FastMCP("DeepValue-AI-Agent")

    # Tool 1: 财务健康检查
    @mcp.tool()
    def get_financial_health_check_tool(ticker: str) -> Dict[str, Any]:
        """
        获取目标股票过去 3 期（通常近3年）营收、净利润、经营现金流增长与波动信息。
        用于判断是否存在明显“大起大落”。
        """
        return get_financial_health_check(ticker)

    # Tool 2: 市场情绪检索（预留）
    @mcp.tool()
    def search_market_sentiment_tool(ticker: str) -> Dict[str, Any]:
        """
        检索近3个月负面新闻、监管处罚、减持等事件。
        当前为占位实现，后续可对接真实检索 API。
        """
        return search_market_sentiment(ticker)

    # Tool 3: A/H 溢价
    @mcp.tool()
    def calculate_ah_premium_tool(ticker: str) -> Dict[str, Any]:
        """
        对 A+H 股票计算 A/H 溢价率（简化口径）。
        """
        return calculate_ah_premium(ticker)

    print("[INFO] MCP Server 即将启动：DeepValue-AI-Agent")
    print("[INFO] 已注册 Tools: get_financial_health_check_tool, search_market_sentiment_tool, calculate_ah_premium_tool")

    # 启动服务（默认 stdio 传输，适合本地 Agent/Client 对接）
    mcp.run()


if __name__ == "__main__":
    run_server()
