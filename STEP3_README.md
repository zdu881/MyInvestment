# Step 3 使用说明（MCP Server + Tools + Demo Client）

## 1) 安装依赖
```bash
python3 -m pip install mcp akshare pandas
```

> 如果你已经安装过 `akshare` 和 `pandas`，只需补装 `mcp` 即可。

## 2) 启动 MCP Server
```bash
python3 step3_mcp_server.py
```

服务名：`DeepValue-AI-Agent`

已注册工具：
- `get_financial_health_check_tool(ticker)`
- `search_market_sentiment_tool(ticker)`
- `calculate_ah_premium_tool(ticker)`

## 3) 本地直接演示（不依赖 MCP 通道）
```bash
python3 step3_mcp_client_demo.py
```

该脚本会依次调用 3 个工具函数，并打印 JSON 结果，便于你先检查逻辑是否符合预期。

## 4) 常见问题

1. **报错：`No module named mcp`**
   - 执行：`python3 -m pip install mcp`

2. **AkShare 接口字段变动导致取数失败**
   - 打开文档：<https://akshare.akfamily.xyz/>
   - 检查函数是否存在：
     ```bash
     python3 -c "import akshare as ak; print(hasattr(ak, 'stock_profit_sheet_by_report_em')); print(hasattr(ak, 'stock_cash_flow_sheet_by_report_em'))"
     ```
   - 打印返回列名：
     ```bash
     python3 -c "import akshare as ak; df=ak.stock_profit_sheet_by_report_em(symbol='600036'); print(df.columns.tolist())"
     ```

3. **A/H 溢价提示未命中映射**
   - 在 `mcp_tools.py` 的 `_guess_hk_ticker_for_ah` 中补充映射即可。

## 5) 下一步
Step 4 将把：
- Step 1 `candidates.csv`
- Step 2 `candidates_step2.csv`
- Step 3 三个工具
串成一个“分析代理”流程，并输出 Markdown 投资可行性报告。

---

## Step 4 一键生成报告

```bash
python3 step4_generate_report.py
```

输出文件：
- `investment_feasibility_report.md`
- `step4_llm_prompt_template.md`
