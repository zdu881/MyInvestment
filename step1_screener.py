#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 1: 环境配置与数据获取 (MVP)
---------------------------------
本脚本会完成以下事情：
1) 使用 AkShare 拉取 A 股实时行情（全市场）
2) 按“硬指标”先做第一轮筛选：
   - 总市值 > 200亿
   - PE(TTM) < 10
   - PB < 1.0
   - 一手成本 <= 8000（价格 * 100）
3) 再按股票代码逐个拉取股息率（Dividend Yield）
4) 保留股息率 > 5% 的股票
5) 导出 candidates.csv（UTF-8 with BOM，方便 Excel 打开）

注意：
- AkShare 的接口字段可能随版本变化，代码中已做字段兼容处理和异常兜底。
- 如果接口变动较大，请查看文末提示的“如何查询文档”。
"""

# 导入标准库：time 用于重试等待，traceback 用于打印详细异常，typing 用于类型标注
import re
import os
import time
from datetime import date, timedelta
import traceback
from typing import Optional, List

# 导入第三方库：pandas 做数据处理，akshare 做数据抓取
import pandas as pd
import akshare as ak
from lixinger_adapter import LixingerOpenAPIAdapter

try:
    import baostock as bs
except Exception:
    bs = None


# =============================
# 全局参数区（你可以按需调整）
# =============================
# 市值门槛：200亿人民币
MARKET_CAP_THRESHOLD = 20_000_000_000
# PE(TTM) 门槛
PE_THRESHOLD = 10.0
# PB 门槛
PB_THRESHOLD = 1.0
# 股息率门槛（百分比）：> 5%
DIVIDEND_YIELD_THRESHOLD = 5.0
# 一手成本门槛：<= 8000 元
LOT_COST_THRESHOLD = 8000.0
# 单次网络请求最大重试次数
MAX_RETRY = 3
# 每次重试之间等待秒数
RETRY_SLEEP_SECONDS = 1.5
# 是否启用严格模式：True=全部硬条件都必须满足；False=满足若干项即可
STRICT_MODE = False
# 预筛阶段（不含股息率）需要命中的最少条件数（共4项）
MIN_PASS_COUNT_PRE_DIVIDEND = 3
# 最终阶段（含股息率）需要命中的最少条件数（共5项）
MIN_PASS_COUNT_FINAL = 4
# Baostock 回退时，限制在防御型行业以提升可用性与速度
BAOSTOCK_DEFENSIVE_INDUSTRY_KEYWORDS = [
    "银行", "煤炭", "石油", "天然气", "电力", "高速公路", "港口", "建筑", "工程", "铁路", "水务"
]
# Lixinger token 环境变量名
LIXINGER_TOKEN_ENV = "LIXINGER_TOKEN"
# Lixinger 可调参数（支持环境变量覆盖）
# - LIXINGER_TIMEOUT_SECONDS
# - LIXINGER_MAX_RETRY
# - LIXINGER_RETRY_SLEEP_SECONDS
# - LIXINGER_MAX_RPM
LIXINGER_TIMEOUT_SECONDS = int(os.getenv("LIXINGER_TIMEOUT_SECONDS", "15"))
LIXINGER_MAX_RETRY = int(os.getenv("LIXINGER_MAX_RETRY", str(MAX_RETRY)))
LIXINGER_RETRY_SLEEP_SECONDS = float(os.getenv("LIXINGER_RETRY_SLEEP_SECONDS", str(RETRY_SLEEP_SECONDS)))
LIXINGER_MAX_RPM = int(os.getenv("LIXINGER_MAX_RPM", "900"))


# =============================
# 工具函数区
# =============================
def safe_to_float(value) -> Optional[float]:
    """
    安全地把任意值转为 float。
    - 如果是 None、空字符串、'-'、'--' 等异常值，返回 None
    - 如果转换失败，返回 None
    """
    try:
        # 先处理最常见的“缺失值表现形式”
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if text in {"", "-", "--", "None", "nan", "NaN"}:
                return None
            # 去掉逗号分隔符，例如 "12,345.67"
            text = text.replace(",", "")
            return float(text)
        # 其他类型直接尝试 float
        return float(value)
    except Exception:
        return None


def find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    从候选列名列表中，找到 DataFrame 里第一个存在的列名。
    若都不存在，返回 None。
    """
    for col in candidates:
        if col in df.columns:
            return col
    return None


def is_valid_a_share_code(code_with_prefix: str) -> bool:
    """
    判断是否为常见 A 股代码（sh/sz + 6位）。
    - 上证 A 股常见前缀：60/68
    - 深证 A 股常见前缀：00/30
    """
    if not isinstance(code_with_prefix, str):
        return False
    if not re.match(r"^(sh|sz)\.\d{6}$", code_with_prefix):
        return False

    six = code_with_prefix.split(".")[-1]
    return six.startswith(("60", "68", "00", "30"))


def fetch_a_share_spot_with_retry() -> pd.DataFrame:
    """
    拉取 A 股实时行情，带重试。
    默认使用 ak.stock_zh_a_spot_em()。
    """
    last_error = None
    for i in range(1, MAX_RETRY + 1):
        try:
            print(f"[INFO] 正在拉取 A 股实时行情，第 {i}/{MAX_RETRY} 次尝试...")
            df = ak.stock_zh_a_spot_em()
            if df is None or df.empty:
                raise ValueError("返回数据为空")
            print(f"[INFO] 实时行情拉取成功，共 {len(df)} 条")
            return df
        except Exception as e:
            last_error = e
            print(f"[WARN] 拉取失败：{e}")
            if i < MAX_RETRY:
                time.sleep(RETRY_SLEEP_SECONDS)

    # 所有重试都失败后，抛出更清晰的错误
    raise RuntimeError(f"A 股实时行情拉取失败（已重试 {MAX_RETRY} 次）：{last_error}")


def normalize_spot_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    将 AkShare 实时行情字段标准化为统一字段名，便于后续计算。

    目标字段：
    - ticker        股票代码
    - name          股票名称
    - current_price 当前价格
    - pe_ttm        市盈率（TTM / 动态）
    - pb            市净率
    - total_mv      总市值
    """
    # 复制一份，避免直接修改原始数据
    df = raw_df.copy()

    # 根据不同版本 AkShare 的列名差异，准备候选列名
    code_col = find_first_existing_column(df, ["代码", "symbol", "股票代码"])
    name_col = find_first_existing_column(df, ["名称", "name", "股票名称"])
    price_col = find_first_existing_column(df, ["最新价", "现价", "close", "最新"])
    pe_col = find_first_existing_column(df, ["市盈率-动态", "市盈率", "pe", "PE"])
    pb_col = find_first_existing_column(df, ["市净率", "pb", "PB"])
    mv_col = find_first_existing_column(df, ["总市值", "total_mv", "总市值(元)"])

    # 检查关键列是否齐全（除了 pe/pb 可能存在缺失）
    required_cols = {
        "股票代码": code_col,
        "股票名称": name_col,
        "最新价": price_col,
        "总市值": mv_col,
    }
    missing = [k for k, v in required_cols.items() if v is None]
    if missing:
        raise KeyError(
            f"实时行情缺少关键列：{missing}。当前列名：{list(df.columns)}"
        )

    # 统一命名
    result = pd.DataFrame()
    result["ticker"] = df[code_col].astype(str).str.zfill(6)
    result["name"] = df[name_col].astype(str)
    result["current_price"] = df[price_col].apply(safe_to_float)
    result["pe_ttm"] = df[pe_col].apply(safe_to_float) if pe_col else None
    result["pb"] = df[pb_col].apply(safe_to_float) if pb_col else None
    result["total_mv"] = df[mv_col].apply(safe_to_float)

    # 计算一手成本 = 价格 * 100
    result["lot_cost"] = result["current_price"] * 100

    return result


def pre_filter_by_hard_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    第一轮硬指标过滤（不含股息率）：
    1) 总市值 > 200亿
    2) PE < 10
    3) PB < 1.0
    4) 一手成本 <= 8000
    """
    filtered = df.copy()

    # 删除关键数值为空的数据
    filtered = filtered.dropna(subset=["current_price", "pe_ttm", "pb", "total_mv", "lot_cost"])

    # 先计算每条规则是否命中（预筛阶段暂不含股息率）
    filtered["rule_market_cap"] = filtered["total_mv"] > MARKET_CAP_THRESHOLD
    filtered["rule_pe"] = (filtered["pe_ttm"] > 0) & (filtered["pe_ttm"] < PE_THRESHOLD)
    filtered["rule_pb"] = (filtered["pb"] > 0) & (filtered["pb"] < PB_THRESHOLD)
    filtered["rule_lot_cost"] = filtered["lot_cost"] <= LOT_COST_THRESHOLD

    rule_cols = ["rule_market_cap", "rule_pe", "rule_pb", "rule_lot_cost"]
    filtered["pass_count_pre"] = filtered[rule_cols].sum(axis=1)

    # 严格模式：4/4 全命中；放宽模式：满足若干项即可
    if STRICT_MODE:
        filtered = filtered[filtered["pass_count_pre"] == len(rule_cols)]
    else:
        filtered = filtered[filtered["pass_count_pre"] >= MIN_PASS_COUNT_PRE_DIVIDEND]

    # 重置索引，便于后续遍历
    filtered = filtered.reset_index(drop=True)
    return filtered


def fetch_dividend_yield_for_one_ticker(ticker: str) -> Optional[float]:
    """
    获取单只股票的最新股息率（百分比）。

    兼容两种 AkShare 接口命名：
    - ak.stock_a_lg_indicator(symbol="000001")
    - ak.stock_a_indicator_lg(symbol="000001")

    返回：
    - float: 股息率（%）
    - None : 获取失败或数据缺失
    """
    # 先尝试获取函数对象（不同版本名称可能不同）
    api_func = None
    if hasattr(ak, "stock_a_lg_indicator"):
        api_func = getattr(ak, "stock_a_lg_indicator")
    elif hasattr(ak, "stock_a_indicator_lg"):
        api_func = getattr(ak, "stock_a_indicator_lg")

    # 如果两个接口都没有，直接返回 None
    if api_func is None:
        return None

    # 对单只股票做重试，避免偶发网络波动
    for i in range(1, MAX_RETRY + 1):
        try:
            df = api_func(symbol=ticker)
            if df is None or df.empty:
                return None

            # 可能的股息率列名候选（随版本和接口变化）
            dy_col = find_first_existing_column(df, ["dv_ratio", "dv_ttm", "股息率", "股息率TTM"])
            if dy_col is None:
                return None

            # 取最后一行（通常是最新日期）
            latest_val = safe_to_float(df[dy_col].iloc[-1])
            return latest_val
        except Exception:
            if i < MAX_RETRY:
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                return None

    return None


def add_dividend_yield_and_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    对第一轮筛选结果逐只补齐股息率，然后过滤股息率 > 5%。
    """
    if df.empty:
        # 如果前面已经没有股票，直接返回空表并加上列
        out = df.copy()
        out["dividend_yield"] = pd.Series(dtype="float64")
        return out

    rows = []
    total = len(df)

    for idx, row in df.iterrows():
        ticker = row["ticker"]
        # 打印进度，便于观察脚本运行状态
        if (idx + 1) % 20 == 0 or idx == 0 or idx + 1 == total:
            print(f"[INFO] 正在获取股息率：{idx + 1}/{total} - {ticker}")

        dy = fetch_dividend_yield_for_one_ticker(ticker)

        # 复制原行数据并新增股息率
        new_row = row.to_dict()
        new_row["dividend_yield"] = dy
        rows.append(new_row)

        # 小睡一下，降低被限流概率
        time.sleep(0.05)

    out = pd.DataFrame(rows)

    # 计算股息率规则，并统计最终命中数（5项）
    out["rule_dividend"] = out["dividend_yield"] > DIVIDEND_YIELD_THRESHOLD
    out["pass_count_final"] = out[["rule_market_cap", "rule_pe", "rule_pb", "rule_lot_cost", "rule_dividend"]].sum(axis=1)

    # 严格模式：5/5 全命中；放宽模式：满足若干项即可
    if STRICT_MODE:
        out = out[out["pass_count_final"] == 5]
    else:
        out = out[out["pass_count_final"] >= MIN_PASS_COUNT_FINAL]

    # 按股息率从高到低排序
    out = out.sort_values(by=["dividend_yield", "pe_ttm"], ascending=[False, True]).reset_index(drop=True)
    return out


def run_akshare_pipeline() -> pd.DataFrame:
    """
    使用 AkShare 全量流程。
    返回标准化后的最终候选表（含 dividend_yield）。
    """
    spot_raw = fetch_a_share_spot_with_retry()
    spot_std = normalize_spot_dataframe(spot_raw)
    print(f"[INFO] [AkShare] 标准化后记录数：{len(spot_std)}")

    round1 = pre_filter_by_hard_rules(spot_std)
    print(f"[INFO] [AkShare] 第一轮过滤后记录数：{len(round1)}")

    final_df = add_dividend_yield_and_filter(round1)
    print(f"[INFO] [AkShare] 最终候选记录数：{len(final_df)}")
    final_df["data_source"] = "akshare"
    return final_df


def run_baostock_fallback_pipeline() -> pd.DataFrame:
    """
    Baostock 兜底流程：
    - 当前网络下 AkShare 不可用时启用
    - 为控制速度，先聚焦防御型行业（银行/能源/基建）
    - 指标口径：
      - 市值=现价*总股本(totalShare)
      - 股息率=每股现金分红/现价
      - PE=peTTM, PB=pbMRQ
    """
    if bs is None:
        raise RuntimeError("未安装 baostock，无法执行兜底流程。请先 pip install baostock")

    login_result = bs.login()
    if login_result.error_code != "0":
        raise RuntimeError(f"Baostock 登录失败：{login_result.error_msg}")

    try:
        # 1) 行业表中选防御型行业股票
        industry_rs = bs.query_stock_industry()
        industry_rows = []
        while industry_rs.next():
            industry_rows.append(industry_rs.get_row_data())
        industry_df = pd.DataFrame(industry_rows, columns=industry_rs.fields)

        if industry_df.empty:
            raise RuntimeError("Baostock 行业数据为空")

        mask = industry_df["industry"].fillna("").apply(
            lambda text: any(k in text for k in BAOSTOCK_DEFENSIVE_INDUSTRY_KEYWORDS)
        )
        universe = industry_df[mask].copy()
        universe = universe[universe["code"].apply(is_valid_a_share_code)]
        universe = universe.drop_duplicates(subset=["code"]).reset_index(drop=True)

        if universe.empty:
            print("[WARN] [Baostock] 防御型行业股票池为空")
            return pd.DataFrame(columns=["ticker", "name", "current_price", "pe_ttm", "pb", "dividend_yield", "lot_cost", "data_source"])

        end_day = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        start_day = (pd.to_datetime(end_day) - pd.Timedelta(days=25)).strftime("%Y-%m-%d")

        rows = []
        for idx, row in universe.iterrows():
            code = row["code"]
            name = row.get("code_name", "")

            # 2) 日线估值（取最近一个交易日）
            k_rs = bs.query_history_k_data_plus(
                code,
                "date,close,peTTM,pbMRQ,isST",
                start_date=start_day,
                end_date=end_day,
                frequency="d",
                adjustflag="3",
            )
            k_rows = []
            while k_rs.next():
                k_rows.append(k_rs.get_row_data())
            if not k_rows:
                continue

            k_df = pd.DataFrame(k_rows, columns=k_rs.fields)
            k_df = k_df[k_df["isST"] != "1"]
            if k_df.empty:
                continue
            latest = k_df.iloc[-1]

            current_price = safe_to_float(latest.get("close"))
            pe_ttm = safe_to_float(latest.get("peTTM"))
            pb = safe_to_float(latest.get("pbMRQ"))
            if current_price is None or current_price <= 0:
                continue

            lot_cost = current_price * 100

            # 3) 利润表读取 totalShare，用于估算总市值
            total_share = None
            for y, q in [(2024, 4), (2023, 4)]:
                p_rs = bs.query_profit_data(code=code, year=y, quarter=q)
                p_rows = []
                while p_rs.next():
                    p_rows.append(p_rs.get_row_data())
                if p_rows:
                    p_df = pd.DataFrame(p_rows, columns=p_rs.fields)
                    total_share = safe_to_float(p_df.iloc[-1].get("totalShare"))
                    if total_share is not None:
                        break
            total_mv = current_price * total_share if total_share is not None else None

            # 4) 分红表读取每股现金分红，估算股息率
            dps = None
            for year in ["2025", "2024", "2023"]:
                d_rs = bs.query_dividend_data(code=code, year=year, yearType="report")
                d_rows = []
                while d_rs.next():
                    d_rows.append(d_rs.get_row_data())
                if d_rows:
                    d_df = pd.DataFrame(d_rows, columns=d_rs.fields)
                    dps = safe_to_float(d_df.iloc[-1].get("dividCashPsBeforeTax"))
                    if dps is not None:
                        break
            dividend_yield = (dps / current_price) * 100 if (dps is not None and current_price > 0) else None

            rows.append(
                {
                    "ticker": code.split(".")[-1],
                    "name": name,
                    "current_price": current_price,
                    "pe_ttm": pe_ttm,
                    "pb": pb,
                    "total_mv": total_mv,
                    "lot_cost": lot_cost,
                    "dividend_yield": dividend_yield,
                    "data_source": "baostock",
                    "rule_market_cap": (total_mv is not None) and (total_mv > MARKET_CAP_THRESHOLD),
                    "rule_pe": (pe_ttm is not None) and (pe_ttm > 0) and (pe_ttm < PE_THRESHOLD),
                    "rule_pb": (pb is not None) and (pb > 0) and (pb < PB_THRESHOLD),
                    "rule_lot_cost": lot_cost <= LOT_COST_THRESHOLD,
                    "rule_dividend": (dividend_yield is not None) and (dividend_yield > DIVIDEND_YIELD_THRESHOLD),
                }
            )

            if (idx + 1) % 30 == 0:
                time.sleep(0.2)

        out = pd.DataFrame(rows)
        if out.empty:
            return pd.DataFrame(columns=["ticker", "name", "current_price", "pe_ttm", "pb", "dividend_yield", "lot_cost", "data_source"])

        out["pass_count_final"] = out[["rule_market_cap", "rule_pe", "rule_pb", "rule_lot_cost", "rule_dividend"]].sum(axis=1)
        if STRICT_MODE:
            out = out[out["pass_count_final"] == 5]
        else:
            out = out[out["pass_count_final"] >= MIN_PASS_COUNT_FINAL]

        out = out.sort_values(by=["pass_count_final", "dividend_yield", "pe_ttm"], ascending=[False, False, True]).reset_index(drop=True)
        print(f"[INFO] [Baostock] 兜底候选记录数：{len(out)}")
        return out
    finally:
        bs.logout()


def fetch_total_share_by_baostock(code_with_prefix: str) -> Optional[float]:
    """从 Baostock 利润表获取总股本，用于由市值反推现价。"""
    total_share = None
    for y, q in [(2024, 4), (2023, 4)]:
        p_rs = bs.query_profit_data(code=code_with_prefix, year=y, quarter=q)
        p_rows = []
        while p_rs.next():
            p_rows.append(p_rs.get_row_data())
        if p_rows:
            p_df = pd.DataFrame(p_rows, columns=p_rs.fields)
            total_share = safe_to_float(p_df.iloc[-1].get("totalShare"))
            if total_share is not None:
                return total_share
    return None


def run_lixinger_pipeline() -> pd.DataFrame:
    """
    Lixinger 兜底流程（AkShare 失败时启用）：
    - token 从环境变量 LIXINGER_TOKEN 读取
    - 非金融基本面从 Lixinger 获取（mc/pe/pb/股息率相关）
    - 股票池与总股本通过 Baostock 补齐
    """
    token = os.getenv(LIXINGER_TOKEN_ENV, "").strip()
    if not token:
        raise RuntimeError("未检测到 LIXINGER_TOKEN，跳过 Lixinger 数据源")
    if bs is None:
        raise RuntimeError("Lixinger 适配流程依赖 baostock 股票池，请先安装 baostock")

    login_result = bs.login()
    if login_result.error_code != "0":
        raise RuntimeError(f"Baostock 登录失败：{login_result.error_msg}")

    try:
        industry_rs = bs.query_stock_industry()
        industry_rows = []
        while industry_rs.next():
            industry_rows.append(industry_rs.get_row_data())
        industry_df = pd.DataFrame(industry_rows, columns=industry_rs.fields)
        if industry_df.empty:
            raise RuntimeError("Baostock 行业数据为空")

        mask = industry_df["industry"].fillna("").apply(
            lambda text: any(k in text for k in BAOSTOCK_DEFENSIVE_INDUSTRY_KEYWORDS)
        )
        universe = industry_df[mask].copy()
        universe = universe[universe["code"].apply(is_valid_a_share_code)]
        universe["ticker"] = universe["code"].str.split(".").str[-1]
        universe = universe.drop_duplicates(subset=["ticker"]).reset_index(drop=True)

        if universe.empty:
            return pd.DataFrame(columns=["ticker", "name", "current_price", "pe_ttm", "pb", "dividend_yield", "lot_cost", "data_source"])

        adapter = LixingerOpenAPIAdapter(
            token=token,
            timeout=LIXINGER_TIMEOUT_SECONDS,
            max_retry=LIXINGER_MAX_RETRY,
            retry_sleep_seconds=LIXINGER_RETRY_SLEEP_SECONDS,
            max_requests_per_minute=LIXINGER_MAX_RPM,
        )
        print(
            "[INFO] [Lixinger] 配置："
            f"timeout={LIXINGER_TIMEOUT_SECONDS}s, "
            f"max_retry={LIXINGER_MAX_RETRY}, "
            f"retry_sleep={LIXINGER_RETRY_SLEEP_SECONDS}s, "
            f"max_rpm={LIXINGER_MAX_RPM}"
        )

        # 先请求核心估值字段
        target_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        stock_codes = universe["ticker"].tolist()
        metrics_core = ["mc", "pe_ttm", "pb"]

        core_df = adapter.query_non_financial(
            date_str=target_date,
            stock_codes=stock_codes,
            metrics_list=metrics_core,
        )
        if core_df.empty:
            raise RuntimeError("Lixinger 返回空数据")

        # 尝试请求股息率字段（字段名不稳定，逐个尝试）
        dividend_df = pd.DataFrame()
        for metric_name in ["dyr", "d_yield", "dividend_yield", "dv_ratio"]:
            try:
                temp = adapter.query_non_financial(
                    date_str=target_date,
                    stock_codes=stock_codes,
                    metrics_list=[metric_name],
                )
                if not temp.empty and metric_name in temp.columns:
                    dividend_df = temp[["stockCode", metric_name]].copy()
                    dividend_df = dividend_df.rename(columns={metric_name: "dividend_yield"})
                    break
            except Exception:
                continue

        core_df["ticker"] = core_df["stockCode"].astype(str).str.zfill(6)
        merged = core_df.merge(
            universe[["ticker", "code_name", "code"]],
            on="ticker",
            how="left",
        )

        if not dividend_df.empty:
            dividend_df["ticker"] = dividend_df["stockCode"].astype(str).str.zfill(6)
            merged = merged.merge(dividend_df[["ticker", "dividend_yield"]], on="ticker", how="left")
        else:
            merged["dividend_yield"] = None

        # 将 Lixinger 字段映射为内部统一字段
        merged["name"] = merged["code_name"].fillna("")
        merged["total_mv"] = merged["mc"].apply(safe_to_float)
        merged["pe_ttm"] = merged["pe_ttm"].apply(safe_to_float)
        merged["pb"] = merged["pb"].apply(safe_to_float)
        merged["dividend_yield"] = merged["dividend_yield"].apply(safe_to_float)

        # 由市值/总股本反推现价
        price_list = []
        for _, row in merged.iterrows():
            code_with_prefix = row.get("code")
            mc = safe_to_float(row.get("total_mv"))
            if not code_with_prefix or mc is None:
                price_list.append(None)
                continue

            total_share = fetch_total_share_by_baostock(code_with_prefix)
            if total_share is None or total_share <= 0:
                price_list.append(None)
            else:
                price_list.append(mc / total_share)

        merged["current_price"] = price_list
        merged["lot_cost"] = merged["current_price"] * 100

        # 规则命中情况
        merged = merged.dropna(subset=["current_price", "lot_cost"])
        merged["rule_market_cap"] = (merged["total_mv"].notna()) & (merged["total_mv"] > MARKET_CAP_THRESHOLD)
        merged["rule_pe"] = (merged["pe_ttm"].notna()) & (merged["pe_ttm"] > 0) & (merged["pe_ttm"] < PE_THRESHOLD)
        merged["rule_pb"] = (merged["pb"].notna()) & (merged["pb"] > 0) & (merged["pb"] < PB_THRESHOLD)
        merged["rule_lot_cost"] = merged["lot_cost"] <= LOT_COST_THRESHOLD
        merged["rule_dividend"] = (merged["dividend_yield"].notna()) & (merged["dividend_yield"] > DIVIDEND_YIELD_THRESHOLD)
        merged["pass_count_final"] = merged[["rule_market_cap", "rule_pe", "rule_pb", "rule_lot_cost", "rule_dividend"]].sum(axis=1)

        if STRICT_MODE:
            merged = merged[merged["pass_count_final"] == 5]
        else:
            merged = merged[merged["pass_count_final"] >= MIN_PASS_COUNT_FINAL]

        out = merged[[
            "ticker", "name", "current_price", "pe_ttm", "pb", "dividend_yield", "lot_cost",
            "pass_count_final", "rule_market_cap", "rule_pe", "rule_pb", "rule_lot_cost", "rule_dividend"
        ]].copy()
        out["data_source"] = "lixinger"
        out = out.sort_values(by=["pass_count_final", "dividend_yield", "pe_ttm"], ascending=[False, False, True]).reset_index(drop=True)
        print(f"[INFO] [Lixinger] 候选记录数：{len(out)}")
        return out
    finally:
        bs.logout()


def main():
    """
    主流程：拉取数据 -> 标准化 -> 第一轮过滤 -> 补股息率 -> 最终过滤 -> 导出 CSV
    """
    try:
        # 1) 先走 AkShare 主流程，失败后自动切换 Lixinger，再兜底 Baostock
        try:
            final_df = run_akshare_pipeline()
        except Exception as ak_error:
            print(f"[WARN] AkShare 主流程失败，准备切换 Lixinger：{ak_error}")
            try:
                final_df = run_lixinger_pipeline()
            except Exception as lix_error:
                print(f"[WARN] Lixinger 失败，准备切换 Baostock 兜底：{lix_error}")
                final_df = run_baostock_fallback_pipeline()

        # 5) 输出列（按你的要求）
        output_cols = [
            "ticker",
            "name",
            "current_price",
            "pe_ttm",
            "pb",
            "dividend_yield",
            "lot_cost",
            "data_source",
            "pass_count_final",
            "rule_market_cap",
            "rule_pe",
            "rule_pb",
            "rule_lot_cost",
            "rule_dividend",
        ]

        # 只保留目标列，且将列名改为更友好的中文
        output_df = final_df[output_cols].copy()
        output_df = output_df.rename(
            columns={
                "ticker": "股票代码",
                "name": "名称",
                "current_price": "现价",
                "pe_ttm": "PE(TTM)",
                "pb": "PB",
                "dividend_yield": "股息率(%)",
                "lot_cost": "一手成本",
                "data_source": "数据源",
                "pass_count_final": "命中条件数",
                "rule_market_cap": "命中_市值",
                "rule_pe": "命中_PE",
                "rule_pb": "命中_PB",
                "rule_lot_cost": "命中_一手成本",
                "rule_dividend": "命中_股息率",
            }
        )

        # 保留两位小数（非必须，但更易读）
        for col in ["现价", "PE(TTM)", "PB", "股息率(%)", "一手成本"]:
            output_df[col] = output_df[col].round(2)

        # 导出 CSV，使用 utf-8-sig 兼容 Excel 中文显示
        output_path = "candidates.csv"
        output_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"[SUCCESS] 已输出候选列表：{output_path}")

    except Exception as e:
        # 捕获主流程异常，避免程序“硬崩”
        print("\n[ERROR] 脚本执行失败：", e)
        print("[ERROR] 详细堆栈如下：")
        traceback.print_exc()

        # 给出接口排查建议（你要求的“如何查询文档”）
        print("\n[提示] 可能是 AkShare 接口或字段更新导致。你可以这样排查：")
        print("1) 打开官方文档首页：https://akshare.akfamily.xyz/")
        print("2) 在文档站内搜索：stock_zh_a_spot_em、stock_a_lg_indicator")
        print("3) 在本地查看函数是否存在：")
        print("   python -c \"import akshare as ak; print(hasattr(ak, 'stock_zh_a_spot_em')); print(hasattr(ak, 'stock_a_lg_indicator')); print(hasattr(ak, 'stock_a_indicator_lg'))\"")
        print("4) 查看返回字段：")
        print("   python -c \"import akshare as ak; df=ak.stock_zh_a_spot_em(); print(df.columns.tolist())\"")
        print("5) 若要启用 Lixinger：")
        print("   export LIXINGER_TOKEN='你的token' && python3 step1_screener.py")
        print("6) 若要调节 Lixinger 限流与重试（示例）：")
        print("   LIXINGER_MAX_RPM=600 LIXINGER_MAX_RETRY=5 LIXINGER_TIMEOUT_SECONDS=20 python3 step1_screener.py")


# Python 脚本入口
if __name__ == "__main__":
    main()
