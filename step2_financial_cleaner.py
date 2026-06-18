#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Step 2: 财务深度清洗（现金流真伪校验）
--------------------------------------
本脚本用于对 Step 1 的候选股票做二次过滤：
1) 读取 candidates.csv
2) 批量抓取每只股票最新财报中的：
   - 经营活动产生的现金流量净额（OCF）
   - 净利润（Net Income）
3) 计算 OCF/NetIncome 比率
4) 仅保留 OCF/NetIncome > 1.0 的公司（且净利润 > 0）
5) 导出 candidates_step2.csv

为什么这样做：
- 有些公司“账面利润看起来不错”，但现金流很差，可能存在应收堆积、利润质量低等问题。
- OCF/NetIncome > 1 通常意味着利润更“有现金支撑”。
"""

# 导入标准库：os 用于文件判断，time 用于重试等待，traceback 用于打印错误堆栈
import os
import multiprocessing as mp
from pathlib import Path
import signal
import time
import traceback
import threading
from contextlib import contextmanager
from typing import Any, Callable, Optional, List, Tuple, Dict

# 导入第三方库：pandas 处理表格，akshare 拉财务数据
import pandas as pd
import akshare as ak

try:
    import baostock as bs
except Exception:
    bs = None


# =============================
# 全局参数（可按需调整）
# =============================
# 输入文件：Step 1 产出的候选股票列表
INPUT_CSV = "candidates.csv"
# 输出文件：Step 2 清洗后的候选列表
OUTPUT_CSV = "candidates_step2.csv"
# OCF/净利润 比率阈值
OCF_NET_INCOME_THRESHOLD = 1.0
# 是否按行业使用差异化阈值（True 时优先行业阈值，否则统一使用 OCF_NET_INCOME_THRESHOLD）
USE_INDUSTRY_THRESHOLD = True
# 分行业阈值（关键词匹配，越靠前优先级越高）
INDUSTRY_OCF_THRESHOLD_RULES = [
    ("银行", 0.5),
    ("建筑", 0.3),
    ("工程", 0.3),
    ("高速公路", 0.6),
    ("港口", 0.6),
    ("煤炭", 0.8),
    ("石油", 0.8),
    ("天然气", 0.8),
    ("电力", 0.8),
]

def env_int(name: str, default: int) -> int:
    try:
        value = int(str(os.environ.get(name, "")).strip())
        return value if value > 0 else default
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    try:
        value = float(str(os.environ.get(name, "")).strip())
        return value if value > 0 else default
    except Exception:
        return default


# 网络请求最大重试次数
MAX_RETRY = env_int("STEP2_MAX_RETRY", 3)
# 重试间隔（秒）
RETRY_SLEEP_SECONDS = env_float("STEP2_RETRY_SLEEP_SECONDS", 1.2)
# 每只股票请求之间的休眠，减少限流概率
PER_TICKER_SLEEP_SECONDS = env_float("STEP2_PER_TICKER_SLEEP_SECONDS", 0.08)
# 单个 AkShare 调用超时（秒）
AK_CALL_TIMEOUT_SECONDS = env_float("STEP2_AK_CALL_TIMEOUT_SECONDS", 8.0)
# 单个 Baostock 调用超时（秒）
BAOSTOCK_CALL_TIMEOUT_SECONDS = env_float("STEP2_BAOSTOCK_CALL_TIMEOUT_SECONDS", 8.0)
# 单只股票完整财务计算超时（秒）
PER_TICKER_TIMEOUT_SECONDS = env_float("STEP2_PER_TICKER_TIMEOUT_SECONDS", 15.0)
# 行业映射加载超时（秒）
INDUSTRY_MAP_TIMEOUT_SECONDS = env_float("STEP2_INDUSTRY_MAP_TIMEOUT_SECONDS", 8.0)


# =============================
# 通用工具函数
# =============================
class ExternalCallTimeout(TimeoutError):
    """Raised when a third-party data source call exceeds its local timeout."""


def _alarm_timeout_supported() -> bool:
    return (
        hasattr(signal, "SIGALRM")
        and hasattr(signal, "setitimer")
        and threading.current_thread() is threading.main_thread()
    )


def _fork_context():
    try:
        return mp.get_context("fork")
    except ValueError:
        return None


def _process_timeout_supported() -> bool:
    return mp.current_process().name == "MainProcess" and _fork_context() is not None


def _timeout_worker(func: Callable[[], Any], queue) -> None:
    try:
        queue.put(("ok", func()))
    except BaseException as exc:
        queue.put(("error", type(exc).__name__, str(exc)))


@contextmanager
def timeout_after(timeout_sec: float, label: str):
    if timeout_sec <= 0 or not _alarm_timeout_supported():
        yield
        return

    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    started_at = time.monotonic()

    def _handle_timeout(_signum, _frame):
        raise ExternalCallTimeout(f"{label} timed out after {timeout_sec:.1f}s")

    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_sec)
    try:
        yield
    finally:
        elapsed = time.monotonic() - started_at
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            remaining = max(0.001, previous_timer[0] - elapsed)
            signal.setitimer(signal.ITIMER_REAL, remaining, previous_timer[1])


def run_with_timeout(func: Callable[[], Any], timeout_sec: float, label: str) -> Any:
    if timeout_sec > 0 and _process_timeout_supported():
        ctx = _fork_context()
        if ctx is not None:
            queue = ctx.Queue(maxsize=1)
            proc = ctx.Process(target=_timeout_worker, args=(func, queue), daemon=True)
            proc.start()
            proc.join(timeout_sec)
            if proc.is_alive():
                proc.terminate()
                proc.join(1.0)
                if proc.is_alive():
                    proc.kill()
                    proc.join(1.0)
                raise ExternalCallTimeout(f"{label} timed out after {timeout_sec:.1f}s")
            if queue.empty():
                raise RuntimeError(f"{label} failed without returning a result (exitcode={proc.exitcode})")
            payload = queue.get()
            if payload[0] == "ok":
                return payload[1]
            raise RuntimeError(f"{label} failed: {payload[1]}: {payload[2]}")

    with timeout_after(timeout_sec, label):
        return func()


def logout_baostock_safely() -> None:
    if bs is None:
        return
    try:
        bs.logout()
    except Exception:
        return


def remove_output_file() -> None:
    try:
        Path(OUTPUT_CSV).unlink(missing_ok=True)
    except Exception:
        return


def write_csv_atomic(df: pd.DataFrame, output_path: str) -> None:
    path = Path(output_path)
    tmp_path = path.with_name(f"{path.name}.tmp")
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    os.replace(tmp_path, path)


def safe_to_float(value) -> Optional[float]:
    """
    将各种可能格式的数据安全转换为 float。
    支持：
    - 普通数字
    - 带逗号字符串："1,234.56"
    - 带百分号字符串："12.3%"
    - 空值、'-'、'--' 等异常值会返回 None
    """
    try:
        # None 直接返回 None
        if value is None:
            return None

        # 如果是字符串，先做清洗
        if isinstance(value, str):
            text = value.strip()
            if text in {"", "-", "--", "None", "nan", "NaN"}:
                return None
            text = text.replace(",", "")
            text = text.replace("%", "")
            return float(text)

        # 其他类型尝试直接转换
        return float(value)
    except Exception:
        return None


def find_first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    在 DataFrame 中，按顺序查找候选列名里第一个存在的列。
    如果一个都找不到，返回 None。
    """
    for col in candidates:
        if col in df.columns:
            return col
    return None


def normalize_ticker(value: str) -> str:
    """
    规范化股票代码：确保是 6 位字符串（A 股常见格式）。
    """
    text = str(value).strip()
    # 提取数字部分（兼容可能出现的前后缀）
    digits = "".join(ch for ch in text if ch.isdigit())
    # 左侧补零到 6 位
    return digits.zfill(6)[-6:]


def _load_industry_map_from_baostock_unchecked() -> Dict[str, str]:
    login_result = bs.login()
    if login_result.error_code != "0":
        return {}

    try:
        rs = bs.query_stock_industry()
        rows = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return {}

        df = pd.DataFrame(rows, columns=rs.fields)
        if "code" not in df.columns or "industry" not in df.columns:
            return {}

        df = df.copy()
        df["ticker"] = df["code"].astype(str).str.split(".").str[-1].str.zfill(6)
        df["industry"] = df["industry"].astype(str)
        df = df.drop_duplicates(subset=["ticker"], keep="last")
        return dict(zip(df["ticker"], df["industry"]))
    except Exception:
        return {}
    finally:
        logout_baostock_safely()


def load_industry_map_from_baostock() -> Dict[str, str]:
    """
    从 Baostock 读取“股票代码->行业”映射。
    返回示例：{"601668": "房屋建筑业"}
    """
    if bs is None:
        return {}

    try:
        result = run_with_timeout(
            _load_industry_map_from_baostock_unchecked,
            INDUSTRY_MAP_TIMEOUT_SECONDS,
            "baostock query_stock_industry",
        )
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


def get_threshold_by_industry(industry_text: str) -> float:
    """根据行业关键词返回阈值，匹配不到时返回默认阈值。"""
    if not USE_INDUSTRY_THRESHOLD:
        return OCF_NET_INCOME_THRESHOLD

    text = str(industry_text or "")
    for keyword, threshold in INDUSTRY_OCF_THRESHOLD_RULES:
        if keyword in text:
            return threshold
    return OCF_NET_INCOME_THRESHOLD


# =============================
# 财务数据抓取函数
# =============================
def _call_ak_function_with_retry(func_name: str, symbol: str) -> Optional[pd.DataFrame]:
    """
    按函数名动态调用 AkShare 接口，并带重试。

    参数：
    - func_name: AkShare 函数名（字符串）
    - symbol: 股票代码（如 600519）

    返回：
    - 成功：DataFrame
    - 失败：None
    """
    # 若当前 akshare 版本没有这个函数，直接返回 None
    if not hasattr(ak, func_name):
        return None

    # 通过 getattr 获取函数对象
    api_func = getattr(ak, func_name)

    # 重试调用
    for i in range(1, MAX_RETRY + 1):
        try:
            df = run_with_timeout(
                lambda: api_func(symbol=symbol),
                AK_CALL_TIMEOUT_SECONDS,
                f"akshare {func_name}({symbol})",
            )
            if df is not None and not df.empty:
                return df
            return None
        except Exception:
            if i < MAX_RETRY:
                time.sleep(RETRY_SLEEP_SECONDS)
            else:
                return None

    return None


def fetch_cashflow_and_profit(symbol: str) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], str, str]:
    """
    获取某只股票的现金流量表和利润表。

    这里做了“多接口兜底”，因为 AkShare 在不同版本中命名可能有差异。

    返回：
    - cashflow_df: 现金流量表
    - profit_df: 利润表
    - cashflow_api_used: 实际命中的现金流接口名
    - profit_api_used: 实际命中的利润表接口名
    """
    # 候选接口名（按优先级排列）
    cashflow_candidates = [
        "stock_cash_flow_sheet_by_report_em",   # 常见：按报告期
        "stock_cash_flow_sheet_by_yearly_em",   # 兜底：按年度
    ]
    profit_candidates = [
        "stock_profit_sheet_by_report_em",      # 常见：按报告期
        "stock_profit_sheet_by_yearly_em",      # 兜底：按年度
    ]

    # 默认返回值
    cashflow_df = None
    profit_df = None
    cashflow_api_used = ""
    profit_api_used = ""

    # 依次尝试现金流接口
    for api_name in cashflow_candidates:
        temp_df = _call_ak_function_with_retry(api_name, symbol)
        if temp_df is not None and not temp_df.empty:
            cashflow_df = temp_df
            cashflow_api_used = api_name
            break

    # 依次尝试利润表接口
    for api_name in profit_candidates:
        temp_df = _call_ak_function_with_retry(api_name, symbol)
        if temp_df is not None and not temp_df.empty:
            profit_df = temp_df
            profit_api_used = api_name
            break

    return cashflow_df, profit_df, cashflow_api_used, profit_api_used


def _fetch_by_baostock_unchecked(symbol: str) -> Tuple[Optional[float], Optional[float], Optional[str], str]:
    code = f"sh.{symbol}" if symbol.startswith("6") else f"sz.{symbol}"

    login_result = bs.login()
    if login_result.error_code != "0":
        return None, None, None, "baostock_login_failed"

    try:
        net_income = None
        report_period = None
        cfo_to_np = None

        # 1) 读取最近可用年报净利润
        for y, q in [(2024, 4), (2023, 4)]:
            p_rs = bs.query_profit_data(code=code, year=y, quarter=q)
            p_rows = []
            while p_rs.next():
                p_rows.append(p_rs.get_row_data())
            if p_rows:
                p_df = pd.DataFrame(p_rows, columns=p_rs.fields)
                last = p_df.iloc[-1]
                net_income = safe_to_float(last.get("netProfit"))
                report_period = str(last.get("statDate"))
                if net_income is not None:
                    break

        # 2) 读取最近可用年报 CFOToNP
        for y, q in [(2024, 4), (2023, 4)]:
            c_rs = bs.query_cash_flow_data(code=code, year=y, quarter=q)
            c_rows = []
            while c_rs.next():
                c_rows.append(c_rs.get_row_data())
            if c_rows:
                c_df = pd.DataFrame(c_rows, columns=c_rs.fields)
                last = c_df.iloc[-1]
                cfo_to_np = safe_to_float(last.get("CFOToNP"))
                if report_period is None:
                    report_period = str(last.get("statDate"))
                if cfo_to_np is not None:
                    break

        if net_income is None or cfo_to_np is None:
            return None, None, report_period, "baostock_missing_fields"

        ocf = cfo_to_np * net_income
        return ocf, net_income, report_period, "baostock"
    finally:
        logout_baostock_safely()


def fetch_by_baostock(symbol: str) -> Tuple[Optional[float], Optional[float], Optional[str], str]:
    """
    使用 Baostock 获取 OCF/净利润口径数据。

    Baostock 的现金流接口直接提供 CFOToNP（经营现金流/净利润）比例，
    利润表提供 netProfit。因此可反推出 OCF=ratio*netProfit。

    返回：
    - ocf
    - net_income
    - report_period
    - source_tag
    """
    if bs is None:
        return None, None, None, "baostock_unavailable"

    try:
        return run_with_timeout(
            lambda: _fetch_by_baostock_unchecked(symbol),
            PER_TICKER_TIMEOUT_SECONDS,
            f"baostock financial fetch {symbol}",
        )
    except ExternalCallTimeout:
        return None, None, None, "baostock_timeout"
    except Exception:
        return None, None, None, "baostock_error"


# =============================
# 指标提取与计算函数
# =============================
def extract_latest_value(df: pd.DataFrame, value_col_candidates: List[str]) -> Tuple[Optional[float], Optional[str]]:
    """
    从财报表格里提取“最新一期”的目标数值。

    处理思路：
    1) 先找到报告期列（如果有）并排序，让最新期在最后
    2) 在候选指标列中找到第一列存在的
    3) 取最后一行并转为 float

    返回：
    - 数值（float 或 None）
    - 报告期字符串（或 None）
    """
    if df is None or df.empty:
        return None, None

    data = df.copy()

    # 常见报告期列名（不同接口可能不同）
    report_col = find_first_existing_column(data, ["REPORT_DATE", "报告日期", "报告期", "日期"])

    # 若有报告期列，尝试转时间并排序
    report_label = None
    if report_col is not None:
        data["__report_date_tmp"] = pd.to_datetime(data[report_col], errors="coerce")
        data = data.sort_values(by="__report_date_tmp", ascending=True)
        # 记录最新一期标签
        report_label = str(data[report_col].iloc[-1])

    # 找目标值列
    value_col = find_first_existing_column(data, value_col_candidates)
    if value_col is None:
        return None, report_label

    # 提取最新值
    latest_value = safe_to_float(data[value_col].iloc[-1])
    return latest_value, report_label


def calculate_ocf_net_income_ratio(symbol: str) -> dict:
    """
    计算单只股票的 OCF/NetIncome 比率。

    返回字典字段：
    - ticker
    - report_period
    - ocf
    - net_income
    - ocf_net_income_ratio
    - cashflow_api
    - profit_api
    - status（ok / error）
    - message（错误信息或说明）
    """
    try:
        # 先尝试 AkShare 拉两张表
        cashflow_df, profit_df, cashflow_api, profit_api = fetch_cashflow_and_profit(symbol)

        # 若 AkShare 失败，自动切换 Baostock
        if cashflow_df is None or profit_df is None:
            ocf, net_income, report_period, source_tag = fetch_by_baostock(symbol)
            if ocf is None or net_income is None:
                return {
                    "ticker": symbol,
                    "report_period": report_period,
                    "ocf": None,
                    "net_income": None,
                    "ocf_net_income_ratio": None,
                    "cashflow_api": cashflow_api,
                    "profit_api": profit_api,
                    "status": "error",
                    "source": source_tag,
                    "message": "AkShare失败且Baostock兜底失败",
                }

            if net_income <= 0:
                return {
                    "ticker": symbol,
                    "report_period": report_period,
                    "ocf": ocf,
                    "net_income": net_income,
                    "ocf_net_income_ratio": None,
                    "cashflow_api": cashflow_api,
                    "profit_api": profit_api,
                    "status": "error",
                    "source": source_tag,
                    "message": "净利润<=0，不符合策略",
                }

            ratio = ocf / net_income
            return {
                "ticker": symbol,
                "report_period": report_period,
                "ocf": ocf,
                "net_income": net_income,
                "ocf_net_income_ratio": ratio,
                "cashflow_api": cashflow_api,
                "profit_api": profit_api,
                "status": "ok",
                "source": source_tag,
                "message": "success",
            }

        # 现金流字段候选
        ocf_candidates = [
            "经营活动产生的现金流量净额",
            "经营活动现金流量净额",
            "经营现金流量净额",
            "NETCASH_OPERATE",
        ]

        # 净利润字段候选
        net_income_candidates = [
            "净利润",
            "净利润（含少数股东损益）",
            "净利润(含少数股东损益)",
            "归属于母公司股东的净利润",
            "NETPROFIT",
        ]

        # 从两张表提取最新期数据
        ocf, report_cf = extract_latest_value(cashflow_df, ocf_candidates)
        net_income, report_pf = extract_latest_value(profit_df, net_income_candidates)

        # 尽量统一报告期展示
        report_period = report_cf if report_cf is not None else report_pf

        # 校验数据完整性
        if ocf is None or net_income is None:
            return {
                "ticker": symbol,
                "report_period": report_period,
                "ocf": ocf,
                "net_income": net_income,
                "ocf_net_income_ratio": None,
                "cashflow_api": cashflow_api,
                "profit_api": profit_api,
                "status": "error",
                "message": "未找到 OCF 或净利润字段（可能是接口字段变化）",
            }

        # 净利润 <= 0 的公司直接判定为不符合“保守策略”
        if net_income <= 0:
            return {
                "ticker": symbol,
                "report_period": report_period,
                "ocf": ocf,
                "net_income": net_income,
                "ocf_net_income_ratio": None,
                "cashflow_api": cashflow_api,
                "profit_api": profit_api,
                "status": "error",
                "message": "净利润<=0，不符合策略",
            }

        # 计算比率
        ratio = ocf / net_income

        return {
            "ticker": symbol,
            "report_period": report_period,
            "ocf": ocf,
            "net_income": net_income,
            "ocf_net_income_ratio": ratio,
            "cashflow_api": cashflow_api,
            "profit_api": profit_api,
            "status": "ok",
            "source": "akshare",
            "message": "success",
        }

    except ExternalCallTimeout as e:
        return {
            "ticker": symbol,
            "report_period": None,
            "ocf": None,
            "net_income": None,
            "ocf_net_income_ratio": None,
            "cashflow_api": "",
            "profit_api": "",
            "status": "error",
            "source": "timeout",
            "message": str(e),
        }
    except Exception as e:
        return {
            "ticker": symbol,
            "report_period": None,
            "ocf": None,
            "net_income": None,
            "ocf_net_income_ratio": None,
            "cashflow_api": "",
            "profit_api": "",
            "status": "error",
            "source": "unknown",
            "message": f"异常: {e}",
        }


# =============================
# 主流程函数
# =============================
def main() -> int:
    """
    主流程：
    1) 读取 Step 1 的候选文件
    2) 对每只候选股票计算 OCF/NetIncome
    3) 合并回原候选表并过滤
    4) 导出 Step 2 结果
    """
    try:
        # 1) 输入文件存在性检查
        if not os.path.exists(INPUT_CSV):
            raise FileNotFoundError(
                f"未找到输入文件 {INPUT_CSV}。请先运行 step1_screener.py 生成候选列表。"
            )

        # 2) 读取 candidates.csv
        candidates_df = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")
        if candidates_df.empty:
            raise ValueError(f"{INPUT_CSV} 为空，没有可处理的股票。")

        # 3) 识别股票代码列（兼容中文/英文列名）
        ticker_col = find_first_existing_column(candidates_df, ["股票代码", "ticker", "代码"])
        if ticker_col is None:
            raise KeyError(f"{INPUT_CSV} 中未找到股票代码列。当前列：{list(candidates_df.columns)}")

        # 4) 规范化股票代码
        candidates_df["ticker_norm"] = candidates_df[ticker_col].apply(normalize_ticker)

        # 4.1) 加载行业信息并计算每只股票阈值
        industry_map = load_industry_map_from_baostock()
        candidates_df["行业"] = candidates_df["ticker_norm"].map(industry_map).fillna("未知")
        candidates_df["行业阈值"] = candidates_df["行业"].apply(get_threshold_by_industry)

        # 5) 逐只股票计算 OCF/NetIncome
        metrics_rows = []
        total = len(candidates_df)

        for idx, row in candidates_df.iterrows():
            ticker = row["ticker_norm"]
            if idx == 0 or (idx + 1) % 10 == 0 or idx + 1 == total:
                print(f"[INFO] 正在处理财务数据：{idx + 1}/{total} - {ticker}")

            try:
                metrics = run_with_timeout(
                    lambda ticker=ticker: calculate_ocf_net_income_ratio(ticker),
                    PER_TICKER_TIMEOUT_SECONDS,
                    f"step2 ticker {ticker}",
                )
            except ExternalCallTimeout as e:
                metrics = {
                    "ticker": ticker,
                    "report_period": None,
                    "ocf": None,
                    "net_income": None,
                    "ocf_net_income_ratio": None,
                    "cashflow_api": "",
                    "profit_api": "",
                    "status": "error",
                    "source": "timeout",
                    "message": str(e),
                }
            metrics_rows.append(metrics)

            # 降低请求频率，减少触发限流风险
            time.sleep(PER_TICKER_SLEEP_SECONDS)

        metrics_df = pd.DataFrame(metrics_rows)

        # 6.1) 合并行业阈值到指标结果，便于按股过滤
        metrics_df = metrics_df.merge(
            candidates_df[["ticker_norm", "行业", "行业阈值"]],
            left_on="ticker",
            right_on="ticker_norm",
            how="left",
        )
        metrics_df["行业阈值"] = metrics_df["行业阈值"].fillna(OCF_NET_INCOME_THRESHOLD)

        # 6) 只保留计算成功的数据
        ok_df = metrics_df[metrics_df["status"] == "ok"].copy()
        if ok_df.empty:
            remove_output_file()
            print("\n[ERROR] 所有候选股票的财务数据抓取均失败，已拒绝生成空的 Step 2 结果。")
            fail_df = metrics_df[metrics_df["status"] != "ok"].copy()
            if not fail_df.empty:
                print("[INFO] 失败摘要（前10条）：")
                print(fail_df[["ticker", "source", "message"]].head(10).to_string(index=False))
            return 1

        # 7) 应用策略核心过滤：按行业阈值过滤
        ok_df = ok_df[ok_df["ocf_net_income_ratio"] > ok_df["行业阈值"]]

        # 8) 合并回原 candidates，得到“通过 Step 2 的最终结果”
        metric_cols = ["ticker", "report_period", "ocf", "net_income", "ocf_net_income_ratio", "source"]
        merged = candidates_df.merge(
            ok_df[metric_cols],
            left_on="ticker_norm",
            right_on="ticker",
            how="inner",
        )

        # 9) 组织输出列（保留 Step 1 主要字段 + Step 2 新字段）
        preferred_cols = [
            "股票代码", "名称", "现价", "PE(TTM)", "PB", "股息率(%)", "一手成本",
            "行业", "行业阈值", "report_period", "ocf", "net_income", "ocf_net_income_ratio", "source"
        ]

        # 只选择存在的列，避免列缺失报错
        existing_cols = [c for c in preferred_cols if c in merged.columns]
        output_df = merged[existing_cols].copy()

        # 重命名 Step 2 新字段为中文
        output_df = output_df.rename(
            columns={
                "report_period": "报告期",
                "ocf": "经营现金流净额",
                "net_income": "净利润",
                "ocf_net_income_ratio": "OCF/净利润",
                "source": "财务数据源",
                "行业阈值": "OCF阈值",
            }
        )

        # 美化数值显示
        for col in ["经营现金流净额", "净利润", "OCF/净利润"]:
            if col in output_df.columns:
                output_df[col] = pd.to_numeric(output_df[col], errors="coerce").round(4)

        # 10) 导出结果
        write_csv_atomic(output_df, OUTPUT_CSV)
        print(f"[SUCCESS] Step 2 完成，输出文件：{OUTPUT_CSV}，共 {len(output_df)} 条")

        # 11) 输出未通过原因摘要（方便你排查）
        fail_df = metrics_df[metrics_df["status"] != "ok"].copy()
        if not fail_df.empty:
            print("\n[INFO] 以下股票在数据抓取或字段识别时失败（展示前10条）：")
            print(fail_df[["ticker", "message"]].head(10).to_string(index=False))
        return 0

    except Exception as e:
        remove_output_file()
        print("\n[ERROR] Step 2 执行失败：", e)
        print("[ERROR] 详细堆栈：")
        traceback.print_exc()

        # 输出 AkShare 字段/接口更新时的排查建议
        print("\n[提示] 可能是 AkShare 接口字段变化，请按以下方式排查：")
        print("1) 官方文档：https://akshare.akfamily.xyz/")
        print("2) 搜索接口：stock_cash_flow_sheet_by_report_em、stock_profit_sheet_by_report_em")
        print("3) 本地检查函数是否存在：")
        print("   python3 -c \"import akshare as ak; print(hasattr(ak, 'stock_cash_flow_sheet_by_report_em')); print(hasattr(ak, 'stock_profit_sheet_by_report_em'))\"")
        print("4) 打印字段查看真实列名：")
        print("   python3 -c \"import akshare as ak; df=ak.stock_cash_flow_sheet_by_report_em(symbol='600000'); print(df.columns.tolist())\"")
        print("   python3 -c \"import akshare as ak; df=ak.stock_profit_sheet_by_report_em(symbol='600000'); print(df.columns.tolist())\"")
        return 1


# Python 入口
if __name__ == "__main__":
    raise SystemExit(main())
