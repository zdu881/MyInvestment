#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Lixinger 开放 API 适配器（非官方轻封装）
---------------------------------------
用途：
1) 统一处理 token、重试、接口路径兼容
2) 提供“非金融基本面”批量查询能力
3) 返回 pandas.DataFrame，便于与现有 Step1 逻辑衔接
"""

import time
from collections import deque
from typing import Dict, List, Optional

import pandas as pd
import requests

try:
    import lixinger_openapi as lo
except Exception:
    lo = None


class LixingerOpenAPIAdapter:
    """理杏仁 Open API 简易适配器。"""

    def __init__(
        self,
        token: str,
        base_url: str = "https://open.lixinger.com/api",
        timeout: int = 15,
        max_retry: int = 3,
        retry_sleep_seconds: float = 1.0,
        max_requests_per_minute: int = 900,
    ) -> None:
        """
        参数说明：
        - max_requests_per_minute: 本地节流阈值，默认 900/min，留出官方 1000/min 的安全余量。
        """
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retry = max_retry
        self.retry_sleep_seconds = retry_sleep_seconds
        self.max_requests_per_minute = max_requests_per_minute
        self._request_timestamps = deque()

        # SDK 初始化（与 qiansen1386/lixinger-openapi 用法保持一致）
        self._sdk_ready = False
        if lo is not None:
            try:
                lo.set_token(self.token, write_token=False)
                self._sdk_ready = True
            except Exception:
                self._sdk_ready = False

    def _full_url(self, path: str) -> str:
        """把路径标准化为完整 URL。"""
        normalized = path.replace(".", "/").lstrip("/")
        return f"{self.base_url}/{normalized}"

    def _throttle_if_needed(self) -> None:
        """
        本地节流：默认把请求速率控制在每分钟 900 次以内（低于官方 1000 次上限）。
        """
        now = time.time()
        window_start = now - 60

        # 移除一分钟窗口外的请求时间戳
        while self._request_timestamps and self._request_timestamps[0] < window_start:
            self._request_timestamps.popleft()

        # 若已达到本地阈值，则等待到窗口释放
        if len(self._request_timestamps) >= self.max_requests_per_minute:
            wait_seconds = self._request_timestamps[0] + 60 - now
            if wait_seconds > 0:
                time.sleep(wait_seconds)

        self._request_timestamps.append(time.time())

    def _post_json(self, path: str, payload: Dict) -> Dict:
        """POST JSON 请求，带重试。"""
        last_error: Optional[Exception] = None
        for i in range(1, self.max_retry + 1):
            try:
                self._throttle_if_needed()

                response = requests.post(
                    url=self._full_url(path),
                    json=payload,
                    timeout=self.timeout,
                    headers={
                        "Content-Type": "application/json",
                        "Accept-Encoding": "gzip, deflate, br",
                    },
                )

                # 429: 触发频率限制时进行重试退避
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after is not None and str(retry_after).strip().isdigit():
                        sleep_seconds = max(float(retry_after), self.retry_sleep_seconds)
                    else:
                        sleep_seconds = self.retry_sleep_seconds * (2 ** (i - 1))

                    if i < self.max_retry:
                        time.sleep(sleep_seconds)
                        continue
                    response.raise_for_status()

                response.raise_for_status()
                return response.json()
            except Exception as error:
                last_error = error
                if i < self.max_retry:
                    # 网络波动或服务端短暂异常时，做指数退避
                    time.sleep(self.retry_sleep_seconds * (2 ** (i - 1)))

        raise RuntimeError(f"Lixinger 请求失败（path={path}）：{last_error}")

    def _query_json_via_sdk(self, path: str, payload_without_token: Dict) -> Dict:
        """
        通过 qiansen1386/lixinger-openapi 的 query_json 调用接口。
        该 SDK 内部会自动补 token 并将点号路径替换为斜杠。
        """
        if not self._sdk_ready:
            raise RuntimeError("lixinger_openapi SDK 不可用")

        # 复制入参，避免 SDK 内部修改影响外部引用
        request_payload = dict(payload_without_token)
        result = lo.query_json(path, request_payload)
        if not isinstance(result, dict):
            raise RuntimeError("SDK 返回结果不是字典")
        return result

    @staticmethod
    def _is_success_response(result: Dict) -> bool:
        """
        兼容不同版本返回格式的“成功”判断：
        - 旧版常见：code=0, msg=success, data=[...]
        - 新版实测：code=1, message=success, data=[...]
        """
        if not isinstance(result, dict):
            return False
        data = result.get("data")
        if not isinstance(data, list):
            return False
        if len(data) == 0:
            return False

        code = result.get("code")
        message = str(result.get("message") or result.get("msg") or "").lower()

        if message == "success":
            return True
        if code in (0, 1):
            return True
        return False

    def query_non_financial(
        self,
        date_str: str,
        stock_codes: List[str],
        metrics_list: List[str],
        batch_size: int = 60,
    ) -> pd.DataFrame:
        """
        查询“非金融基本面”数据。

        入参：
        - date_str: 查询日期，格式示例 2026-02-12
        - stock_codes: 6位股票代码列表（不带 sh/sz）
        - metrics_list: 指标列表，例如 ["mc", "pe_ttm", "pb"]

        返回：
        - DataFrame，至少包含 stockCode 与 date 列（若接口成功）

        说明：
        - 优先新路径 cn/company/fundamental/non_financial
        - 失败后回退老路径 a/stock/fundamental/non_financial
        """
        payload = {
            "date": date_str,
            "stockCodes": stock_codes,
            "metricsList": metrics_list,
        }

        # 新旧路径自动兼容
        candidate_paths = [
            "cn/company/fundamental/non_financial",
            "a.stock.fundamental.non_financial",
            "a/stock/fundamental/non_financial",
        ]

        def query_one_payload(one_payload: Dict) -> pd.DataFrame:
            # 先走 SDK（你指定仓库的实现方式），再回退 HTTP
            errors: List[str] = []

            for path in candidate_paths:
                try:
                    result = self._query_json_via_sdk(path, one_payload)
                    code = result.get("code", -1)
                    msg = result.get("msg") or result.get("message") or ""
                    data = result.get("data", [])
                    if self._is_success_response(result):
                        return pd.DataFrame(data)
                    errors.append(f"SDK[{path}] code={code}, msg={msg}")
                except Exception as error:
                    errors.append(f"SDK[{path}] {error}")

            payload_with_token = dict(one_payload)
            payload_with_token["token"] = self.token
            for path in candidate_paths:
                try:
                    result = self._post_json(path, payload_with_token)
                    code = result.get("code", -1)
                    msg = result.get("msg") or result.get("message") or ""
                    data = result.get("data", [])
                    if self._is_success_response(result):
                        return pd.DataFrame(data)
                    errors.append(f"HTTP[{path}] code={code}, msg={msg}")
                except Exception as error:
                    errors.append(f"HTTP[{path}] {error}")

            raise RuntimeError(" | ".join(errors[-6:]))

        # 分批查询，避免一次传入股票过多导致接口返回空结果/错误码
        all_frames: List[pd.DataFrame] = []
        all_errors: List[str] = []
        code_list = [str(x).zfill(6) for x in stock_codes]

        for start in range(0, len(code_list), max(1, batch_size)):
            batch_codes = code_list[start : start + max(1, batch_size)]
            batch_payload = {
                "date": date_str,
                "stockCodes": batch_codes,
                "metricsList": metrics_list,
            }
            try:
                frame = query_one_payload(batch_payload)
                if frame is not None and not frame.empty:
                    all_frames.append(frame)
            except Exception as batch_error:
                all_errors.append(f"batch[{start}-{start+len(batch_codes)-1}] {batch_error}")

        if all_frames:
            merged_df = pd.concat(all_frames, axis=0, ignore_index=True)
            if "stockCode" in merged_df.columns:
                merged_df["stockCode"] = merged_df["stockCode"].astype(str).str.zfill(6)
                merged_df = merged_df.drop_duplicates(subset=["stockCode"], keep="last")
            return merged_df

        raise RuntimeError("Lixinger 非金融接口失败：" + " | ".join(all_errors[-6:]))
