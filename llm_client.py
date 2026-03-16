from __future__ import annotations

import json
import os
from pathlib import Path
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

import requests


DEFAULT_PROVIDER = "siliconflow"
DEFAULT_BASE_URL = "https://api.siliconflow.cn/v1/chat/completions"
DEFAULT_MODEL = "Pro/zai-org/GLM-5"
DEFAULT_MAX_TOKENS = 320
VERDICT_ORDER = {"buy": 0, "observe": 1, "avoid": 2}


class LLMClientError(RuntimeError):
    """Raised when the configured LLM provider cannot return a usable result."""


@dataclass(frozen=True)
class LLMRuntimeConfig:
    enabled: bool
    provider: str
    base_url: str
    model: str
    timeout_sec: float
    temperature: float
    max_tokens: int
    api_key_env: str
    env_file: str

    @classmethod
    def from_runtime_config(
        cls,
        config: Optional[Dict[str, Any]] = None,
        environ: Optional[Mapping[str, str]] = None,
        *,
        base_dir: Optional[str | Path] = None,
    ) -> "LLMRuntimeConfig":
        payload = config or {}
        env = environ or os.environ
        env_file_raw = str(payload.get("env_file") or env.get("MYINVEST_LLM_ENV_FILE") or ".env.local").strip()
        env_file = ""
        if env_file_raw:
            env_path = Path(env_file_raw)
            if not env_path.is_absolute():
                env_path = ((Path(base_dir) if base_dir is not None else Path.cwd()) / env_path).resolve()
            env_file = str(env_path)
        return cls(
            enabled=_as_bool(env.get("MYINVEST_LLM_ENABLED", payload.get("enabled", True)), default=True),
            provider=str(payload.get("provider") or env.get("MYINVEST_LLM_PROVIDER") or DEFAULT_PROVIDER).strip(),
            base_url=str(env.get("SILICONFLOW_BASE_URL") or payload.get("base_url") or DEFAULT_BASE_URL).strip(),
            model=str(env.get("SILICONFLOW_MODEL") or payload.get("model") or DEFAULT_MODEL).strip(),
            timeout_sec=_as_float(env.get("SILICONFLOW_TIMEOUT_SEC") or payload.get("timeout_sec"), 45.0),
            temperature=_as_float(env.get("SILICONFLOW_TEMPERATURE") or payload.get("temperature"), 0.2),
            max_tokens=_as_int(env.get("SILICONFLOW_MAX_TOKENS") or payload.get("max_tokens"), DEFAULT_MAX_TOKENS),
            api_key_env=str(payload.get("api_key_env") or "SILICONFLOW_API_KEY").strip() or "SILICONFLOW_API_KEY",
            env_file=env_file,
        )

    def api_key(self, environ: Optional[Mapping[str, str]] = None) -> str:
        if environ is not None:
            return str(environ.get(self.api_key_env, "")).strip()

        env = dict(os.environ)
        if self.env_file:
            for key, value in _load_env_file(Path(self.env_file)).items():
                env.setdefault(key, value)
        return str(env.get(self.api_key_env, "")).strip()

    def is_available(self, environ: Optional[Mapping[str, str]] = None) -> bool:
        return bool(self.enabled and self.api_key(environ=environ))


def enhance_stock_research_summary(
    *,
    ticker: str,
    name: str,
    tools: Dict[str, Any],
    base_summary: Dict[str, Any],
    runtime_config: LLMRuntimeConfig,
) -> Dict[str, Any]:
    """Use LLM to refine per-ticker research while keeping conservative guardrails."""
    if not runtime_config.enabled:
        raise LLMClientError("LLM integration disabled by config")
    if not runtime_config.api_key():
        raise LLMClientError(f"LLM api key missing: env {runtime_config.api_key_env}")

    response = _chat_completion(
        runtime_config=runtime_config,
        messages=_build_stock_research_messages(
            ticker=ticker,
            name=name,
            tools=tools,
            base_summary=base_summary,
        ),
    )
    parsed = _extract_json_payload(response["content"])
    llm_summary = _normalize_llm_summary(parsed, fallback=base_summary)
    merged = _merge_research_summary(base_summary=base_summary, llm_summary=llm_summary)
    merged["analysis_mode"] = "llm_assisted"
    merged["llm"] = {
        "provider": runtime_config.provider,
        "model": str(response.get("model") or runtime_config.model),
        "usage": response.get("usage") or {},
    }
    rationale = llm_summary.get("llm_rationale")
    if rationale:
        merged["llm_rationale"] = rationale
    return merged


def _chat_completion(runtime_config: LLMRuntimeConfig, messages: Sequence[Dict[str, str]]) -> Dict[str, Any]:
    payload = {
        "model": runtime_config.model,
        "messages": list(messages),
        "temperature": runtime_config.temperature,
        "max_tokens": runtime_config.max_tokens,
    }
    headers = {
        "Authorization": f"Bearer {runtime_config.api_key()}",
        "Content-Type": "application/json",
    }
    response = requests.post(
        runtime_config.base_url,
        headers=headers,
        json=payload,
        timeout=runtime_config.timeout_sec,
    )
    try:
        data = response.json()
    except Exception:
        body_preview = (response.text or "")[:300]
        raise LLMClientError(f"LLM provider returned non-JSON response (status={response.status_code}): {body_preview}")

    if response.status_code >= 400:
        error = data.get("error") if isinstance(data, dict) else data
        raise LLMClientError(f"LLM provider error (status={response.status_code}): {error}")

    choices = data.get("choices") or []
    if not choices:
        raise LLMClientError("LLM provider returned empty choices")
    message = choices[0].get("message") or {}
    content = str(message.get("content") or "").strip()
    if not content:
        raise LLMClientError("LLM provider returned empty content")
    return {
        "content": content,
        "model": data.get("model") or runtime_config.model,
        "usage": data.get("usage") or {},
    }


def _build_stock_research_messages(
    *,
    ticker: str,
    name: str,
    tools: Dict[str, Any],
    base_summary: Dict[str, Any],
) -> List[Dict[str, str]]:
    evidence = {
        "ticker": ticker,
        "name": name,
        "base_summary": {
            "thesis": list(base_summary.get("thesis", [])),
            "risk_flags": list(base_summary.get("risk_flags", [])),
            "confidence": base_summary.get("confidence"),
            "verdict": base_summary.get("verdict"),
            "abstain_reason": base_summary.get("abstain_reason"),
            "missing_evidence": list(base_summary.get("missing_evidence", [])),
            "reentry_triggers": list(base_summary.get("reentry_triggers", [])),
        },
        "tool_evidence": tools,
    }
    system_prompt = (
        "你是 A 股防御型投资研究助手。"
        "你只能基于给定证据做判断，不得杜撰。"
        "输出必须是 JSON 对象，不要 markdown，不要额外解释。"
        "JSON schema: {\"thesis\": [str], \"risk_flags\": [str], \"confidence\": number, "
        "\"verdict\": \"buy|observe|avoid\", \"llm_rationale\": str, \"abstain_reason\": str, "
        "\"missing_evidence\": [str], \"reentry_triggers\": [str]}."
        "要求：thesis 最多 3 条，risk_flags 最多 4 条，missing_evidence 最多 3 条，"
        "reentry_triggers 最多 3 条，confidence 介于 0 到 1，结论要保守，输出尽量简洁。"
    )
    user_prompt = (
        "请基于以下证据，生成该股票的研究摘要。"
        "先判断是否值得交易，再补充买入逻辑与硬风险。"
        "如果证据不足，请降低 confidence，并明确说明为什么此刻更适合不交易、还缺什么证据、什么条件下才允许重入。\n"
        f"{json.dumps(evidence, ensure_ascii=False, sort_keys=True)}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def _extract_json_payload(content: str) -> Dict[str, Any]:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise LLMClientError("LLM response does not contain a valid JSON object")
        try:
            payload = json.loads(text[start : end + 1])
        except Exception as exc:
            raise LLMClientError(f"LLM JSON parse failed: {exc}")
    if not isinstance(payload, dict):
        raise LLMClientError("LLM response JSON must be an object")
    return payload


def _normalize_llm_summary(payload: Dict[str, Any], fallback: Dict[str, Any]) -> Dict[str, Any]:
    thesis = _clean_string_list(payload.get("thesis"), limit=3) or list(fallback.get("thesis", []))[:3]
    risk_flags = _clean_string_list(payload.get("risk_flags"), limit=4) or list(fallback.get("risk_flags", []))[:4]
    confidence = _clamp_float(payload.get("confidence"), _as_float(fallback.get("confidence"), 0.0))
    verdict = str(payload.get("verdict") or fallback.get("verdict") or "observe").strip().lower()
    if verdict not in VERDICT_ORDER:
        verdict = str(fallback.get("verdict") or "observe").strip().lower()
        if verdict not in VERDICT_ORDER:
            verdict = "observe"
    rationale = str(payload.get("llm_rationale") or payload.get("rationale") or "").strip()
    abstain_reason = str(payload.get("abstain_reason") or fallback.get("abstain_reason") or "").strip()
    missing_evidence = _clean_string_list(payload.get("missing_evidence"), limit=3) or list(
        fallback.get("missing_evidence", [])
    )[:3]
    reentry_triggers = _clean_string_list(payload.get("reentry_triggers"), limit=3) or list(
        fallback.get("reentry_triggers", [])
    )[:3]
    return {
        "thesis": thesis,
        "risk_flags": risk_flags,
        "confidence": round(confidence, 2),
        "verdict": verdict,
        "llm_rationale": rationale,
        "abstain_reason": abstain_reason,
        "missing_evidence": missing_evidence,
        "reentry_triggers": reentry_triggers,
    }


def _merge_research_summary(base_summary: Dict[str, Any], llm_summary: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base_summary)
    merged["thesis"] = _merge_unique(llm_summary.get("thesis", []), base_summary.get("thesis", []), limit=3)
    merged["risk_flags"] = _merge_unique(base_summary.get("risk_flags", []), llm_summary.get("risk_flags", []), limit=4)
    merged["confidence"] = round(
        min(_as_float(base_summary.get("confidence"), 0.0), _as_float(llm_summary.get("confidence"), 0.0)),
        2,
    )
    merged["verdict"] = _more_conservative_verdict(
        str(base_summary.get("verdict") or "observe"),
        str(llm_summary.get("verdict") or "observe"),
    )
    merged["abstain_reason"] = str(
        llm_summary.get("abstain_reason") or base_summary.get("abstain_reason") or ""
    ).strip()
    merged["missing_evidence"] = _merge_unique(
        llm_summary.get("missing_evidence", []),
        base_summary.get("missing_evidence", []),
        limit=3,
    )
    merged["reentry_triggers"] = _merge_unique(
        llm_summary.get("reentry_triggers", []),
        base_summary.get("reentry_triggers", []),
        limit=3,
    )
    return merged


def _more_conservative_verdict(left: str, right: str) -> str:
    a = left.strip().lower()
    b = right.strip().lower()
    if a not in VERDICT_ORDER:
        a = "observe"
    if b not in VERDICT_ORDER:
        b = "observe"
    return a if VERDICT_ORDER[a] >= VERDICT_ORDER[b] else b


def _clean_string_list(value: Any, limit: int) -> List[str]:
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, Iterable):
        items = list(value)
    else:
        return []
    cleaned: List[str] = []
    seen = set()
    for item in items:
        text = str(item).strip()
        if not text:
            continue
        if text not in seen:
            seen.add(text)
            cleaned.append(text)
        if len(cleaned) >= limit:
            break
    return cleaned


def _load_env_file(path: Path) -> Dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}

    values: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def _merge_unique(primary: Sequence[str], secondary: Sequence[str], *, limit: int) -> List[str]:
    items: List[str] = []
    seen = set()
    for source in (primary, secondary):
        for item in source:
            text = str(item).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            items.append(text)
            if len(items) >= limit:
                return items
    return items


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _as_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(str(value).strip())
    except Exception:
        return default


def _as_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(str(value).strip()))
    except Exception:
        return default


def _clamp_float(value: Any, default: float) -> float:
    try:
        numeric = float(value)
    except Exception:
        numeric = default
    return max(0.0, min(1.0, numeric))
