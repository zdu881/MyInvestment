from __future__ import annotations

import json
from pathlib import Path

import pytest

import agent_system
import llm_client


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload, ensure_ascii=False)

    def json(self) -> dict:
        return self._payload


def test_enhance_stock_research_summary_merges_conservatively(monkeypatch) -> None:
    monkeypatch.setenv("SILICONFLOW_API_KEY", "test-key")

    def _fake_post(url, headers, json, timeout):
        assert url == "https://api.siliconflow.cn/v1/chat/completions"
        assert headers["Authorization"] == "Bearer test-key"
        assert json["model"] == "Pro/zai-org/GLM-5"
        return _FakeResponse(
            200,
            {
                "model": "Pro/zai-org/GLM-5",
                "usage": {"total_tokens": 123},
                "choices": [
                    {
                        "message": {
                            "content": json_module.dumps(
                                {
                                    "thesis": ["现金流改善", "估值仍有安全边际"],
                                    "risk_flags": ["存在监管类负面舆情，需复核公告"],
                                    "confidence": 0.58,
                                    "verdict": "observe",
                                    "llm_rationale": "证据偏谨慎，先观察。",
                                },
                                ensure_ascii=False,
                            )
                        }
                    }
                ],
            },
        )

    json_module = json
    monkeypatch.setattr(llm_client.requests, "post", _fake_post)

    config = llm_client.LLMRuntimeConfig.from_runtime_config({"enabled": True})
    base_summary = {
        "ticker": "600519",
        "thesis": ["近三期财务波动可控"],
        "risk_flags": [],
        "confidence": 0.67,
        "verdict": "buy",
        "tool_evidence": {},
        "analysis_mode": "heuristic",
    }

    result = llm_client.enhance_stock_research_summary(
        ticker="600519",
        name="贵州茅台",
        tools={"health": {"ok": True, "data": {}}, "sentiment": {"ok": True, "data": {}}, "ah": {"ok": False, "data": {}}},
        base_summary=base_summary,
        runtime_config=config,
    )

    assert result["analysis_mode"] == "llm_assisted"
    assert result["verdict"] == "observe"
    assert result["confidence"] == 0.58
    assert result["thesis"][0] == "现金流改善"
    assert result["llm"]["model"] == "Pro/zai-org/GLM-5"
    assert result["llm_rationale"] == "证据偏谨慎，先观察。"


def test_agent_system_falls_back_when_llm_errors(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "agent_config.json"
    config_path.write_text(
        json.dumps(
            {
                "paths": {"runs_root": "runs", "state_root": "state"},
                "llm": {"enabled": True},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    system = agent_system.AgentSystem(str(config_path))

    def _raise_llm_error(**kwargs):
        raise agent_system.LLMClientError("provider unavailable")

    monkeypatch.setattr(agent_system, "enhance_stock_research_summary", _raise_llm_error)

    summary = system._derive_research_summary(
        "600519",
        tools={
            "health": {"ok": True, "data": {"big_fluctuation": False, "ocf_growth_pct": 10.0, "risk_flags": []}},
            "sentiment": {"ok": True, "data": {"risk_score": 0.0, "categories": [], "negative_events": []}},
            "ah": {"ok": False, "data": {}},
        },
        name="贵州茅台",
        dry_run=False,
    )

    assert summary["analysis_mode"] == "heuristic"
    assert summary["verdict"] in {"buy", "observe"}
    assert summary["llm"]["error"] == "provider unavailable"


def test_llm_client_requires_api_key(tmp_path: Path) -> None:
    config = llm_client.LLMRuntimeConfig.from_runtime_config(
        {"enabled": True, "env_file": ""},
        environ={},
        base_dir=tmp_path,
    )
    with pytest.raises(llm_client.LLMClientError):
        llm_client.enhance_stock_research_summary(
            ticker="600519",
            name="贵州茅台",
            tools={},
            base_summary={"thesis": [], "risk_flags": [], "confidence": 0.5, "verdict": "observe"},
            runtime_config=config,
        )


def test_llm_client_loads_api_key_from_env_file(tmp_path: Path) -> None:
    env_file = tmp_path / ".env.local"
    env_file.write_text("SILICONFLOW_API_KEY=file-key\n", encoding="utf-8")

    config = llm_client.LLMRuntimeConfig.from_runtime_config(
        {"enabled": True, "env_file": ".env.local"},
        base_dir=tmp_path,
    )

    assert config.api_key() == "file-key"
    assert config.is_available() is True
