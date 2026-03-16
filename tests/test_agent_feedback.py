from __future__ import annotations

import json
from pathlib import Path

import agent_feedback
import agent_system


def test_synthesize_feedback_declares_objective_only_learning_policy() -> None:
    result = agent_feedback.synthesize_feedback(
        [
            {
                "timestamp": "2026-03-10T20:30:00+08:00",
                "quality_score": 0.72,
                "cost_ratio": 0.001,
                "tickers": ["600519"],
                "risk_flags": ["存在监管类负面舆情，需复核公告"],
            }
        ]
    )

    assert result["learning_policy"] == "objective_execution_only"
    assert result["human_review_signals_included"] is False
    assert "execution_history" in result["sources_used"]
    assert "human_decision" in result["sources_excluded"]


def test_build_markdown_includes_feedback_learning_policy() -> None:
    summary = {
        "generated_at": "2026-03-16T10:00:00",
        "window_start": "2026-03-01",
        "window_end": "2026-03-16",
        "sample_size": 1,
        "avg_quality_score": 0.7,
        "avg_cost_ratio": 0.001,
        "model_feedback": {
            "learning_policy": "objective_execution_only",
            "human_review_signals_included": False,
            "sources_used": ["execution_history"],
            "sources_excluded": ["human_decision", "review_note"],
            "min_confidence_buy": 0.72,
            "max_new_positions_override": 2,
            "ticker_penalties": {},
            "risk_flag_penalties": {},
        },
        "quality_rows": [],
    }

    markdown = agent_feedback.build_markdown(summary)

    assert "## Learning Policy" in markdown
    assert "objective_execution_only" in markdown
    assert "human_review_signals_included: False" in markdown


def test_agent_system_ignores_feedback_marked_as_human_style_learning(tmp_path: Path) -> None:
    config_path = tmp_path / "agent_config.json"
    config_path.write_text(
        json.dumps(
            {
                "paths": {"runs_root": "runs", "state_root": "state"},
                "feedback": {"default_min_confidence_buy": 0.75},
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    state_root = tmp_path / "state"
    state_root.mkdir(parents=True, exist_ok=True)
    (state_root / "model_feedback.json").write_text(
        json.dumps(
            {
                "learning_policy": "human_style_mimic",
                "human_review_signals_included": True,
                "min_confidence_buy": 0.55,
                "max_new_positions_override": 3,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    system = agent_system.AgentSystem(str(config_path))

    assert system._load_model_feedback() == {}
