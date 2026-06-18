from __future__ import annotations

from pathlib import Path

from webapi.main import ExecutionSubmitRequest


ROOT = Path(__file__).resolve().parents[1]
OPENAPI = ROOT / "docs" / "webui" / "openapi.yaml"


def _schema_block(text: str, schema_name: str) -> str:
    lines = text.splitlines()
    marker = f"    {schema_name}:"
    start = lines.index(marker)
    block: list[str] = []
    for line in lines[start:]:
        if block and line.startswith("    ") and not line.startswith("      ") and line.strip().endswith(":"):
            break
        block.append(line)
    return "\n".join(block)


def test_openapi_execution_submit_request_matches_runtime_model() -> None:
    text = OPENAPI.read_text(encoding="utf-8")
    block = _schema_block(text, "ExecutionSubmitRequest")

    for field_name in ExecutionSubmitRequest.model_fields:
        assert f"        {field_name}:" in block

    assert "        confirm_manual_fill:" in block
    assert "          default: false" in block


def test_openapi_artifact_item_exposes_read_token() -> None:
    text = OPENAPI.read_text(encoding="utf-8")
    block = _schema_block(text, "ArtifactItem")

    assert "required: [name, path, artifact, kind, size]" in block
    assert "        artifact:" in block
    assert "          description: Relative path to use with /api/runs/{runId}/artifact-content" in block
