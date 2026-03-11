from __future__ import annotations

import json
import subprocess
from pathlib import Path

import document_ingest
from webapi.services.file_repo import FileRepo


MINIMAL_TEXT_PDF = b'''%PDF-1.4
1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj
2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj
3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj
4 0 obj << /Length 44 >> stream
BT /F1 18 Tf 50 90 Td (Hello PDF World) Tj ET
endstream endobj
5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj
xref
0 6
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000241 00000 n 
0000000336 00000 n 
trailer << /Root 1 0 R /Size 6 >>
startxref
406
%%EOF
'''


def _write_config(path: Path, pdf_ingest: dict | None = None) -> Path:
    payload = {
        "paths": {
            "runs_root": "runs",
            "state_root": "state",
            "knowledge_root": "knowledge",
        },
        "pdf_ingest": pdf_ingest or {},
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def test_builtin_pdf_ingest_writes_sidecars(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path / "agent_config.json")
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(MINIMAL_TEXT_PDF)

    ingestor = document_ingest.DocumentIngestor(config_path, tmp_path / "knowledge")
    preview = ingestor.preview_pdf(pdf_path)

    assert preview.content_kind == "markdown"
    assert "Hello PDF World" in preview.preview_text
    assert (preview.cache_dir / "full.md").exists()
    assert (preview.cache_dir / "content_list.json").exists()
    meta = json.loads(preview.meta_path.read_text(encoding="utf-8"))
    assert meta["provider"] == "builtin"
    assert meta["profile"]["is_complex"] is False


def test_complex_pdf_uses_mineru_provider_when_available(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_config(
        tmp_path / "agent_config.json",
        {
            "provider": "mineru",
            "complex_only": True,
        },
    )
    pdf_path = tmp_path / "complex.pdf"
    pdf_path.write_bytes(MINIMAL_TEXT_PDF)

    complex_profile = document_ingest.PdfProfile(
        page_count=20,
        sampled_pages=3,
        sample_text_chars=30,
        average_text_chars_per_page=10,
        sample_image_blocks=4,
        file_size_bytes=2048,
        is_complex=True,
        reasons=["page_count>=12", "image_blocks>=3"],
    )
    monkeypatch.setattr(document_ingest, "inspect_pdf", lambda *args, **kwargs: complex_profile)

    def _fake_run_mineru(self, pdf_path: Path, cache_dir: Path) -> None:
        raw_dir = cache_dir / "mineru_raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "demo.md").write_text("# MinerU Output\n\nStructured result\n", encoding="utf-8")
        (raw_dir / "demo_content_list.json").write_text(
            json.dumps([{"type": "text", "page_idx": 0, "text": "Structured result"}], ensure_ascii=False),
            encoding="utf-8",
        )

    monkeypatch.setattr(document_ingest.DocumentIngestor, "_run_mineru", _fake_run_mineru)

    ingestor = document_ingest.DocumentIngestor(config_path, tmp_path / "knowledge")
    preview = ingestor.preview_pdf(pdf_path)
    meta = json.loads(preview.meta_path.read_text(encoding="utf-8"))

    assert "MinerU Output" in preview.preview_text
    assert meta["provider"] == "mineru"
    assert meta["profile"]["is_complex"] is True


def test_file_repo_reads_pdf_artifact_via_document_ingest(tmp_path: Path) -> None:
    run_dir = tmp_path / "runs" / "2026-03-11" / "demo-run"
    run_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = run_dir / "sample.pdf"
    pdf_path.write_bytes(MINIMAL_TEXT_PDF)

    config_path = _write_config(tmp_path / "agent_config.json")
    repo = FileRepo(
        root_dir=tmp_path,
        runs_root=tmp_path / "runs",
        state_root=tmp_path / "state",
        knowledge_root=tmp_path / "knowledge",
        config_path=config_path,
    )

    kind = repo.artifact_kind(pdf_path)
    content_kind, content = repo.read_artifact_content(run_dir, "sample.pdf")

    assert kind == "pdf"
    assert content_kind == "markdown"
    assert "Hello PDF World" in content
    assert any((tmp_path / "knowledge" / "documents").glob("*/full.md"))


def test_mineru_zero_exit_without_output_falls_back_with_actionable_error(tmp_path: Path, monkeypatch) -> None:
    config_path = _write_config(
        tmp_path / "agent_config.json",
        {
            "provider": "mineru",
            "complex_only": False,
            "mineru_backend": "pipeline",
        },
    )
    pdf_path = tmp_path / "sample.pdf"
    pdf_path.write_bytes(MINIMAL_TEXT_PDF)

    captured: dict[str, object] = {}

    def _fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        raw_dir = Path(cmd[4])
        raw_dir.mkdir(parents=True, exist_ok=True)
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout="",
            stderr="2026-03-11 ERROR parse failed\nModuleNotFoundError: No module named 'torch'\n",
        )

    monkeypatch.setattr(document_ingest.shutil, "which", lambda cmd: f"/mock/bin/{Path(str(cmd)).name}")
    monkeypatch.setattr(document_ingest.subprocess, "run", _fake_run)

    ingestor = document_ingest.DocumentIngestor(config_path, tmp_path / "knowledge")
    preview = ingestor.preview_pdf(pdf_path)
    meta = json.loads(preview.meta_path.read_text(encoding="utf-8"))

    assert meta["provider"] == "builtin_fallback"
    assert meta["sidecar_error"] == "ModuleNotFoundError: No module named 'torch'"
    assert preview.preview_text.startswith("> MinerU fallback: ModuleNotFoundError: No module named 'torch'")
    assert captured["env"]["MINERU_DEVICE_MODE"] == "cpu"
    assert captured["env"]["MINERU_VIRTUAL_VRAM_SIZE"] == "1"
