from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


DEFAULT_PDF_INGEST: Dict[str, Any] = {
    "enabled": True,
    "provider": "none",
    "complex_only": True,
    "cache_root": "knowledge/documents",
    "simple_preview_pages": 6,
    "simple_max_chars": 12000,
    "complex_page_threshold": 12,
    "low_text_chars_per_page": 120,
    "large_file_bytes": 10 * 1024 * 1024,
    "image_block_threshold": 3,
    "mineru_cmd": "mineru",
    "mineru_backend": "pipeline",
    "mineru_method": "auto",
    "mineru_lang": "ch",
    "mineru_timeout_sec": 300,
}


@dataclass(frozen=True)
class PdfIngestConfig:
    enabled: bool
    provider: str
    complex_only: bool
    cache_root: Path
    simple_preview_pages: int
    simple_max_chars: int
    complex_page_threshold: int
    low_text_chars_per_page: int
    large_file_bytes: int
    image_block_threshold: int
    mineru_cmd: str
    mineru_backend: str
    mineru_method: str
    mineru_lang: str
    mineru_timeout_sec: int


@dataclass(frozen=True)
class PdfProfile:
    page_count: int
    sampled_pages: int
    sample_text_chars: int
    average_text_chars_per_page: int
    sample_image_blocks: int
    file_size_bytes: int
    is_complex: bool
    reasons: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "page_count": self.page_count,
            "sampled_pages": self.sampled_pages,
            "sample_text_chars": self.sample_text_chars,
            "average_text_chars_per_page": self.average_text_chars_per_page,
            "sample_image_blocks": self.sample_image_blocks,
            "file_size_bytes": self.file_size_bytes,
            "is_complex": self.is_complex,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class PdfPreview:
    content_kind: str
    preview_text: str
    cache_dir: Path
    meta_path: Path


class DocumentIngestError(RuntimeError):
    """Raised when document ingest cannot produce a safe preview."""


class MinerUUnavailableError(DocumentIngestError):
    """Raised when MinerU provider is requested but not installed."""


class MinerUExecutionError(DocumentIngestError):
    """Raised when MinerU execution fails."""


class DocumentIngestor:
    def __init__(self, config_path: Path, knowledge_root: Path) -> None:
        self.config_path = Path(config_path).resolve()
        self.root_dir = self.config_path.parent
        self.knowledge_root = Path(knowledge_root).resolve()
        self.config = load_pdf_ingest_config(self.config_path, self.knowledge_root)

    def preview_pdf(self, pdf_path: Path) -> PdfPreview:
        path = Path(pdf_path).resolve()
        if not path.exists() or not path.is_file():
            raise FileNotFoundError(str(path))

        profile = inspect_pdf(path, self.config)
        cache_dir = self._cache_dir_for(path)
        meta_path = cache_dir / "meta.json"
        cache_dir.mkdir(parents=True, exist_ok=True)

        provider_used = "builtin"
        sidecar_error = ""
        if self.config.enabled and self._should_use_mineru(profile):
            provider_used = "mineru"
            try:
                self._run_mineru(path, cache_dir)
            except DocumentIngestError as exc:
                sidecar_error = str(exc)
                provider_used = "builtin_fallback"

        if provider_used in {"builtin", "builtin_fallback"}:
            self._write_builtin_sidecars(path, cache_dir, profile)
        else:
            self._normalize_mineru_outputs(cache_dir, profile)

        meta = {
            "source_path": str(path),
            "source_name": path.name,
            "source_sha256": file_sha256(path),
            "provider": provider_used,
            "profile": profile.to_dict(),
            "sidecar_error": sidecar_error,
            "full_md_path": str((cache_dir / "full.md").relative_to(self.root_dir)),
            "content_list_path": str((cache_dir / "content_list.json").relative_to(self.root_dir)),
        }
        write_json(meta_path, meta)

        full_md = cache_dir / "full.md"
        preview_text = full_md.read_text(encoding="utf-8", errors="replace")
        if sidecar_error:
            preview_text = f"> MinerU fallback: {sidecar_error}\n\n{preview_text}"
        return PdfPreview(content_kind="markdown", preview_text=preview_text, cache_dir=cache_dir, meta_path=meta_path)

    def _cache_dir_for(self, pdf_path: Path) -> Path:
        return self.config.cache_root / file_sha256(pdf_path)

    def _should_use_mineru(self, profile: PdfProfile) -> bool:
        if self.config.provider.lower() != "mineru":
            return False
        if self.config.complex_only and not profile.is_complex:
            return False
        return True

    def _run_mineru(self, pdf_path: Path, cache_dir: Path) -> None:
        mineru_cmd = shutil.which(self.config.mineru_cmd)
        if not mineru_cmd:
            raise MinerUUnavailableError(f"MinerU command not found: {self.config.mineru_cmd}")

        raw_dir = cache_dir / "mineru_raw"
        raw_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            mineru_cmd,
            "-p",
            str(pdf_path),
            "-o",
            str(raw_dir),
            "-b",
            self.config.mineru_backend,
            "-m",
            self.config.mineru_method,
        ]
        if self.config.mineru_lang:
            cmd.extend(["-l", self.config.mineru_lang])
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=self.config.mineru_timeout_sec)
        if proc.returncode != 0:
            error = (proc.stderr or proc.stdout or "").strip()
            raise MinerUExecutionError(error or f"mineru exited with code {proc.returncode}")

    def _normalize_mineru_outputs(self, cache_dir: Path, profile: PdfProfile) -> None:
        raw_dir = cache_dir / "mineru_raw"
        md_candidates = [p for p in raw_dir.rglob("*.md") if p.is_file()]
        json_candidates = [p for p in raw_dir.rglob("*_content_list.json") if p.is_file()]
        if not md_candidates:
            raise MinerUExecutionError("MinerU output missing markdown file")

        shutil.copy2(md_candidates[0], cache_dir / "full.md")
        if json_candidates:
            shutil.copy2(json_candidates[0], cache_dir / "content_list.json")
        else:
            write_json(cache_dir / "content_list.json", build_builtin_content_list(cache_dir / "full.md", profile.page_count))

    def _write_builtin_sidecars(self, pdf_path: Path, cache_dir: Path, profile: PdfProfile) -> None:
        markdown_text, content_list = extract_builtin_pdf_markdown(pdf_path, self.config, profile)
        (cache_dir / "full.md").write_text(markdown_text, encoding="utf-8")
        write_json(cache_dir / "content_list.json", content_list)


def load_pdf_ingest_config(config_path: Path, knowledge_root: Path) -> PdfIngestConfig:
    root_dir = Path(config_path).resolve().parent
    payload = read_json(Path(config_path))
    raw = payload.get("pdf_ingest", {}) if isinstance(payload.get("pdf_ingest"), dict) else {}
    merged = dict(DEFAULT_PDF_INGEST)
    merged.update(raw)
    cache_root = Path(str(merged.get("cache_root") or DEFAULT_PDF_INGEST["cache_root"]))
    if not cache_root.is_absolute():
        cache_root = (root_dir / cache_root).resolve()
    if cache_root == (root_dir / DEFAULT_PDF_INGEST["cache_root"]).resolve() and not knowledge_root.name == "documents":
        cache_root = (knowledge_root / "documents").resolve()
    return PdfIngestConfig(
        enabled=_as_bool(merged.get("enabled"), True),
        provider=str(merged.get("provider") or "none").strip().lower(),
        complex_only=_as_bool(merged.get("complex_only"), True),
        cache_root=cache_root,
        simple_preview_pages=max(1, _as_int(merged.get("simple_preview_pages"), 6)),
        simple_max_chars=max(1000, _as_int(merged.get("simple_max_chars"), 12000)),
        complex_page_threshold=max(2, _as_int(merged.get("complex_page_threshold"), 12)),
        low_text_chars_per_page=max(20, _as_int(merged.get("low_text_chars_per_page"), 120)),
        large_file_bytes=max(1024 * 1024, _as_int(merged.get("large_file_bytes"), 10 * 1024 * 1024)),
        image_block_threshold=max(1, _as_int(merged.get("image_block_threshold"), 3)),
        mineru_cmd=str(merged.get("mineru_cmd") or "mineru").strip(),
        mineru_backend=str(merged.get("mineru_backend") or "pipeline").strip(),
        mineru_method=str(merged.get("mineru_method") or "auto").strip(),
        mineru_lang=str(merged.get("mineru_lang") or "ch").strip(),
        mineru_timeout_sec=max(30, _as_int(merged.get("mineru_timeout_sec"), 300)),
    )


def inspect_pdf(pdf_path: Path, config: PdfIngestConfig) -> PdfProfile:
    file_size = pdf_path.stat().st_size
    if PdfReader is None:
        return PdfProfile(
            page_count=0,
            sampled_pages=0,
            sample_text_chars=0,
            average_text_chars_per_page=0,
            sample_image_blocks=0,
            file_size_bytes=file_size,
            is_complex=True,
            reasons=["pypdf_unavailable"],
        )

    reader = PdfReader(str(pdf_path))
    page_count = len(reader.pages)
    sample_pages = min(page_count, config.simple_preview_pages)
    sample_text_chars = 0
    sample_image_blocks = 0
    for idx in range(sample_pages):
        page = reader.pages[idx]
        try:
            sample_text_chars += len((page.extract_text() or "").strip())
        except Exception:
            pass
        try:
            sample_image_blocks += len(list(page.images))
        except Exception:
            pass

    avg_chars = int(sample_text_chars / sample_pages) if sample_pages > 0 else 0
    reasons: List[str] = []
    score = 0
    if page_count >= config.complex_page_threshold:
        reasons.append(f"page_count>={config.complex_page_threshold}")
        score += 1
    if avg_chars <= config.low_text_chars_per_page:
        reasons.append(f"low_text_density<={config.low_text_chars_per_page}")
        score += 1
    if sample_image_blocks >= config.image_block_threshold:
        reasons.append(f"image_blocks>={config.image_block_threshold}")
        score += 1
    if file_size >= config.large_file_bytes:
        reasons.append(f"file_size>={config.large_file_bytes}")
        score += 1
    is_complex = score >= 2 or (avg_chars <= config.low_text_chars_per_page and sample_image_blocks > 0)
    return PdfProfile(
        page_count=page_count,
        sampled_pages=sample_pages,
        sample_text_chars=sample_text_chars,
        average_text_chars_per_page=avg_chars,
        sample_image_blocks=sample_image_blocks,
        file_size_bytes=file_size,
        is_complex=is_complex,
        reasons=reasons,
    )


def extract_builtin_pdf_markdown(
    pdf_path: Path,
    config: PdfIngestConfig,
    profile: Optional[PdfProfile] = None,
) -> tuple[str, List[Dict[str, Any]]]:
    profile = profile or inspect_pdf(pdf_path, config)
    lines = [
        f"# PDF Preview: {pdf_path.name}",
        "",
        f"- pages: {profile.page_count}",
        f"- file_size_bytes: {profile.file_size_bytes}",
        f"- complexity: {'complex' if profile.is_complex else 'simple'}",
        f"- complexity_reasons: {', '.join(profile.reasons) if profile.reasons else 'none'}",
        "",
    ]
    if PdfReader is None:
        lines.append("PDF text extraction is unavailable because `pypdf` is not installed.")
        return "\n".join(lines), []

    reader = PdfReader(str(pdf_path))
    content_list: List[Dict[str, Any]] = []
    current_chars = 0
    preview_pages = min(len(reader.pages), config.simple_preview_pages)
    for page_idx in range(preview_pages):
        page = reader.pages[page_idx]
        text = ""
        try:
            text = (page.extract_text() or "").strip()
        except Exception:
            text = ""
        if not text:
            text = "[No extractable text on this page]"
        remaining = config.simple_max_chars - current_chars
        if remaining <= 0:
            break
        snippet = text[:remaining]
        lines.extend([f"## Page {page_idx + 1}", "", snippet, ""])
        content_list.append(
            {
                "type": "text",
                "page_idx": page_idx,
                "text": snippet,
                "source": "builtin",
            }
        )
        current_chars += len(snippet)
        if current_chars >= config.simple_max_chars:
            lines.append("[Preview truncated due to simple_max_chars limit]")
            break
    return "\n".join(lines).strip() + "\n", content_list


def build_builtin_content_list(full_md_path: Path, page_count: int) -> List[Dict[str, Any]]:
    text = full_md_path.read_text(encoding="utf-8", errors="replace")
    return [
        {
            "type": "text",
            "page_idx": 0 if page_count else -1,
            "text": text,
            "source": "builtin_markdown",
        }
    ]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _as_bool(value: Any, default: bool) -> bool:
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


def _as_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(str(value).strip()))
    except Exception:
        return default
