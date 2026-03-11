from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from document_ingest import DocumentIngestor


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(str(value).strip())
    except Exception:
        return default


@dataclass
class FileRepo:
    root_dir: Path
    runs_root: Path
    state_root: Path
    knowledge_root: Path
    config_path: Path

    def read_json(self, path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.exists():
            return {} if default is None else default
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                rows.append(json.loads(text))
        return rows

    def append_jsonl(self, path: Path, row: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def read_text(self, path: Path, default: str = "") -> str:
        if not path.exists():
            return default
        return path.read_text(encoding="utf-8")

    def read_csv(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        rows: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(dict(row))
        return rows

    def find_run_dir(self, run_id: str) -> Path | None:
        if not run_id:
            return None
        matches = sorted(self.runs_root.glob(f"*/{run_id}"))
        if not matches:
            return None
        return matches[-1]

    def list_run_manifests(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        if not self.runs_root.exists():
            return items
        for date_dir in sorted(self.runs_root.glob("*")):
            if not date_dir.is_dir() or date_dir.name == "ops":
                continue
            for run_dir in sorted(date_dir.glob("*")):
                if not run_dir.is_dir():
                    continue
                manifest_path = run_dir / "run_manifest.json"
                if not manifest_path.exists():
                    continue
                manifest = self.read_json(manifest_path, default={})
                if not manifest:
                    continue
                items.append(manifest)
        items.sort(key=lambda x: str(x.get("as_of_ts", "")), reverse=True)
        return items

    @staticmethod
    def artifact_kind(path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix == ".json":
            return "json"
        if suffix == ".jsonl":
            return "jsonl"
        if suffix == ".csv":
            return "csv"
        if suffix == ".md":
            return "markdown"
        if suffix == ".pdf":
            return "pdf"
        return "text"

    def list_run_artifacts(self, run_dir: Path, manifest: dict[str, Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        artifacts = manifest.get("artifacts", [])
        if isinstance(artifacts, list) and artifacts:
            candidates = []
            for artifact in artifacts:
                rel = Path(str(artifact))
                # Artifacts are usually persisted as absolute-like workspace paths.
                path = self.root_dir / rel
                if not path.exists():
                    # Fallback to file name within run dir.
                    path = run_dir / rel.name
                if path.exists():
                    candidates.append(path)
        else:
            candidates = [p for p in run_dir.glob("*") if p.is_file()]

        for path in sorted(set(candidates)):
            try:
                size = path.stat().st_size
            except FileNotFoundError:
                continue
            items.append(
                {
                    "name": path.name,
                    "path": str(path.relative_to(self.root_dir)),
                    "kind": self.artifact_kind(path),
                    "size": int(size),
                }
            )
        return items

    def read_artifact_content(self, run_dir: Path, artifact: str) -> tuple[str, str]:
        if not artifact or artifact.startswith("/"):
            raise ValueError("invalid artifact path")
        rel = Path(artifact)
        if ".." in rel.parts:
            raise ValueError("invalid artifact path")
        path = (run_dir / rel).resolve()
        run_root = run_dir.resolve()
        try:
            path.relative_to(run_root)
        except ValueError as exc:
            raise ValueError("artifact out of run dir") from exc

        if not path.exists() or not path.is_file():
            raise FileNotFoundError(str(path))
        kind = self.artifact_kind(path)
        if kind == "pdf":
            preview = DocumentIngestor(self.config_path, self.knowledge_root).preview_pdf(path)
            return preview.content_kind, preview.preview_text
        text = path.read_text(encoding="utf-8", errors="replace")
        return kind, text

    def pending_review_items(self) -> list[dict[str, Any]]:
        rows = self.read_jsonl(self.state_root / "review_queue.jsonl")
        items = [r for r in rows if str(r.get("status", "")).strip().lower() == "pending"]
        items.sort(key=lambda x: str(x.get("timestamp", "")))
        return items

    def pending_execution_items(self) -> list[dict[str, Any]]:
        rows = self.read_jsonl(self.state_root / "execution_queue.jsonl")
        items = [r for r in rows if str(r.get("status", "")).strip().lower() == "pending"]
        items.sort(key=lambda x: str(x.get("created_at", "")))
        return items

    def get_review_queue_item(self, run_id: str) -> dict[str, Any] | None:
        rows = self.read_jsonl(self.state_root / "review_queue.jsonl")
        rows = [r for r in rows if str(r.get("run_id", "")) == run_id]
        if not rows:
            return None
        rows.sort(key=lambda x: str(x.get("timestamp", "")), reverse=True)
        return rows[0]

    def has_pending_review(self, run_id: str) -> bool:
        return any(str(r.get("run_id", "")) == run_id for r in self.pending_review_items())

    def has_pending_execution(self, run_id: str) -> bool:
        return any(str(r.get("run_id", "")) == run_id for r in self.pending_execution_items())

    def load_proposal_bundle(self, run_id: str) -> dict[str, Any] | None:
        run_dir = self.find_run_dir(run_id)
        if run_dir is None:
            return None
        proposal_path = run_dir / "allocation_proposal.json"
        if not proposal_path.exists():
            return None
        proposal = self.read_json(proposal_path, default={})
        queue_item = self.get_review_queue_item(run_id) or {}
        advice_report = self.read_text(run_dir / "advice_report.md", default="")
        research_rows = self.read_jsonl(run_dir / "stock_research.jsonl")
        rebalance_actions = self.read_csv(run_dir / "rebalance_actions.csv")

        for row in rebalance_actions:
            for key in ("current_weight", "target_weight", "delta_weight"):
                row[key] = _safe_float(row.get(key), 0.0)

        return {
            "run_id": run_id,
            "proposal": proposal,
            "review_queue_item": queue_item,
            "advice_report": advice_report,
            "research_rows": research_rows,
            "rebalance_actions": rebalance_actions,
        }
