#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Automatic promotion pipeline for skill candidates.

It aggregates knowledge/skill_candidates.jsonl and maintains a versioned
knowledge/skills_registry.csv.
"""

import argparse
import csv
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple


REGISTRY_COLUMNS = [
    "skill_name",
    "skill_key",
    "pattern_type",
    "title",
    "version",
    "status",
    "created_from_run",
    "last_validated_at",
    "owner",
    "rollback_version",
    "quality_score",
    "evidence_count",
]


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        text = str(value).strip().replace(",", "")
        if text in {"", "-", "--", "None", "nan", "NaN"}:
            return default
        return float(text)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except Exception:
        return default


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_registry(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = [dict(row) for row in reader]
    normalized: List[Dict[str, str]] = []
    for row in rows:
        out = {k: str(row.get(k, "") or "") for k in REGISTRY_COLUMNS}
        if not out["skill_key"]:
            pattern = str(row.get("pattern_type", "") or "")
            title = str(row.get("title", "") or row.get("skill_name", "") or "")
            out["skill_key"] = build_skill_key(pattern, title)
        if not out["version"]:
            out["version"] = "1"
        normalized.append(out)
    return normalized


def write_registry(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=REGISTRY_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in REGISTRY_COLUMNS})


def build_skill_key(pattern_type: str, title: str) -> str:
    return f"{pattern_type.strip().lower()}|{title.strip().lower()}"


def build_skill_name(pattern_type: str, title: str) -> str:
    base = build_skill_key(pattern_type, title)
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:10]
    prefix = (pattern_type.strip().lower() or "skill").replace(" ", "_")
    return f"{prefix}_{digest}"


def aggregate_candidates(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    agg: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        pattern_type = str(row.get("pattern_type", "")).strip() or "generic_rule"
        title = str(row.get("title", "")).strip() or "untitled_skill"
        key = build_skill_key(pattern_type, title)

        if key not in agg:
            agg[key] = {
                "skill_key": key,
                "pattern_type": pattern_type,
                "title": title,
                "impact_scores": [],
                "run_ids": set(),
                "trading_dates": set(),
                "latest_run_id": "",
                "latest_created_at": "",
                "evidence_count": 0,
            }

        item = agg[key]
        item["impact_scores"].append(safe_float(row.get("impact_score"), 0.5))
        run_id = str(row.get("run_id", "")).strip()
        if run_id:
            item["run_ids"].add(run_id)
            item["latest_run_id"] = run_id
        trading_date = str(row.get("trading_date", "")).strip()
        if trading_date:
            item["trading_dates"].add(trading_date)
        created_at = str(row.get("created_at", "")).strip()
        if created_at and created_at >= str(item["latest_created_at"]):
            item["latest_created_at"] = created_at
        item["evidence_count"] += safe_int(row.get("evidence_count"), 1)

    for key, item in agg.items():
        unique_runs = len(item["run_ids"])
        unique_days = len(item["trading_dates"])
        avg_impact = (
            sum(item["impact_scores"]) / len(item["impact_scores"])
            if item["impact_scores"]
            else 0.5
        )
        frequency_score = min(1.0, unique_runs / 5.0)
        diversity_score = min(1.0, unique_days / 4.0)
        quality_score = 0.50 * avg_impact + 0.35 * frequency_score + 0.15 * diversity_score
        item["unique_run_count"] = unique_runs
        item["unique_day_count"] = unique_days
        item["avg_impact_score"] = round(avg_impact, 4)
        item["quality_score"] = round(quality_score, 4)
        item["skill_name"] = build_skill_name(item["pattern_type"], item["title"])
        item["run_ids"] = sorted(item["run_ids"])
        item["trading_dates"] = sorted(item["trading_dates"])
    return agg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote skill candidates into registry")
    parser.add_argument("--knowledge-root", default="knowledge")
    parser.add_argument("--min-occurrences", type=int, default=3)
    parser.add_argument("--quality-threshold", type=float, default=0.62)
    parser.add_argument("--promote-delta", type=float, default=0.08)
    parser.add_argument("--owner", default="auto_agent")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now_ts = datetime.now().isoformat(timespec="seconds")
    knowledge_root = Path(args.knowledge_root)
    candidates_path = knowledge_root / "skill_candidates.jsonl"
    registry_path = knowledge_root / "skills_registry.csv"
    history_path = knowledge_root / "skills_registry_history.jsonl"
    summary_path = knowledge_root / "skill_promotion_last_run.json"

    candidates = read_jsonl(candidates_path)
    aggregates = aggregate_candidates(candidates)
    registry_rows = read_registry(registry_path)

    # keep an index to mutable row references
    by_key: Dict[str, List[Dict[str, str]]] = {}
    for row in registry_rows:
        by_key.setdefault(row["skill_key"], []).append(row)

    actions: List[Dict[str, Any]] = []
    for key, item in sorted(aggregates.items()):
        unique_runs = safe_int(item.get("unique_run_count"), 0)
        quality_score = safe_float(item.get("quality_score"), 0.0)
        if unique_runs < int(args.min_occurrences):
            continue
        if quality_score < float(args.quality_threshold):
            continue

        rows_for_key = by_key.get(key, [])
        active_row = None
        if rows_for_key:
            active_candidates = [
                r for r in rows_for_key if str(r.get("status", "")).strip().lower() == "active"
            ]
            if active_candidates:
                active_row = sorted(active_candidates, key=lambda r: safe_int(r.get("version"), 1))[-1]
            else:
                active_row = sorted(rows_for_key, key=lambda r: safe_int(r.get("version"), 1))[-1]

        if active_row is None:
            new_row = {
                "skill_name": item["skill_name"],
                "skill_key": key,
                "pattern_type": str(item.get("pattern_type", "")),
                "title": str(item.get("title", "")),
                "version": "1",
                "status": "active",
                "created_from_run": str(item.get("latest_run_id", "")),
                "last_validated_at": now_ts,
                "owner": args.owner,
                "rollback_version": "",
                "quality_score": f"{quality_score:.4f}",
                "evidence_count": str(safe_int(item.get("evidence_count"), 0)),
            }
            registry_rows.append(new_row)
            by_key.setdefault(key, []).append(new_row)
            actions.append(
                {
                    "action": "promoted_new",
                    "timestamp": now_ts,
                    "skill_key": key,
                    "skill_name": new_row["skill_name"],
                    "version": 1,
                    "quality_score": quality_score,
                    "evidence_count": safe_int(item.get("evidence_count"), 0),
                    "run_id": str(item.get("latest_run_id", "")),
                }
            )
            continue

        active_version = safe_int(active_row.get("version"), 1)
        active_quality = safe_float(active_row.get("quality_score"), 0.0)
        active_evidence = safe_int(active_row.get("evidence_count"), 0)
        new_evidence = safe_int(item.get("evidence_count"), 0)

        if (
            quality_score >= active_quality + float(args.promote_delta)
            and new_evidence >= active_evidence + 1
        ):
            active_row["status"] = "superseded"
            active_row["rollback_version"] = str(active_version)
            active_row["last_validated_at"] = now_ts

            new_row = {
                "skill_name": item["skill_name"],
                "skill_key": key,
                "pattern_type": str(item.get("pattern_type", "")),
                "title": str(item.get("title", "")),
                "version": str(active_version + 1),
                "status": "active",
                "created_from_run": str(item.get("latest_run_id", "")),
                "last_validated_at": now_ts,
                "owner": args.owner,
                "rollback_version": str(active_version),
                "quality_score": f"{quality_score:.4f}",
                "evidence_count": str(new_evidence),
            }
            registry_rows.append(new_row)
            by_key.setdefault(key, []).append(new_row)
            actions.append(
                {
                    "action": "promoted_upgrade",
                    "timestamp": now_ts,
                    "skill_key": key,
                    "skill_name": new_row["skill_name"],
                    "from_version": active_version,
                    "to_version": active_version + 1,
                    "quality_score": quality_score,
                    "evidence_count": new_evidence,
                    "run_id": str(item.get("latest_run_id", "")),
                }
            )
        else:
            active_row["last_validated_at"] = now_ts
            active_row["quality_score"] = f"{max(active_quality, quality_score):.4f}"
            active_row["evidence_count"] = str(max(active_evidence, new_evidence))
            actions.append(
                {
                    "action": "revalidated",
                    "timestamp": now_ts,
                    "skill_key": key,
                    "skill_name": str(active_row.get("skill_name", "")),
                    "version": safe_int(active_row.get("version"), 1),
                    "quality_score": max(active_quality, quality_score),
                    "evidence_count": max(active_evidence, new_evidence),
                    "run_id": str(item.get("latest_run_id", "")),
                }
            )

    # stable ordering for diff readability
    registry_rows = sorted(
        registry_rows,
        key=lambda r: (str(r.get("skill_key", "")), safe_int(r.get("version"), 1)),
    )

    summary = {
        "generated_at": now_ts,
        "dry_run": args.dry_run,
        "candidate_count": len(candidates),
        "aggregate_count": len(aggregates),
        "eligible_count": len(
            [
                1
                for item in aggregates.values()
                if safe_int(item.get("unique_run_count"), 0) >= int(args.min_occurrences)
                and safe_float(item.get("quality_score"), 0.0) >= float(args.quality_threshold)
            ]
        ),
        "registry_size": len(registry_rows),
        "actions_count": len(actions),
        "actions": actions[:100],
    }

    if not args.dry_run:
        if not registry_path.exists():
            registry_path.parent.mkdir(parents=True, exist_ok=True)
        write_registry(registry_path, registry_rows)
        append_jsonl(history_path, actions)
        write_json(summary_path, summary)

    print(f"[INFO] candidate_count={summary['candidate_count']}")
    print(f"[INFO] aggregate_count={summary['aggregate_count']}")
    print(f"[INFO] eligible_count={summary['eligible_count']}")
    print(f"[INFO] actions_count={summary['actions_count']}")
    if not args.dry_run:
        print(f"[INFO] registry={registry_path}")
        print(f"[INFO] history={history_path}")
        print(f"[INFO] summary={summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
