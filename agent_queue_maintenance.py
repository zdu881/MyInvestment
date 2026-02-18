#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Queue maintenance for review and execution pipelines.

Responsibilities:
- mark pending queue items as stale when waiting too long
- clear stale marks when no longer stale
- archive completed queue items older than retention window
- output a maintenance summary JSON
"""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def now_local(tz_hours: int = 8) -> datetime:
    return datetime.now(timezone(timedelta(hours=tz_hours)))


def parse_iso_datetime(value: Any, default_tz: timezone) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=default_tz)
        return dt
    except Exception:
        return None


def age_hours(ts: Any, now_dt: datetime) -> Optional[float]:
    dt = parse_iso_datetime(ts, now_dt.tzinfo or timezone.utc)
    if dt is None:
        return None
    return (now_dt - dt).total_seconds() / 3600.0


def age_days(ts: Any, now_dt: datetime) -> Optional[float]:
    h = age_hours(ts, now_dt)
    if h is None:
        return None
    return h / 24.0


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


def write_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def maintain_review_queue(
    rows: List[Dict[str, Any]],
    now_dt: datetime,
    stale_hours_threshold: float,
    retain_days: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    archived: List[Dict[str, Any]] = []

    marked_stale = 0
    cleared_stale = 0

    for row in rows:
        status = str(row.get("status", "")).strip().lower()

        if status == "pending":
            created_ts = row.get("timestamp") or row.get("created_at")
            h = age_hours(created_ts, now_dt)
            if h is not None and h >= stale_hours_threshold:
                if not row.get("stale", False):
                    marked_stale += 1
                row["stale"] = True
                row["stale_reason"] = "review_timeout"
                row["stale_hours"] = round(h, 2)
                row["stale_since"] = now_dt.isoformat(timespec="seconds")
            else:
                if row.get("stale", False):
                    cleared_stale += 1
                row.pop("stale", None)
                row.pop("stale_reason", None)
                row.pop("stale_hours", None)
                row.pop("stale_since", None)
            kept.append(row)
            continue

        reviewed_ts = row.get("reviewed_at") or row.get("timestamp")
        d = age_days(reviewed_ts, now_dt)
        if d is not None and d >= retain_days:
            archived.append(row)
        else:
            kept.append(row)

    summary = {
        "input_count": len(rows),
        "kept_count": len(kept),
        "archived_count": len(archived),
        "marked_stale": marked_stale,
        "cleared_stale": cleared_stale,
    }
    return kept, archived, summary


def maintain_execution_queue(
    rows: List[Dict[str, Any]],
    now_dt: datetime,
    stale_hours_threshold: float,
    retain_days: float,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    archived: List[Dict[str, Any]] = []

    marked_stale = 0
    cleared_stale = 0

    for row in rows:
        status = str(row.get("status", "")).strip().lower()

        if status == "pending":
            created_ts = row.get("created_at") or row.get("timestamp")
            h = age_hours(created_ts, now_dt)
            if h is not None and h >= stale_hours_threshold:
                if not row.get("stale", False):
                    marked_stale += 1
                row["stale"] = True
                row["stale_reason"] = "execution_timeout"
                row["stale_hours"] = round(h, 2)
                row["stale_since"] = now_dt.isoformat(timespec="seconds")
            else:
                if row.get("stale", False):
                    cleared_stale += 1
                row.pop("stale", None)
                row.pop("stale_reason", None)
                row.pop("stale_hours", None)
                row.pop("stale_since", None)
            kept.append(row)
            continue

        finished_ts = row.get("executed_at") or row.get("updated_at") or row.get("created_at")
        d = age_days(finished_ts, now_dt)
        if d is not None and d >= retain_days:
            archived.append(row)
        else:
            kept.append(row)

    summary = {
        "input_count": len(rows),
        "kept_count": len(kept),
        "archived_count": len(archived),
        "marked_stale": marked_stale,
        "cleared_stale": cleared_stale,
    }
    return kept, archived, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Queue maintenance")
    parser.add_argument("--state-root", default="state")
    parser.add_argument("--timezone-offset-hours", type=int, default=8)
    parser.add_argument("--review-stale-hours", type=float, default=24.0)
    parser.add_argument("--execution-stale-hours", type=float, default=24.0)
    parser.add_argument("--retain-days", type=float, default=14.0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state_root = Path(args.state_root)
    now_dt = now_local(args.timezone_offset_hours)

    review_queue_path = state_root / "review_queue.jsonl"
    execution_queue_path = state_root / "execution_queue.jsonl"

    review_rows = read_jsonl(review_queue_path)
    execution_rows = read_jsonl(execution_queue_path)

    review_kept, review_archived, review_summary = maintain_review_queue(
        rows=review_rows,
        now_dt=now_dt,
        stale_hours_threshold=args.review_stale_hours,
        retain_days=args.retain_days,
    )
    execution_kept, execution_archived, execution_summary = maintain_execution_queue(
        rows=execution_rows,
        now_dt=now_dt,
        stale_hours_threshold=args.execution_stale_hours,
        retain_days=args.retain_days,
    )

    summary = {
        "run_at": now_dt.isoformat(timespec="seconds"),
        "dry_run": args.dry_run,
        "review": review_summary,
        "execution": execution_summary,
    }

    if not args.dry_run:
        write_jsonl(review_queue_path, review_kept)
        write_jsonl(execution_queue_path, execution_kept)

        archive_root = state_root / "archive"
        append_jsonl(archive_root / "review_queue_archive.jsonl", review_archived)
        append_jsonl(archive_root / "execution_queue_archive.jsonl", execution_archived)

        write_json(state_root / "maintenance_last_run.json", summary)

    if not args.quiet:
        print(f"[INFO] dry_run={args.dry_run}")
        print(
            "[INFO] review: "
            f"input={review_summary['input_count']} "
            f"stale_marked={review_summary['marked_stale']} "
            f"archived={review_summary['archived_count']}"
        )
        print(
            "[INFO] execution: "
            f"input={execution_summary['input_count']} "
            f"stale_marked={execution_summary['marked_stale']} "
            f"archived={execution_summary['archived_count']}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
