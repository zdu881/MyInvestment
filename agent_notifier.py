#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dispatch alert events to ntfy push channel.

Primary source:
- state/alerts_events.jsonl

State files:
- state/notify_cursor.json
- state/notify_dedupe.json
- state/notify_delivery_log.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except Exception:
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(str(value).strip())
    except Exception:
        return default


def parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def read_json(path: Path, default: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if not path.exists():
        return {} if default is None else dict(default)
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
            try:
                row = json.loads(text)
                if isinstance(row, dict):
                    rows.append(row)
            except Exception:
                continue
    return rows


def append_jsonl(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse_csv_set(value: str) -> set[str]:
    return {
        x.strip().lower()
        for x in str(value or "").split(",")
        if x.strip()
    }


def should_send(row: Dict[str, Any], send_events: set[str], send_levels: set[str]) -> Tuple[bool, str]:
    event = str(row.get("event", "")).strip().lower()
    level = str(row.get("level", "")).strip().lower()

    if not event:
        return False, "missing_event"
    if send_events and event not in send_events:
        return False, "event_filtered"
    if level and send_levels and level not in send_levels:
        return False, "level_filtered"
    return True, ""


def dedupe_key(row: Dict[str, Any]) -> str:
    event = str(row.get("event", "")).strip().lower()
    level = str(row.get("level", "")).strip().lower()
    check_id = str(row.get("check_id", "")).strip()
    return f"{event}|{check_id}|{level}"


def in_cooldown(last_sent_iso: str, now_utc: datetime, cooldown_minutes: int) -> bool:
    if cooldown_minutes <= 0:
        return False
    last_dt = parse_iso_datetime(last_sent_iso)
    if last_dt is None:
        return False
    return now_utc < (last_dt.astimezone(timezone.utc) + timedelta(minutes=cooldown_minutes))


def priority_by_level(level: str) -> str:
    normalized = str(level or "").strip().lower()
    if normalized == "critical":
        return "5"
    if normalized == "warn":
        return "4"
    return "2"


def tags_by_level(level: str) -> str:
    normalized = str(level or "").strip().lower()
    if normalized == "critical":
        return "rotating_light,warning"
    if normalized == "warn":
        return "warning"
    return "white_check_mark"


def build_ntfy_payload(row: Dict[str, Any]) -> Tuple[str, str, str, str]:
    event = str(row.get("event", "")).strip().lower() or "event"
    level = str(row.get("level", "")).strip().lower() or "unknown"
    check_id = str(row.get("check_id", "")).strip() or "unknown_check"

    title = f"[MyInvestment][{level.upper()}] {event}: {check_id}"
    lines = [
        f"event={event}",
        f"level={level}",
        f"check_id={check_id}",
        f"value={row.get('value')}",
        f"message={row.get('message', '')}",
        f"source={row.get('source', '')}",
        f"timestamp={row.get('timestamp', '')}",
    ]
    body = "\n".join(lines)
    priority = priority_by_level(level)
    tags = tags_by_level(level)
    return title, body, priority, tags


def send_ntfy(
    base_url: str,
    topic: str,
    title: str,
    body: str,
    priority: str,
    tags: str,
    timeout_sec: float,
) -> str:
    topic_path = quote(str(topic).strip(), safe="")
    url = f"{str(base_url).rstrip('/')}/{topic_path}"
    req = Request(
        url=url,
        data=body.encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            "Title": title,
            "Priority": str(priority),
            "Tags": tags,
        },
    )
    try:
        with urlopen(req, timeout=max(1.0, timeout_sec)) as resp:
            content = resp.read().decode("utf-8", errors="replace")
            return content.strip()
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"URL error: {exc}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dispatch alert events to ntfy")
    parser.add_argument("--enabled", action="store_true")
    parser.add_argument("--state-root", default="state")
    parser.add_argument("--events-path", default="state/alerts_events.jsonl")
    parser.add_argument("--cursor-path", default="state/notify_cursor.json")
    parser.add_argument("--dedupe-path", default="state/notify_dedupe.json")
    parser.add_argument("--delivery-log", default="state/notify_delivery_log.jsonl")
    parser.add_argument("--send-events", default="opened,escalated,resolved")
    parser.add_argument("--send-levels", default="warn,critical")
    parser.add_argument("--cooldown-minutes", type=int, default=30)
    parser.add_argument("--max-events", type=int, default=200)
    parser.add_argument("--ntfy-enabled", action="store_true")
    parser.add_argument("--ntfy-base-url", default="https://ntfy.sh")
    parser.add_argument("--ntfy-topic", default="")
    parser.add_argument("--ntfy-timeout-sec", type=float, default=8.0)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.enabled:
        print("[INFO] notifier is disabled")
        return 0

    if not args.ntfy_enabled:
        print("[INFO] ntfy channel is disabled")
        return 0

    ntfy_topic = str(args.ntfy_topic or os.getenv("MYINVEST_NTFY_TOPIC", "")).strip()
    ntfy_base_url = str(args.ntfy_base_url or os.getenv("MYINVEST_NTFY_BASE_URL", "https://ntfy.sh")).strip()
    if not ntfy_topic:
        print("[ERROR] ntfy topic is required when ntfy channel is enabled")
        return 2

    events_path = Path(args.events_path)
    cursor_path = Path(args.cursor_path)
    dedupe_path = Path(args.dedupe_path)
    delivery_log_path = Path(args.delivery_log)

    cursor = read_json(cursor_path, default={"offset": 0})
    offset = max(0, safe_int(cursor.get("offset"), 0))

    rows = read_jsonl(events_path)
    total_rows = len(rows)

    if offset > total_rows:
        offset = 0

    batch_size = max(1, int(args.max_events))
    start = offset
    end = min(total_rows, start + batch_size)
    batch = rows[start:end]

    send_events = parse_csv_set(args.send_events)
    send_levels = parse_csv_set(args.send_levels)

    dedupe = read_json(dedupe_path, default={"last_sent": {}})
    last_sent = dict(dedupe.get("last_sent", {})) if isinstance(dedupe.get("last_sent", {}), dict) else {}

    now_utc = datetime.now(timezone.utc)
    now_ts = now_utc.isoformat(timespec="seconds")

    delivery_rows: List[Dict[str, Any]] = []
    sent_count = 0
    failed_count = 0
    skipped_count = 0

    for idx, row in enumerate(batch, start=start):
        event = str(row.get("event", "")).strip().lower()
        level = str(row.get("level", "")).strip().lower()
        check_id = str(row.get("check_id", "")).strip()

        ok, reason = should_send(row, send_events, send_levels)
        if not ok:
            skipped_count += 1
            delivery_rows.append(
                {
                    "timestamp": now_ts,
                    "index": idx,
                    "status": "skipped",
                    "reason": reason,
                    "event": event,
                    "level": level,
                    "check_id": check_id,
                }
            )
            continue

        key = dedupe_key(row)
        if in_cooldown(last_sent.get(key, ""), now_utc, int(args.cooldown_minutes)):
            skipped_count += 1
            delivery_rows.append(
                {
                    "timestamp": now_ts,
                    "index": idx,
                    "status": "skipped",
                    "reason": "cooldown",
                    "event": event,
                    "level": level,
                    "check_id": check_id,
                    "dedupe_key": key,
                }
            )
            continue

        title, body, priority, tags = build_ntfy_payload(row)

        if args.dry_run:
            sent_count += 1
            delivery_rows.append(
                {
                    "timestamp": now_ts,
                    "index": idx,
                    "status": "dry_run",
                    "event": event,
                    "level": level,
                    "check_id": check_id,
                    "title": title,
                    "priority": priority,
                    "topic": ntfy_topic,
                }
            )
            continue

        try:
            response_text = send_ntfy(
                base_url=ntfy_base_url,
                topic=ntfy_topic,
                title=title,
                body=body,
                priority=priority,
                tags=tags,
                timeout_sec=float(args.ntfy_timeout_sec),
            )
            sent_count += 1
            last_sent[key] = now_ts
            delivery_rows.append(
                {
                    "timestamp": now_ts,
                    "index": idx,
                    "status": "sent",
                    "event": event,
                    "level": level,
                    "check_id": check_id,
                    "title": title,
                    "priority": priority,
                    "topic": ntfy_topic,
                    "response": response_text,
                }
            )
        except Exception as exc:
            failed_count += 1
            delivery_rows.append(
                {
                    "timestamp": now_ts,
                    "index": idx,
                    "status": "failed",
                    "event": event,
                    "level": level,
                    "check_id": check_id,
                    "title": title,
                    "priority": priority,
                    "topic": ntfy_topic,
                    "error": str(exc),
                }
            )

    append_jsonl(delivery_log_path, delivery_rows)

    write_json(
        cursor_path,
        {
            "offset": end,
            "updated_at": now_ts,
            "events_path": str(events_path),
            "total_rows": total_rows,
            "processed_rows": len(batch),
        },
    )

    # Retain a compact dedupe map.
    dedupe_retained: Dict[str, str] = {}
    retention_cutoff = now_utc - timedelta(days=7)
    for key, sent_ts in last_sent.items():
        dt = parse_iso_datetime(sent_ts)
        if dt is None:
            continue
        if dt.astimezone(timezone.utc) >= retention_cutoff:
            dedupe_retained[key] = sent_ts

    write_json(
        dedupe_path,
        {
            "updated_at": now_ts,
            "cooldown_minutes": int(args.cooldown_minutes),
            "last_sent": dedupe_retained,
        },
    )

    print(
        f"[INFO] notifier processed={len(batch)} sent={sent_count} skipped={skipped_count} failed={failed_count} cursor={end}/{total_rows}"
    )
    return 0 if failed_count == 0 else 3


if __name__ == "__main__":
    raise SystemExit(main())
