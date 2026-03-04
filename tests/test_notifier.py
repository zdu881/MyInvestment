from __future__ import annotations

import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def test_notifier_dry_run_updates_cursor_and_log(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    events_path = state_root / "alerts_events.jsonl"
    cursor_path = state_root / "notify_cursor.json"
    dedupe_path = state_root / "notify_dedupe.json"
    delivery_log = state_root / "notify_delivery_log.jsonl"

    _write_jsonl(
        events_path,
        [
            {
                "timestamp": "2026-02-25T12:00:00+08:00",
                "event": "opened",
                "check_id": "q_backlog",
                "level": "warn",
                "value": 2,
                "message": "pending review backlog",
                "source": "runs/ops/ops_report_latest.json",
            },
            {
                "timestamp": "2026-02-25T12:05:00+08:00",
                "event": "escalated",
                "check_id": "q_backlog",
                "level": "critical",
                "value": 4,
                "message": "backlog escalated",
                "source": "runs/ops/ops_report_latest.json",
            },
        ],
    )

    cmd = [
        sys.executable,
        "agent_notifier.py",
        "--enabled",
        "--ntfy-enabled",
        "--dry-run",
        "--state-root",
        str(state_root),
        "--events-path",
        str(events_path),
        "--cursor-path",
        str(cursor_path),
        "--dedupe-path",
        str(dedupe_path),
        "--delivery-log",
        str(delivery_log),
        "--ntfy-topic",
        "my-topic",
    ]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    cursor = _read_json(cursor_path)
    assert cursor["offset"] == 2

    logs = _read_jsonl(delivery_log)
    assert len(logs) == 2
    assert logs[0]["status"] == "dry_run"
    assert logs[1]["status"] == "dry_run"

    # Second run has no new events.
    proc2 = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    assert proc2.returncode == 0, proc2.stdout + proc2.stderr

    logs2 = _read_jsonl(delivery_log)
    assert len(logs2) == 2


class _CaptureHandler(BaseHTTPRequestHandler):
    records: list[dict] = []

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(length).decode("utf-8", errors="replace")
        self.__class__.records.append(
            {
                "path": self.path,
                "title": self.headers.get("Title"),
                "priority": self.headers.get("Priority"),
                "tags": self.headers.get("Tags"),
                "body": payload,
            }
        )
        body = b'{"id":"ok"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args) -> None:  # suppress test noise
        return


def test_notifier_cooldown_deduplicates(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    events_path = state_root / "alerts_events.jsonl"
    cursor_path = state_root / "notify_cursor.json"
    dedupe_path = state_root / "notify_dedupe.json"
    delivery_log = state_root / "notify_delivery_log.jsonl"

    event = {
        "timestamp": "2026-02-25T13:00:00+08:00",
        "event": "opened",
        "check_id": "exec_stale",
        "level": "critical",
        "value": 50.2,
        "message": "oldest pending execution is too old",
        "source": "runs/ops/ops_report_latest.json",
    }
    _write_jsonl(events_path, [event])

    _CaptureHandler.records = []
    server = HTTPServer(("127.0.0.1", 0), _CaptureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        cmd = [
            sys.executable,
            "agent_notifier.py",
            "--enabled",
            "--ntfy-enabled",
            "--state-root",
            str(state_root),
            "--events-path",
            str(events_path),
            "--cursor-path",
            str(cursor_path),
            "--dedupe-path",
            str(dedupe_path),
            "--delivery-log",
            str(delivery_log),
            "--ntfy-base-url",
            base_url,
            "--ntfy-topic",
            "test-topic",
            "--cooldown-minutes",
            "60",
        ]

        first = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
        assert first.returncode == 0, first.stdout + first.stderr
        assert len(_CaptureHandler.records) == 1

        # Append another duplicate event and run again; it should be skipped by cooldown.
        with events_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

        second = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
        assert second.returncode == 0, second.stdout + second.stderr
        assert len(_CaptureHandler.records) == 1

        logs = _read_jsonl(delivery_log)
        assert any(x.get("status") == "sent" for x in logs)
        assert any(x.get("status") == "skipped" and x.get("reason") == "cooldown" for x in logs)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_notifier_reads_topic_from_env(tmp_path: Path, monkeypatch) -> None:
    state_root = tmp_path / "state"
    events_path = state_root / "alerts_events.jsonl"
    cursor_path = state_root / "notify_cursor.json"
    dedupe_path = state_root / "notify_dedupe.json"
    delivery_log = state_root / "notify_delivery_log.jsonl"

    _write_jsonl(
        events_path,
        [
            {
                "timestamp": "2026-02-25T14:00:00+08:00",
                "event": "opened",
                "check_id": "review_backlog",
                "level": "warn",
                "value": 3,
                "message": "pending review backlog",
                "source": "runs/ops/ops_report_latest.json",
            }
        ],
    )

    _CaptureHandler.records = []
    server = HTTPServer(("127.0.0.1", 0), _CaptureHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        monkeypatch.setenv("MYINVEST_NTFY_TOPIC", "env-topic")
        base_url = f"http://127.0.0.1:{server.server_port}"
        cmd = [
            sys.executable,
            "agent_notifier.py",
            "--enabled",
            "--ntfy-enabled",
            "--state-root",
            str(state_root),
            "--events-path",
            str(events_path),
            "--cursor-path",
            str(cursor_path),
            "--dedupe-path",
            str(dedupe_path),
            "--delivery-log",
            str(delivery_log),
            "--ntfy-base-url",
            base_url,
        ]
        proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)

        assert proc.returncode == 0, proc.stdout + proc.stderr
        assert len(_CaptureHandler.records) == 1
        assert _CaptureHandler.records[0]["path"] == "/env-topic"

        logs = _read_jsonl(delivery_log)
        assert any(x.get("status") == "sent" and x.get("topic") == "env-topic" for x in logs)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
