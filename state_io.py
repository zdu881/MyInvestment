from __future__ import annotations

import json
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import fcntl


class LockTimeoutError(TimeoutError):
    pass


@contextmanager
def advisory_lock(
    lock_path: Path,
    timeout_sec: float = 10.0,
    poll_interval_sec: float = 0.05,
) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        deadline = time.monotonic() + max(0.0, timeout_sec)
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if time.monotonic() >= deadline:
                    raise LockTimeoutError(f"timed out acquiring lock: {lock_path}") from exc
                time.sleep(poll_interval_sec)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def write_text_atomic(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding=encoding) as f:
            f.write(text)
        os.replace(tmp_name, path)
    except Exception:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    write_text_atomic(path, json.dumps(payload, ensure_ascii=False, indent=2))


def write_jsonl_atomic(path: Path, rows: list[dict[str, Any]]) -> None:
    text = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    write_text_atomic(path, text)
