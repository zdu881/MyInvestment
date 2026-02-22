from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass
class CommandResult:
    command: list[str]
    exit_code: int
    stdout_tail: str
    stderr_tail: str

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


def run_command(command: list[str], cwd: Path, timeout_sec: int = 120) -> CommandResult:
    proc = subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        timeout=timeout_sec,
    )
    return CommandResult(
        command=command,
        exit_code=proc.returncode,
        stdout_tail=(proc.stdout or "")[-4000:],
        stderr_tail=(proc.stderr or "")[-4000:],
    )
