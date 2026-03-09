from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest

ROOT = Path(__file__).resolve().parents[1]
VIEWER_TOKEN = "viewer-token"


def _build_test_workspace(tmp_path: Path) -> Path:
    runs_src = ROOT / "runs"
    state_src = ROOT / "state"
    knowledge_src = ROOT / "knowledge"
    if runs_src.exists():
        shutil.copytree(runs_src, tmp_path / "runs")
    else:
        (tmp_path / "runs").mkdir(parents=True, exist_ok=True)
    if state_src.exists():
        shutil.copytree(state_src, tmp_path / "state")
    else:
        (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    if knowledge_src.exists():
        shutil.copytree(knowledge_src, tmp_path / "knowledge")
    else:
        (tmp_path / "knowledge").mkdir(parents=True, exist_ok=True)
    shutil.copytree(ROOT / "webui", tmp_path / "webui")
    shutil.copy2(ROOT / "agent_config.json", tmp_path / "agent_config.json")
    return tmp_path


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _wait_for_server(base_url: str, proc: subprocess.Popen[str], timeout_sec: float = 20.0) -> None:
    deadline = time.time() + timeout_sec
    last_error: Exception | None = None
    while time.time() < deadline:
        if proc.poll() is not None:
            output = ""
            if proc.stdout is not None:
                output = proc.stdout.read()
            raise RuntimeError(f"uvicorn exited before ready: {output}")
        try:
            with urlopen(f"{base_url}/health", timeout=1.0) as resp:
                if resp.status == 200:
                    return
        except URLError as exc:
            last_error = exc
        except OSError as exc:
            last_error = exc
        time.sleep(0.2)

    raise RuntimeError(f"server did not become ready in {timeout_sec}s: {last_error}")


def _start_server(workspace: Path, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["MYINVEST_ROOT"] = str(workspace)
    env["MYINVEST_VIEWER_TOKEN"] = VIEWER_TOKEN
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "webapi.main:create_app",
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _stop_server(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


@pytest.mark.e2e
def test_webui_language_switch_persists(tmp_path: Path) -> None:
    playwright_sync_api = pytest.importorskip("playwright.sync_api")
    sync_playwright = playwright_sync_api.sync_playwright
    PlaywrightError = playwright_sync_api.Error

    workspace = _build_test_workspace(tmp_path)
    port = _pick_free_port()
    base_url = f"http://127.0.0.1:{port}"

    proc = _start_server(workspace, port)
    try:
        _wait_for_server(base_url, proc)

        with sync_playwright() as playwright:
            try:
                browser = playwright.chromium.launch(headless=True)
            except PlaywrightError as exc:
                pytest.skip(f"playwright browser is unavailable: {exc}")

            context = browser.new_context()
            page = context.new_page()

            page.goto(base_url, wait_until="domcontentloaded")
            page.wait_for_selector("#languageSelect")
            page.evaluate(
                "([locale, token]) => {"
                "window.localStorage.setItem('myinvestment_locale', locale);"
                "window.localStorage.setItem('myinvestment_api_token', token);"
                "}",
                ["zh-CN", VIEWER_TOKEN],
            )
            page.reload(wait_until="domcontentloaded")
            page.wait_for_selector("#languageSelect")

            assert page.locator("#saveTokenBtn").inner_text().strip() == "保存 Token"
            assert page.locator("button.tab[data-view='action-center']").inner_text().strip() == "行动中心"

            page.select_option("#languageSelect", "en-US")
            page.wait_for_function(
                "() => document.querySelector('#saveTokenBtn')?.textContent?.trim() === 'Save Token'"
            )

            assert page.locator("#saveTokenBtn").inner_text().strip() == "Save Token"
            assert page.locator("button.tab[data-view='action-center']").inner_text().strip() == "Action Center"

            page.reload(wait_until="domcontentloaded")
            page.wait_for_function(
                "() => document.querySelector('#saveTokenBtn')?.textContent?.trim() === 'Save Token'"
            )
            assert page.locator("#saveTokenBtn").inner_text().strip() == "Save Token"

            browser.close()
    finally:
        _stop_server(proc)
