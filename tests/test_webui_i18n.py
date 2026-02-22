from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCALES_DIR = ROOT / "webui" / "static" / "locales"
INDEX_HTML = ROOT / "webui" / "static" / "index.html"
APP_JS = ROOT / "webui" / "static" / "app.js"


def _load_locale(name: str) -> dict[str, str]:
    path = LOCALES_DIR / f"{name}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_locale_key_sets_match() -> None:
    zh = _load_locale("zh-CN")
    en = _load_locale("en-US")
    assert set(zh.keys()) == set(en.keys())


def test_index_html_i18n_keys_exist() -> None:
    zh = _load_locale("zh-CN")
    en = _load_locale("en-US")

    html = INDEX_HTML.read_text(encoding="utf-8")
    keys = set(re.findall(r'data-i18n="([^"]+)"', html))
    keys.update(re.findall(r'data-i18n-placeholder="([^"]+)"', html))

    assert keys
    assert not (keys - set(zh.keys()))
    assert not (keys - set(en.keys()))


def test_app_js_translation_keys_exist() -> None:
    zh = _load_locale("zh-CN")
    en = _load_locale("en-US")

    js = APP_JS.read_text(encoding="utf-8")
    keys = set(re.findall(r"\bt\('([A-Za-z0-9_.]+)'", js))
    keys.update(
        {
            "level.critical",
            "level.warn",
            "level.ok",
            "status.open",
            "status.resolved",
            "event.opened",
            "event.escalated",
            "event.deescalated",
            "event.resolved",
            "event.reminder",
        }
    )

    assert keys
    assert not (keys - set(zh.keys()))
    assert not (keys - set(en.keys()))
