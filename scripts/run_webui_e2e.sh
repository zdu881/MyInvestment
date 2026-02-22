#!/usr/bin/env bash
set -euo pipefail

export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
pytest -q tests/test_webui_e2e_playwright.py "$@"
