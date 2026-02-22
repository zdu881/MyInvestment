#!/usr/bin/env bash
set -euo pipefail

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8787}"

uvicorn webapi.main:app --host "$HOST" --port "$PORT" --reload
