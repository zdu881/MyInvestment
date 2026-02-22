# Web API Development Notes

## Run API locally

```bash
./scripts/start_webui.sh
```

Optional environment variables:

```bash
export MYINVEST_ROOT=/data/home/sim6g/MyInvestment
export MYINVEST_API_TOKEN=your_token
```

If `MYINVEST_API_TOKEN` is set, mutating endpoints require header `X-API-Token`.
Web UI is served at:

```bash
http://localhost:8787/
```

## Run tests

Use the provided wrapper to avoid external pytest plugin conflicts in this environment:

```bash
./scripts/run_tests.sh
```

Optional browser E2E for language switch:

```bash
pip install playwright
python -m playwright install chromium
./scripts/run_webui_e2e.sh
```

If Playwright is not installed, this E2E test is auto-skipped by pytest.

## End-to-end acceptance flow

1. Open `http://localhost:8787/`.
2. In `Action Center`, confirm pending reviews / executions are visible.
3. In `Proposal Review`, select one pending run and submit `hold` or `approve`.
4. In `Execution Queue`, select one pending execution and submit `dry-run` execute.
5. In `State & Config`, apply a small patch (for example `{\"action_center\":{\"max_alerts\":6}}`) and verify config refresh.
6. Check `state/webui_audit_log.jsonl` contains the above actions.
