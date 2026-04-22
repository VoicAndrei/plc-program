#!/usr/bin/env bash
# plc-program — start the live ingest + dashboard server (macOS / Linux)
set -e
cd "$(dirname "$0")"

if [ ! -x ".venv/bin/python" ]; then
  echo "[plc-program] .venv missing — running setup.sh first"
  ./setup.sh
fi

echo "[plc-program] starting live server on http://localhost:8766"
echo "[plc-program] open http://localhost:8766/report.html?mode=live"
exec .venv/bin/python live_server.py
