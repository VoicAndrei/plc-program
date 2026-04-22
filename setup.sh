#!/usr/bin/env bash
# plc-program — one-time macOS/Linux setup
set -e
cd "$(dirname "$0")"

if ! command -v python3 >/dev/null 2>&1; then
  echo "[plc-program] python3 not found. Install Python 3.10+ first." >&2
  exit 1
fi

python3 -m venv .venv
if [ -d wheels ]; then
  echo "[plc-program] using local wheels folder (offline mode)"
  .venv/bin/pip install --no-index --find-links wheels -r requirements.txt
else
  .venv/bin/pip install -r requirements.txt
fi

echo
echo "[plc-program] setup complete."
echo "  1. Edit connection.yaml — set endpoint to your PLC"
echo "  2. (Optional) export PLC_USER=... PLC_PASS=..."
echo "  3. ./run.sh"
echo "  4. Open http://localhost:8766/report.html?mode=live"
