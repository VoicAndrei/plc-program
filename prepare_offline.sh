#!/usr/bin/env bash
# ============================================================
#  plc-program — pre-download Windows wheels for offline deploy.
#
#  Run this on ANY machine with internet (macOS/Linux/Windows).
#  Produces a ./wheels directory containing every Python
#  dependency (and its transitive deps) as Windows-compatible
#  wheels. Then copy the whole project folder, this wheels
#  directory, and a matching python-X.Y.Z-amd64.exe installer
#  onto a USB stick. On the offline Windows PC:
#
#    1. install Python (same minor version as targeted below),
#    2. run setup.bat — it auto-detects ./wheels and installs
#       with --no-index, no internet required.
#
#  Usage:
#    ./prepare_offline.sh           # defaults to Python 3.12
#    ./prepare_offline.sh 3.11      # target a different version
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"

PYM="${1:-3.12}"
PYV="${PYM/./}"   # 3.12 -> 312
ABI="cp${PYV}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "[plc-program] python3 not found on PATH. Install Python first." >&2
  exit 1
fi

echo "[plc-program] target: Python ${PYM} on Windows x86_64"
echo "[plc-program] cleaning ./wheels and re-downloading..."
rm -rf wheels
mkdir -p wheels

python3 -m pip download \
  --only-binary=:all: \
  --platform win_amd64 \
  --python-version "${PYV}" \
  --implementation cp \
  --abi "${ABI}" \
  -r requirements.txt \
  -d wheels

echo
echo "[plc-program] downloaded $(ls -1 wheels | wc -l | tr -d ' ') wheels:"
ls -1 wheels
echo
echo "[plc-program] next:"
echo "  1. Download the matching Python installer:"
echo "       https://www.python.org/ftp/python/${PYM}.0/python-${PYM}.0-amd64.exe"
echo "     (any 3.${PYV:1} patch version is fine; tick 'Add to PATH')."
echo "  2. Copy this folder (including ./wheels and the .exe) to a USB stick."
echo "  3. On the offline Windows PC: run the .exe, then double-click setup.bat."
