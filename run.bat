@echo off
REM ============================================================
REM  plc-program — start the live ingest + dashboard server
REM
REM  Prereqs (one-time setup, see README.md):
REM    1. Install Python 3.10+ from python.org (tick "Add to PATH")
REM    2. In this folder, run: setup.bat
REM    3. Edit connection.yaml — set endpoint to your PLC's URL
REM    4. (Optional) If the PLC needs auth, set env vars:
REM         set PLC_USER=<username>
REM         set PLC_PASS=<password>
REM ============================================================
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
  echo [plc-program] virtual env not found — run setup.bat first.
  pause
  exit /b 1
)

echo [plc-program] starting live server on http://localhost:8766
echo [plc-program] open http://localhost:8766/report.html?mode=live
echo [plc-program] Ctrl+C to stop
echo.
".venv\Scripts\python.exe" live_server.py

pause
