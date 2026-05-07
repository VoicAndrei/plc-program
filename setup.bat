@echo off
REM ============================================================
REM  plc-program — one-time Windows setup
REM
REM  Creates a local .venv and installs dependencies from
REM  requirements.txt. If this laptop has no internet access at
REM  the plant, drop a 'wheels' folder in this directory first
REM  (see README.md → Offline install).
REM ============================================================
cd /d "%~dp0"

where python >nul 2>&1
if errorlevel 1 (
  echo [plc-program] Python not found on PATH.
  echo               Install Python 3.10 or newer from https://python.org
  echo               and make sure "Add to PATH" is ticked.
  pause
  exit /b 1
)

REM Always start from a fresh .venv. If the project folder was copied
REM from another machine (e.g. via USB), the leftover .venv contains
REM site-packages built for THAT machine's Python — including ABI-tagged
REM .pyd files that won't load here. Nuking it forces pip to install
REM matching binaries from .\wheels\.
if exist .venv (
  echo [plc-program] removing stale .venv from prior install...
  rmdir /s /q .venv
)

echo [plc-program] creating virtual env (.venv)...
python -m venv .venv
if errorlevel 1 (
  echo [plc-program] failed to create venv.
  pause
  exit /b 1
)

echo [plc-program] installing dependencies...
if exist "wheels\" (
  echo [plc-program] using local wheels folder (offline mode)
  ".venv\Scripts\python.exe" -m pip install --no-index --find-links wheels -r requirements.txt
) else (
  ".venv\Scripts\python.exe" -m pip install -r requirements.txt
)

if errorlevel 1 (
  echo [plc-program] dependency install failed.
  pause
  exit /b 1
)

echo.
echo [plc-program] dependencies installed. Creating desktop shortcut...
call "%~dp0create_shortcut.bat" --silent

echo.
echo [plc-program] setup complete. Next steps:
echo   1. Edit connection.yaml — set endpoint to your PLC
echo   2. (Optional) Set PLC_USER and PLC_PASS env vars
echo   3. Double-click "PLC Dashboard" on the Desktop.
echo      (It starts the server and opens the browser automatically.)
echo.
pause
