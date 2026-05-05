@echo off
REM ============================================================
REM  plc-program — operator launcher
REM
REM  Starts live_server.py and opens the dashboard in the default
REM  browser. The cmd window IS the server: closing it stops
REM  ingestion. If the server is already running, this just opens
REM  a fresh dashboard tab and exits.
REM
REM  Wired up by create_shortcut.bat into a Desktop shortcut named
REM  "PLC Dashboard".
REM ============================================================
setlocal
cd /d "%~dp0"

set URL=http://localhost:8766/report.html?mode=live
set PORT=8766

if not exist ".venv\Scripts\python.exe" (
  echo [plc-program] virtual env not found. Run setup.bat first.
  pause
  exit /b 1
)

REM Already running? Just pop a tab and bail.
powershell -NoProfile -Command "try { (New-Object Net.Sockets.TcpClient('localhost', %PORT%)).Close(); exit 0 } catch { exit 1 }" >nul 2>&1
if not errorlevel 1 (
  echo [plc-program] server already running on port %PORT%. Opening dashboard.
  start "" "%URL%"
  exit /b 0
)

REM Spawn the browser-opener in the background; it polls the port
REM and launches the default browser once the server is listening.
start "" /B cmd /c ""%~dp0_open_browser.bat""

echo [plc-program] starting live server on http://localhost:%PORT%
echo [plc-program] the dashboard opens automatically when ready.
echo [plc-program] CLOSE THIS WINDOW to stop the server.
echo.
".venv\Scripts\python.exe" live_server.py

endlocal
