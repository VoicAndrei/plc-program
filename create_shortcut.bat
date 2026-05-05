@echo off
REM ============================================================
REM  plc-program — drop a "PLC Dashboard" shortcut on the
REM  current user's Desktop pointing at start.bat.
REM
REM  Run with --silent to suppress the trailing pause (used when
REM  setup.bat chains this in at the end).
REM ============================================================
setlocal
set HERE=%~dp0
if "%HERE:~-1%"=="\" set HERE=%HERE:~0,-1%

powershell -NoProfile -ExecutionPolicy Bypass -Command "$ws = New-Object -COM WScript.Shell; $lnk = $ws.CreateShortcut(([Environment]::GetFolderPath('Desktop')) + '\PLC Dashboard.lnk'); $lnk.TargetPath = '%HERE%\start.bat'; $lnk.WorkingDirectory = '%HERE%'; $lnk.IconLocation = 'shell32.dll,13'; $lnk.Description = 'Start the PLC ingest server and open the dashboard.'; $lnk.Save()"

if errorlevel 1 (
  echo [plc-program] failed to create the desktop shortcut.
  if /i not "%1"=="--silent" pause
  exit /b 1
)

echo [plc-program] desktop shortcut created: PLC Dashboard
echo [plc-program] double-click it to start the server and open the dashboard.

if /i not "%1"=="--silent" pause
endlocal
