@echo off
REM ============================================================
REM  plc-program — pre-download Windows wheels for offline deploy.
REM
REM  Run this on a Windows machine WITH internet. It downloads
REM  every Python dependency (and its transitive deps) into
REM  .\wheels\ for the *currently installed* Python. Then copy
REM  the whole project folder onto a USB stick alongside the
REM  matching python-X.Y.Z-amd64.exe installer. On the offline
REM  PC, run the .exe, then double-click setup.bat — it
REM  auto-detects .\wheels\ and installs with --no-index, no
REM  internet required.
REM ============================================================
setlocal
cd /d "%~dp0"

where python >nul 2>&1
if errorlevel 1 (
  echo [plc-program] Python not found on PATH. Install it first.
  pause
  exit /b 1
)

if exist wheels rmdir /s /q wheels
mkdir wheels

echo [plc-program] downloading wheels for the local Python into .\wheels\
python -m pip download --only-binary=:all: -r requirements.txt -d wheels
if errorlevel 1 (
  echo [plc-program] download failed.
  pause
  exit /b 1
)

echo.
for /f %%C in ('dir /b wheels ^| find /c /v ""') do set N=%%C
echo [plc-program] downloaded %N% wheels.
echo [plc-program] copy this folder (and a matching python-X.Y.Z-amd64.exe
echo               installer) to a USB stick. On the offline PC:
echo                 1. run the .exe (tick "Add Python to PATH"),
echo                 2. double-click setup.bat.
echo               The offline PC must use the same Python minor version
echo               (e.g. 3.12) as this machine, otherwise the wheels won't
echo               match.

pause
endlocal
