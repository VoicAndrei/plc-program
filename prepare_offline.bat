@echo off
REM ============================================================
REM  plc-program — assemble everything an offline PC needs.
REM
REM  Run this on a Windows machine WITH internet (e.g. the test
REM  PC where you've cloned the repo). It downloads:
REM
REM    - python-3.12.X-amd64.exe   (Python installer, ~30 MB)
REM    - .\wheels\                 (every Python dep as a .whl)
REM
REM  Both are .gitignored, so they stay local. After this runs,
REM  copy the whole project folder onto a USB stick and onto the
REM  offline PC. There:
REM    1. run the python-*-amd64.exe (tick "Add Python to PATH")
REM    2. double-click setup.bat
REM    3. double-click the "PLC Dashboard" desktop shortcut.
REM
REM  Re-run this script after pulling fresh changes that touch
REM  requirements.txt, or whenever you want a newer Python patch.
REM ============================================================
setlocal
cd /d "%~dp0"

REM Pinned Python — must match what setup.bat will run on the
REM offline PC. Bump all of these together if you upgrade. Any
REM 3.12.x patch level is wheel-compatible.
set PY_VER=3.12.7
set PY_TAG=312
set PY_ABI=cp312
set PY_FILE=python-%PY_VER%-amd64.exe
set PY_URL=https://www.python.org/ftp/python/%PY_VER%/%PY_FILE%

where python >nul 2>&1
if errorlevel 1 (
  echo [plc-program] Python not found on PATH on THIS machine.
  echo               Install Python %PY_VER% first so pip can resolve deps.
  pause
  exit /b 1
)

REM ── 1. Python installer ─────────────────────────────────────
if exist "%PY_FILE%" (
  echo [plc-program] %PY_FILE% already present, skipping download.
) else (
  echo [plc-program] downloading %PY_FILE% from python.org...
  powershell -NoProfile -Command "$ProgressPreference='SilentlyContinue'; Invoke-WebRequest -Uri '%PY_URL%' -OutFile '%PY_FILE%'"
  if errorlevel 1 (
    echo [plc-program] Python installer download failed.
    pause
    exit /b 1
  )
)

REM ── 2. Wheels ───────────────────────────────────────────────
if exist wheels rmdir /s /q wheels
mkdir wheels

echo [plc-program] downloading wheels for Python %PY_VER% (cp%PY_TAG%) into .\wheels\
REM Pin the wheels to Python %PY_VER% regardless of the local Python on
REM this machine. Without this, pip downloads wheels matching the local
REM interpreter (e.g. cp314 if you have 3.14 installed), and they silently
REM fail to install on the offline PC's 3.12.
python -m pip download ^
  --only-binary=:all: ^
  --platform win_amd64 ^
  --python-version %PY_TAG% ^
  --implementation cp ^
  --abi %PY_ABI% ^
  -r requirements.txt ^
  -d wheels
if errorlevel 1 (
  echo [plc-program] wheels download failed.
  pause
  exit /b 1
)

REM ── Summary ─────────────────────────────────────────────────
for /f %%C in ('dir /b wheels ^| find /c /v ""') do set N=%%C
echo.
echo [plc-program] ready for offline transport:
echo   - %PY_FILE%         (Python installer)
echo   - .\wheels\                          (%N% .whl files)
echo.
echo Copy this whole folder to a USB stick, then on the offline PC:
echo   1. run %PY_FILE% (tick "Add Python to PATH")
echo   2. double-click setup.bat
echo   3. double-click "PLC Dashboard" on the Desktop.

pause
endlocal
