@echo off
REM ============================================================
REM  plc-program — internal helper used by start.bat.
REM  Polls localhost:8766 until the live server is listening,
REM  then opens the dashboard in the default browser.
REM  Not intended to be run directly by operators.
REM ============================================================
setlocal
set URL=http://localhost:8766/report.html?mode=live
set PORT=8766

REM Up to ~15 s of polling (60 * 250 ms). If the server never
REM comes up, we silently give up — the cmd window holding
REM live_server.py will already be showing the failure.
powershell -NoProfile -Command "$t=60; while ($t-- -gt 0) { try { (New-Object Net.Sockets.TcpClient('localhost', %PORT%)).Close(); break } catch { Start-Sleep -Milliseconds 250 } }" >nul 2>&1

start "" "%URL%"
endlocal
