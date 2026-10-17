@echo off
REM ============================================================
REM FeastFlow Big Data UI Launcher
REM Launches local web server and opens the interactive dashboard
REM ============================================================

set PROJECT_DIR=%~dp0..
cd /d "%PROJECT_DIR%"

echo ============================================
echo   FeastFlow Analytics UI Demo Launcher
echo ============================================
echo   Opening dashboard at http://localhost:8000 ...
echo   Press Ctrl+C in this window to stop the server.
echo ============================================

start "" "http://localhost:8000"
python -m http.server 8000 --directory web
