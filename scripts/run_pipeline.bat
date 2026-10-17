@echo off
REM ============================================================
REM End-to-End Pipeline Runner (Windows Batch)
REM ============================================================

set PROJECT_DIR=%~dp0..
cd /d "%PROJECT_DIR%"

set PHASE=%1
if "%PHASE%"=="" set PHASE=all

echo ============================================
echo   Food Delivery Big Data Pipeline (Windows)
echo ============================================
echo   Project Directory: %PROJECT_DIR%
echo   Executing Phase:   %PHASE%
echo ============================================

powershell -ExecutionPolicy Bypass -File "%PROJECT_DIR%\scripts\run_pipeline.ps1" -Phase %PHASE%

exit /b %ERRORLEVEL%
