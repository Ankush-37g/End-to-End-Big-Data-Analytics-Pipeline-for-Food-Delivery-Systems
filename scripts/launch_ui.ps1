# ============================================================
# FeastFlow Big Data UI Launcher (PowerShell)
# ============================================================

$ProjectDir = (Get-Item "$PSScriptRoot\..").FullName
Set-Location $ProjectDir

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  FeastFlow Analytics UI Demo Launcher" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Opening dashboard at http://localhost:8000 ..." -ForegroundColor Green
Write-Host "  Press Ctrl+C to stop the server." -ForegroundColor Yellow
Write-Host "============================================"

Start-Process "http://localhost:8000"
python -m http.server 8000 --directory "$ProjectDir\web"
