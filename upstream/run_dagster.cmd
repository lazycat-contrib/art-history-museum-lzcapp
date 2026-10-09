@echo off
rem Local Dagster for the museum archive (no Docker). Double-click to start it and open the UI.
rem   run_dagster.cmd [start ^| stop ^| status ^| setup]      (details: dagster\run_dagster.ps1)
rem From Git Bash: ./run_dagster.cmd status
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0dagster\run_dagster.ps1" %*
set rc=%errorlevel%
rem Keep the window open to read the result after an error.
if not "%rc%"=="0" pause
exit /b %rc%
