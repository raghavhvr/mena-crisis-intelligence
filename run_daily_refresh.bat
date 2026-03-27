@echo off
:: Crisis Pulse — Daily Data Refresh
:: Runs collect.py and pushes updated pulse_data.json to GitHub
:: Scheduled via Windows Task Scheduler at 08:00 daily

:: ── Config — update these paths ──────────────────────────────────────────────
set REPO_PATH=C:\Users\Raghavendra.Reddy\OneDrive - insidemedia.net\Desktop\Code for Thought\crisis-pulse
set PYTHON=python
:: ─────────────────────────────────────────────────────────────────────────────

echo.
echo ============================================
echo  Crisis Pulse — Daily Refresh
echo  %date% %time%
echo ============================================
echo.

:: Navigate to repo
cd /d "%REPO_PATH%"
if errorlevel 1 (
    echo ERROR: Could not find repo folder. Check REPO_PATH in this script.
    pause
    exit /b 1
)

:: Run the collector
echo [1/3] Running data collector...
%PYTHON% scripts\collect.py
if errorlevel 1 (
    echo ERROR: collect.py failed. Check Python installation.
    pause
    exit /b 1
)

:: Stage the updated JSON
echo.
echo [2/3] Staging updated data...
git add public\pulse_data.json
git diff --staged --quiet
if errorlevel 1 (
    :: Only commit if there are actual changes
    git commit -m "chore: daily pulse refresh %date%"
    echo.
    echo [3/3] Pushing to GitHub...
    git push
    echo.
    echo Done! Dashboard will update within 1 minute.
) else (
    echo No changes to commit — data unchanged.
)

echo.
echo ============================================
echo  Finished at %time%
echo ============================================
