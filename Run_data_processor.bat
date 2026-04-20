@echo off
chcp 65001 >nul
title Sino Iron - Data Processor

set "CURRENT_DIR=%~dp0"
set "LAUNCHER=%CURRENT_DIR%_launcher_processor.py"

if not exist "%LAUNCHER%" (
    echo Error: _launcher_processor.py not found at %LAUNCHER%
    pause
    exit /b 1
)

call conda activate citic
if %errorlevel% neq 0 (
    echo Error: Failed to activate conda environment 'citic'
    pause
    exit /b 1
)

cd /d "%CURRENT_DIR%"
python "%LAUNCHER%"

if %errorlevel% neq 0 (
    echo.
    echo Launcher exited with error code %errorlevel%
    pause
)
