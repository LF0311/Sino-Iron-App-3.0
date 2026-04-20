@echo off
echo ==========================================
echo Sino Iron - Ore Tracking and Prediction App Runner
echo ==========================================

REM Get current directory path of the bat file
set "CURRENT_DIR=%~dp0"
echo Current directory: %CURRENT_DIR%

REM Check if App_0_0_6_1.py file exists
set "APP_FILE=%CURRENT_DIR%App_0_0_7_1.py"
if not exist "%APP_FILE%" (
    echo Error: App_0_0_7_1.py file not found!
    echo Please ensure the file is located at: %APP_FILE%
    pause
    exit /b 1
)

echo Found application file: %APP_FILE%

REM Activate anaconda environment and run streamlit
echo Activating anaconda environment 'citic'...
call conda activate citic
if %errorlevel% neq 0 (
    echo Error: Failed to activate anaconda environment 'citic'!
    echo Please ensure Anaconda is properly installed and the 'citic' environment exists.
    pause
    exit /b 1
)

echo Environment activated successfully!
echo Starting Streamlit application...
echo.

REM Switch to application directory and run streamlit
cd /d "%CURRENT_DIR%"
streamlit run App_0_0_7_1.py

REM If streamlit fails to run, display error message
if %errorlevel% neq 0 (
    echo.
    echo Error: Streamlit failed to run!
    echo Please check:
    echo 1. Is streamlit installed in the 'citic' environment
    echo 2. Does App_0_0_7_1.py file have syntax errors
    echo 3. Are all required dependencies installed
    pause
    exit /b 1
)

echo.
echo Application has been closed.
pause