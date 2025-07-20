@echo off
echo Starting Company Data Finder Backend...
echo =====================================

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python 3.8+ and try again
    pause
    exit /b 1
)

REM Install dependencies if needed
echo Installing/checking dependencies...
pip install -r requirements.txt

REM Initialize database and start server
echo Starting server...
python start_server.py

pause
