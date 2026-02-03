@echo off
setlocal enabledelayedexpansion

rem Switch to script directory
cd /d "%~dp0"

echo ============================================
echo  Solana Auto Trading Runner
echo ============================================
echo.

set "VENV_DIR=.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"

rem Check if virtual environment exists
if not exist "%VENV_PY%" (
    echo Virtual environment not found: %VENV_DIR%
    echo Please run setup.bat first to create the virtual environment.
    echo.
    goto END
)

echo Running auto trading script...
echo Command: "%VENV_PY%" auto.py
echo.

"%VENV_PY%" auto.py

:END
echo.
echo Press Ctrl+C to stop the script.
echo.
pause
endlocal
