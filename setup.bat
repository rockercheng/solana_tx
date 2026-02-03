@echo off
chcp 65001 >nul 2>&1
setlocal enabledelayedexpansion

rem Switch to script directory
cd /d "%~dp0"

rem Set environment variables for UTF-8 encoding
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo ============================================
echo  Solana Auto Trading Setup Script
echo ============================================
echo.

rem Define local Python 3.10 path
set "LOCAL_PY310=python310\python.exe"
set "PY_INSTALLER=python-3.10.11-amd64.exe"
set "PY_EXE="

rem 1. Check for local python310 directory first
if exist "%LOCAL_PY310%" (
    echo Found local Python 3.10: %LOCAL_PY310%
    set "PY_EXE=%LOCAL_PY310%"
    goto PY_OK
)

echo Local Python 3.10 not found, attempting to install...

rem 2. Check if installer exists
if not exist "%PY_INSTALLER%" (
    echo Downloading Python 3.10.11 installer, please wait...
    powershell -Command "try { Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.10.11/python-3.10.11-amd64.exe' -OutFile '%PY_INSTALLER%' } catch { exit 1 }"
    if errorlevel 1 (
        echo Failed to download Python installer. Please check your network.
        echo Or manually download from: https://www.python.org/downloads/release/python-31011/
        goto END
    )
) else (
    echo Found Python installer: %PY_INSTALLER%
)

echo.
echo Installing Python 3.10.11...
echo.
echo IMPORTANT: When the installer opens, please:
echo   1. Check "Add Python to PATH" (optional)
echo   2. Click "Customize installation"
echo   3. Click "Next" on Optional Features
echo   4. Change install location to: %~dp0python310
echo   5. Click "Install"
echo.
echo Press any key to start the installer...
pause >nul

start /wait "" "%PY_INSTALLER%"

rem Wait for installation to complete
timeout /t 2 /nobreak >nul

if exist "%LOCAL_PY310%" (
    echo Python 3.10.11 installed successfully!
    set "PY_EXE=%LOCAL_PY310%"
) else (
    echo Python 3.10 installation failed. Please run %PY_INSTALLER% manually.
    goto END
)

:PY_OK
echo.
echo ============================================
echo  2. Create/Check Python Virtual Environment
echo ============================================

set "VENV_DIR=.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"

if not exist "%VENV_PY%" (
    echo Virtual environment %VENV_DIR% not found, creating with Python 3.10...
    "%PY_EXE%" -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo Failed to create virtual environment. Please check Python installation.
        goto END
    )
) else (
    echo Found existing virtual environment: %VENV_DIR%
)

if not exist "%VENV_PY%" (
    echo Python interpreter not found in virtual environment: %VENV_PY%
    echo Please check if virtual environment was created successfully.
    goto END
)

echo Using virtual environment Python: %VENV_PY%
echo.

echo ============================================
echo  3. Install Dependencies (requirements.txt)
echo ============================================

if not exist "requirements.txt" (
    echo requirements.txt not found in current directory.
    goto AFTER_PIP
)

echo Installing dependencies (this may take a few minutes)...

"%VENV_PY%" -m pip install --upgrade pip
if errorlevel 1 (
    echo Warning: Failed to upgrade pip, continuing with dependency installation.
)

"%VENV_PY%" -m pip install -r requirements.txt
if errorlevel 1 (
    echo Error installing dependencies. Please check the error messages above.
    echo You can manually run: "%VENV_PY%" -m pip install -r requirements.txt
    goto AFTER_PIP
)

echo Dependencies installed successfully!
echo.

:AFTER_PIP
echo ============================================
echo  4. Configure config.json
echo ============================================
echo Please ensure the following files exist:
echo    - auto.py
echo    - config.json
echo.
echo Edit config.json with your settings:
echo    network          = mainnet or testnet
echo    rpc_url          = Leave empty for default RPC, or enter custom RPC URL
echo    private_key      = Your wallet private key (Base58 string)
echo    trade_amount_min = Minimum SOL amount per trade
echo    trade_amount_max = Maximum SOL amount per trade
echo    enable_trading   = true to execute trades, false for dry run
echo.

echo ============================================
echo  5. Run the Trading Script
echo ============================================
echo To start auto trading, run:
echo.
echo    "%VENV_PY%" auto.py
echo.
echo Or simply run: run.bat
echo.
echo Log files:
echo    logs\app.log          (Application logs)
echo    logs\transactions.log (Transaction records)
echo.

:END
echo Setup complete!
echo.
pause
endlocal
