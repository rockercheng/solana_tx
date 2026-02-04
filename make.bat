@echo off

chcp 65001 >nul
setlocal enabledelayedexpansion

rem Switch to script directory
cd /d "%~dp0"

echo ============================================
echo  PyInstaller Build - Solana Auto Trading V2
echo ============================================
echo.

set "VENV_PY=.venv\Scripts\python.exe"

if exist "%VENV_PY%" (
    echo Found virtual environment: %VENV_PY%
) else (
    echo Virtual environment not found, using system Python.
    set "VENV_PY=python"
)

echo.
echo Installing/Upgrading PyInstaller...
"%VENV_PY%" -m pip install --upgrade pyinstaller
if errorlevel 1 (
    echo PyInstaller installation failed.
    goto END
)

echo.
echo Starting PyInstaller build...
"%VENV_PY%" -m PyInstaller --noconfirm --clean --onefile --hidden-import anchorpy_core --name solana_auto auto_v2.py

if errorlevel 1 (
    echo Build failed, please check error messages above.
    goto END
)

echo.
echo Build completed. Executable is in dist folder.

echo.
echo Moving solana_auto.exe to project root...
if exist "dist\solana_auto.exe" (
    move /Y "dist\solana_auto.exe" ".\solana_auto.exe"
) else (
    echo dist\solana_auto.exe not found.
    goto END
)

echo.
echo Creating delivery package solana_auto_package.zip ...
powershell -Command "Compress-Archive -Force -Path 'solana_auto.exe','config.json','readme.txt' -DestinationPath 'solana_auto_package.zip'"
if errorlevel 1 (
    echo Failed to create zip package.
    goto END
)

echo.
echo Package created: solana_auto_package.zip

echo.
:END
echo Done. Press any key to exit...
pause >nul
endlocal
