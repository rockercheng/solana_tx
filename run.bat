@echo off

chcp 65001 >nul

setlocal enabledelayedexpansion

rem 切换到脚本所在目录
cd /d "%~dp0"

echo ============================================
echo  Solana 自动交易运行脚本
echo ============================================
echo.

set "VENV_DIR=.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"

rem 检查虚拟环境是否存在
if not exist "%VENV_PY%" (
    echo 未检测到虚拟环境 %VENV_DIR% 或其中缺少 Python 解释器。
    echo 请先在当前目录运行 setup.bat 创建虚拟环境并安装依赖。
    echo.
    echo 示例:
    echo    setup.bat
    echo.
    goto END
)

echo 使用虚拟环境 Python 运行自动交易脚本...
echo 命令:
echo    "%VENV_PY%" auto.py
echo.

"%VENV_PY%" auto.py

:END
echo.
echo 如需停止自动交易,请在运行窗口中按 Ctrl + C。
echo.
pause >nul
endlocal
