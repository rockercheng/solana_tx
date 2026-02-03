@echo off

chcp 65001 >nul
setlocal enabledelayedexpansion

rem 切换到脚本所在目录
cd /d "%~dp0"

echo ============================================
echo  使用 PyInstaller 打包 Solana 自动交易程序
echo ============================================
echo.

set "VENV_PY=.venv\Scripts\python.exe"

if exist "%VENV_PY%" (
    echo 检测到虚拟环境, 使用: %VENV_PY%
) else (
    echo 未检测到虚拟环境 .venv, 尝试使用系统 Python。
    set "VENV_PY=python"
)

echo.
echo 安装/升级 PyInstaller...
"%VENV_PY%" -m pip install --upgrade pyinstaller
if errorlevel 1 (
    echo 安装 PyInstaller 失败, 请检查错误信息。
    goto END
)

echo.
echo 开始使用 PyInstaller 进行打包...
"%VENV_PY%" -m PyInstaller --noconfirm --clean --onefile --hidden-import anchorpy_core --name solana_auto auto.py

if errorlevel 1 (
    echo 打包失败, 请检查上方错误信息。
    goto END
)

echo.
echo 打包完成。生成的可执行文件位于 dist 目录中。

echo.
echo 将 solana_auto.exe 移动到当前项目根目录...
if exist "dist\solana_auto.exe" (
    move /Y "dist\solana_auto.exe" ".\solana_auto.exe"
) else (
    echo 未找到 dist\solana_auto.exe, 请检查打包是否成功。
    goto END
)

echo.
echo 正在打包交付文件为 solana_auto_package.zip ...
powershell -Command "Compress-Archive -Force -Path 'solana_auto.exe','config.json','readme.txt' -DestinationPath 'solana_auto_package.zip'"
if errorlevel 1 (
    echo 打包 zip 失败, 请检查错误信息。
    goto END
)

echo.
echo 已生成交付包: solana_auto_package.zip

echo.
:END
echo 操作结束, 按任意键退出...
pause >nul
endlocal
