@echo off

chcp 65001 >nul

setlocal enabledelayedexpansion

rem 切换到脚本所在目录
cd /d "%~dp0"

rem 设置环境变量解决 Windows 中文环境下的编码问题
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo ============================================
echo  Solana 自动交易环境初始化脚本
echo ============================================
echo.

rem 定义本地 Python 3.10 路径
set "LOCAL_PY310=python310\python.exe"
set "PY_INSTALLER=python-3.10.11-amd64.exe"
set "PY_EXE="

rem 1. 优先检查本地 python310 目录
if exist "%LOCAL_PY310%" (
    echo 检测到本地 Python 3.10: %LOCAL_PY310%
    set "PY_EXE=%LOCAL_PY310%"
    goto PY_OK
)

echo 未检测到本地 Python 3.10,尝试安装...

rem 2. 检查安装程序是否存在
if not exist "%PY_INSTALLER%" (
    echo 正在下载 Python 3.10.11 安装包,请稍候...
    powershell -Command "try { Invoke-WebRequest -Uri 'https://www.python.org/ftp/python/3.10.11/python-3.10.11-amd64.exe' -OutFile '%PY_INSTALLER%' } catch { exit 1 }"
    if errorlevel 1 (
        echo 下载 Python 安装包失败,请检查网络,或用浏览器手动下载: 
        echo    https://www.python.org/downloads/release/python-31011/
        goto END
    )
) else (
    echo 已存在 Python 安装程序: %PY_INSTALLER%
)

echo.
echo 正在安装 Python 3.10.11 到本地目录 python310...
start /wait "" "%PY_INSTALLER%" /quiet InstallAllUsers=0 PrependPath=0 Include_test=0 TargetDir="%~dp0python310"
if errorlevel 1 (
    echo Python 安装可能失败,请检查安装日志。
    goto END
)

rem 等待安装完成
timeout /t 3 /nobreak >nul

if exist "%LOCAL_PY310%" (
    echo Python 3.10.11 安装成功!
    set "PY_EXE=%LOCAL_PY310%"
) else (
    echo Python 3.10 安装失败,请手动运行 %PY_INSTALLER% 进行安装。
    goto END
)

:PY_OK
echo.
echo ============================================
echo  2. 创建/检查 Python 虚拟环境
echo ============================================

set "VENV_DIR=.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"

if not exist "%VENV_PY%" (
    echo 未检测到虚拟环境 %VENV_DIR%,正在使用 Python 3.10 创建...
    "%PY_EXE%" -m venv "%VENV_DIR%"
    if errorlevel 1 (
        echo 创建虚拟环境失败,请检查 Python 安装后重试。
        goto END
    )
) else (
    echo 已检测到虚拟环境 %VENV_DIR%,将直接使用。
)

if not exist "%VENV_PY%" (
    echo 未在虚拟环境中找到 python 解释器: %VENV_PY%
    echo 请检查虚拟环境是否创建成功。
    goto END
)

echo 使用虚拟环境 Python: %VENV_PY%
echo.

echo ============================================
echo  3. 在虚拟环境中安装项目依赖(requirements.txt)
echo ============================================

if not exist "requirements.txt" (
    echo 未在当前目录找到 requirements.txt,请确认脚本位置是否正确。
    goto AFTER_PIP
)

echo 正在安装依赖(可能需要几分钟,请耐心等待)...

"%VENV_PY%" -m pip install --upgrade pip
if errorlevel 1 (
    echo 升级 pip 时出现问题,将继续尝试安装依赖。
)

"%VENV_PY%" -m pip install -r requirements.txt
if errorlevel 1 (
    echo 安装依赖时出错,请检查上方错误信息。
    echo 你可以手动执行: "%VENV_PY%" -m pip install -r requirements.txt
    goto AFTER_PIP
)

echo 依赖已安装完成(已安装到虚拟环境 %VENV_DIR% 中)。
echo.

:AFTER_PIP
echo ============================================
echo  4. 配置 config.json (源钱包/目的钱包/私钥/金额等)
echo ============================================
echo 1. 请在当前目录确认存在以下文件: 
echo    auto.py
echo    config_example.json
echo    config.json (如果还没有,请先将 config_example.json 复制为 config.json)
echo.
echo 2. 使用文本编辑器打开 config.json,按以下说明配置: 
echo    network          = 选择 mainnet 或 testnet
echo    rpc_url          = 留空则使用默认 RPC,或填自定义节点地址
echo    private_key      = 源钱包私钥(Base58 字符串,请妥善保管勿泄漏)
echo    from_pubkey      = 源钱包地址(可选,留空则使用 private_key 对应地址)
echo    to_pubkey        = 目的钱包地址(接收方地址)
echo    trade_amount_sol = 每次自动转账的 SOL 数量
echo    enable_trading   = true 表示真的发送交易,false 表示只做心跳测试
echo.

echo ============================================
echo  5. 在虚拟环境中运行自动交易脚本
echo ============================================
echo 请在当前目录执行以下命令启动自动交易:
echo.
echo    "%VENV_PY%" auto.py
echo.

echo 日志说明: 
echo    logs\app.log          (一般运行日志)
echo    logs\transactions.log (交易记录日志)
echo.

:END
echo 操作完成,如有错误请根据提示信息处理后重新运行本脚本。
echo.
pause >nul
endlocal
