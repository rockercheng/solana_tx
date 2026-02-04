#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Solana 自动交易脚本 - V2 版本 (使用 Jupiter Swap API)

功能特性：
- 使用 Jupiter Swap API (GET /quote + POST /swap) 替代 Ultra API
- 支持用户自定义 slippage_bps (滑点) 和 priority_fee (优先费)
- 通过配置支持主网 / 测试网(如需 Devnet 可自行配置 rpc_url)
- 账号、私钥等敏感信息全部通过配置文件加载
- 详细的调试日志(控制台 + 文件)
- 独立的交易记录日志
- 所有错误都输出清晰易懂的日志(包含堆栈方便排查)


使用说明(简要)：
1. 安装依赖：
   pip install -r requirements.txt
2. 复制 config_example.json 为 config.json, 并按注释填写你的钱包信息
3. 运行脚本：
   python auto_v2.py

注意：本脚本仅为演示自动交易框架, 不构成任何投资建议. 
在真实主网上使用前, 请务必在测试网充分验证、控制金额、了解风险. 
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
import time
import traceback
from dataclasses import dataclass
from typing import Optional, Callable
import base64
import base58
import random
import requests
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders import message as solders_message
from solders.pubkey import Pubkey
from solana.rpc.api import Client
from solana.rpc.types import TokenAccountOpts

# ===== 全局常量 =====
# 主网 SOL (WSOL) Mint 地址
SOL_MINT = "So11111111111111111111111111111111111111112"
# 主网 USDC Mint 地址
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# Jupiter Swap API 基础 URL
# https://portal.jup.ag/api-keys
JUPITER_SWAP_API_BASE = "https://api.jup.ag/swap/v1"
SWAP_API_KEY = "72675114-cb71-4b8b-8c21-c429b1082843"

# 默认 RPC URL 常量, 便于集中管理和修改
DEFAULT_MAINNET_RPC_URL = "https://solana-rpc.publicnode.com"
DEFAULT_TESTNET_RPC_URL = "https://solana-testnet.api.onfinality.io/public"


# 可以通过环境变量 SOLANA_AUTO_CONFIG 覆盖配置文件路径
DEFAULT_CONFIG_PATH = "config.json"
CONFIG_PATH = os.environ.get("SOLANA_AUTO_CONFIG", DEFAULT_CONFIG_PATH)


@dataclass
class AppConfig:
    """应用配置对象, 便于在代码中类型提示和访问字段. """

    network: str  # mainnet / testnet
    rpc_url: str
    private_key: str  # Base58 编码的私钥字符串
    from_pubkey: Optional[str]
    to_pubkey: str
    trade_mode: str  # "SOL_TO_USDC" 或 "USDC_TO_SOL"
    trade_amount_min: float
    trade_amount_max: float
    tx_interval_s: int
    enable_trading: bool
    max_runs: int
    retry_max_attempts: int  # 外部 HTTP/RPC 调用的最大重试次数
    retry_sleep_s: int  # 外部 HTTP/RPC 重试间隔秒数
    slippage_bps: int  # Swap API 使用的滑点(单位：bps, 50 = 0.5%)
    priority_fee: int  # Swap API 使用的优先费(单位：lamports)


def setup_logging(log_dir: str = "logs") -> tuple[logging.Logger, logging.Logger]:
    """初始化日志系统. 
    - 主日志 logger：打印到控制台 + 写入 logs/app.log
    - 交易日志 tx_logger：专门写入 logs/transactions.log
    """

    os.makedirs(log_dir, exist_ok=True)

    # 主日志
    logger = logging.getLogger("solana_auto_v2")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    log_format = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # 控制台日志(DEBUG 级别)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # 文件日志(包含 DEBUG 级别)
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "app_v2.log"),
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # 交易日志
    tx_logger = logging.getLogger("solana_auto_v2.tx")
    tx_logger.setLevel(logging.INFO)
    tx_logger.handlers.clear()
    tx_logger.propagate = False  # 禁止日志向上传播到父 logger, 避免重复输出

    tx_file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "transactions_v2.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    tx_file_handler.setLevel(logging.INFO)
    tx_file_handler.setFormatter(log_format)
    tx_logger.addHandler(tx_file_handler)
    return logger, tx_logger


def load_config(path: str) -> AppConfig:
    """从 JSON 文件加载配置, 并做基本校验和默认值处理. """

    if not os.path.exists(path):
        raise FileNotFoundError(f"配置文件不存在：{path}, 请先复制 config_example.json 为 {path} 并按说明填写. ")

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # 网络类型
    network = raw.get("network", "testnet").lower()

    # RPC URL：可以显式配置, 也可以根据 network 自动选择
    rpc_url = raw.get("rpc_url")
    if not rpc_url:
        if network == "mainnet":
            rpc_url = DEFAULT_MAINNET_RPC_URL
        elif network == "testnet":
            rpc_url = DEFAULT_TESTNET_RPC_URL
        else:
            raise ValueError(f"未知 network 类型：{network}, 请使用 mainnet / testnet 之一. ")


    # 必填字段简单校验
    if "private_key" not in raw:
        raise ValueError("配置文件中缺少 private_key 字段(Base58 编码的私钥字符串). ")

    if "to_pubkey" not in raw:
        raise ValueError("配置文件中缺少 to_pubkey 字段(交易接收方地址). ")

    if "max_runs" not in raw:
        raise ValueError("配置文件中缺少 max_runs 字段 (必须是大于 0 的整数). ")

    try:
        max_runs_int = int(raw["max_runs"])
    except (TypeError, ValueError) as exc:
        raise ValueError("配置项 max_runs 必须是正整数 (大于 0). ") from exc

    if max_runs_int <= 0:
        raise ValueError(
            f"配置项 max_runs 的值无效: {max_runs_int}. 请填写大于 0 的整数. "
        )

    max_runs = max_runs_int

    trade_mode_raw = "SOL_TO_USDC"

    if "trade_amount_min" not in raw or "trade_amount_max" not in raw:
        raise ValueError(
            "配置文件中缺少 trade_amount_min 或 trade_amount_max 字段 (必须是大于 0 的数字). "
        )

    try:
        trade_amount_min = float(raw["trade_amount_min"])
        trade_amount_max = float(raw["trade_amount_max"])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "配置项 trade_amount_min / trade_amount_max 必须是数字类型, 且大于 0. "
        ) from exc

    if trade_amount_min <= 0 or trade_amount_max <= 0:
        raise ValueError("配置项 trade_amount_min / trade_amount_max 必须大于 0. ")

    if trade_amount_min > trade_amount_max:
        raise ValueError(
            f"配置项 trade_amount_min={trade_amount_min} 大于 trade_amount_max={trade_amount_max}, 请填写正确的范围. "
        )

    # 外部 HTTP/RPC 调用的重试策略
    retry_max_attempts = int(raw.get("retry_max_attempts", 10))
    retry_sleep_s = int(raw.get("retry_sleep_s", 2))

    # Swap API 相关配置
    # slippage_bps: 滑点 (单位：bps, 50 = 0.5%, 100 = 1%)
    slippage_bps = int(raw.get("slippage_bps", 50))
    # priority_fee: 优先费 (单位：lamports, 10000 = 0.00001 SOL)
    priority_fee = int(raw.get("priority_fee", 10000))

    cfg = AppConfig(
        network=network,
        rpc_url=rpc_url,
        private_key=str(raw["private_key"]),
        from_pubkey=raw.get("from_pubkey") or None,
        to_pubkey=str(raw["to_pubkey"]),
        trade_mode=trade_mode_raw,
        trade_amount_min=trade_amount_min,
        trade_amount_max=trade_amount_max,
        tx_interval_s=int(raw.get("tx_interval_s", 60)),
        enable_trading=bool(raw.get("enable_trading", True)),
        max_runs=max_runs,
        retry_max_attempts=retry_max_attempts,
        retry_sleep_s=retry_sleep_s,
        slippage_bps=slippage_bps,
        priority_fee=priority_fee,
    )
    return cfg


def random_trade_amount_sol(cfg: AppConfig) -> float:
    """在 trade_amount_min / trade_amount_max 范围内随机选择本次交易的 SOL 数量(单位：SOL). """
    trade_amount_sol_raw = random.uniform(cfg.trade_amount_min, cfg.trade_amount_max)
    trade_amount_sol = round(trade_amount_sol_raw, 3)
    return trade_amount_sol


def load_keypair_from_base58(secret: str) -> Keypair:
    """从 Base58 编码的私钥字符串创建 Keypair. 

    很多钱包(如 Phantom)导出的私钥是 Base58 字符串, 而不是 64 个数字数组. 
    这里统一使用 Base58, 便于配置和复制粘贴. 
    """

    try:
        secret_stripped = secret.strip()
        return Keypair.from_base58_string(secret_stripped)
    except Exception as e:  # noqa: BLE001 - 需要捕获所有错误并给出清晰提示
        raise ValueError(
            "私钥格式错误：请确认是 Base58 编码的 Solana 私钥字符串, 且不要包含多余空格或换行. "
        ) from e


def build_client(cfg: AppConfig) -> Client:
    """根据配置创建 Solana RPC 客户端. """
    return Client(cfg.rpc_url)


def http_request_with_retry(
    cfg: Optional[AppConfig],
    logger: logging.Logger,
    label: str,
    method: str,
    url: str,
    *,
    session: Optional[requests.Session] = None,
    headers: Optional[dict] = None,
    json_body: Optional[dict] = None,
    timeout: int = 30,
    treat_insufficient_as_fatal: bool = False,
    max_attempts: Optional[int] = None,
    sleep_s: Optional[float] = None,
    response_hook: Optional[Callable[[requests.Response, int, int], None]] = None,
) -> requests.Response:

    """通用 HTTP 请求重试封装. 
    - 不关注具体业务, 只处理: 重试、超时、HTTP 状态码、余额不足等通用模式
    - 具体的 JSON 解析和字段校验由调用方负责
    - 可选 response_hook: 每次非 200 响应时调用, 便于业务层输出更细日志
    """

    if cfg is None and (max_attempts is None or sleep_s is None):
        raise ValueError("http_request_with_retry 需要 cfg 或显式提供 max_attempts 和 sleep_s. ")

    if max_attempts is None:
        max_attempts = cfg.retry_max_attempts  # type: ignore[union-attr]
    if sleep_s is None:
        sleep_s = cfg.retry_sleep_s  # type: ignore[union-attr]

    resp: Optional[requests.Response] = None
    last_resp_text = ""

    for attempt in range(1, max_attempts + 1):
        try:
            if session is not None:
                resp = session.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_body,
                    timeout=timeout,
                )
            else:
                resp = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=json_body,
                    timeout=timeout,
                )
        except Exception as e:  # noqa: BLE001
            if attempt >= max_attempts:
                logger.error("%s失败(已重试 %d 次, 请求异常): %s", label, max_attempts, e)
                raise RuntimeError(f"{label}失败(请求异常): {e}") from e

            logger.warning(
                "%s失败(第 %d 次重试, 共 %d 次, 请求异常): %s",
                label,
                attempt,
                max_attempts,
                e,
            )
        else:
            if resp.status_code == 200:
                return resp

            last_resp_text = resp.text or ""

            # 允许业务层在非 200 响应时做额外处理/日志
            if response_hook is not None:
                try:
                    response_hook(resp, attempt, max_attempts)
                except Exception as hook_e:  # noqa: BLE001
                    logger.warning(
                        "%s 的 response_hook 执行异常(第 %d 次重试, 共 %d 次): %s",
                        label,
                        attempt,
                        max_attempts,
                        hook_e,
                    )

            # 余额不足类错误视为致命错误, 立即终止, 不再重试
            if treat_insufficient_as_fatal:
                lower_msg = last_resp_text.lower()
                if ("insufficient" in lower_msg and "fund" in lower_msg) or ("余额不足" in last_resp_text):
                    logger.error("%s失败, 检测到可能的余额不足: %s", label, last_resp_text)
                    raise RuntimeError(f"{label}失败：余额不足或资金不足. ")

            if attempt >= max_attempts:
                logger.error(
                    "%s失败(已重试 %d 次), HTTP %d: %s",
                    label,
                    max_attempts,
                    resp.status_code,
                    last_resp_text,
                )
                raise RuntimeError(f"{label}失败, HTTP {resp.status_code}: {last_resp_text}")

            logger.warning(
                "%s失败(第 %d 次重试, 共 %d 次), HTTP %d: %s",
                label,
                attempt,
                max_attempts,
                resp.status_code,
                last_resp_text,
            )

        if attempt < max_attempts:
            time.sleep(sleep_s)

    # 理论上不会到这里, 防御性处理
    raise RuntimeError(f"{label}失败, HTTP {getattr(resp, 'status_code', '未知')}: {last_resp_text}")


def get_usdc_balance(
    client: Client,
    owner_pubkey: str,
    usdc_mint: str,
    logger: logging.Logger,
    rpc_url: Optional[str] = None,
    retry_max_attempts: int = 10,
    retry_sleep_s: int = 2,
) -> int:
    """查询指定钱包当前的 USDC 余额(最小单位). 
    
    改进点：
    1. 显式指定 commitment: "confirmed" 确保读取已确认的数据
    2. 增加重试机制
    3. 增加 Token Account 解析容错
    """

    owner_pk = Pubkey.from_string(owner_pubkey)
    mint_pk = Pubkey.from_string(usdc_mint)

    # 使用 TokenAccountOpts, 以兼容当前 solana-py 版本
    opts = TokenAccountOpts(mint=mint_pk, encoding="jsonParsed")

    try:
        resp = client.get_token_accounts_by_owner(owner_pk, opts)

        # 兼容 typed 响应和 dict 响应两种形式
        if hasattr(resp, "value"):
            value = resp.value
        else:
            result = resp.get("result", {})
            value = result.get("value", [])
    except Exception as e:  # noqa: BLE001
        # 这一层失败通常是响应结构与当前 solana-py 版本的 typed 定义不完全匹配
        # 下面会自动退回到原始 JSON-RPC 调用, 因此这里仅记录为 WARNING
        logger.warning("通过 solana-py 获取 USDC 余额失败, 将退回原始 JSON-RPC 调用: %s", e)

        # 如果未显式传入 rpc_url, 则尝试从 client 中获取
        if rpc_url is None:
            try:
                rpc_url = getattr(getattr(client, "_provider", None), "endpoint_uri", None)
            except Exception:  # noqa: BLE001
                rpc_url = None

        if not rpc_url:
            # 无法获得 RPC 地址, 只能把异常抛出去
            raise

        # 退回到直接使用 JSON-RPC 请求, 并增加重试机制
        # 显式指定 commitment: "confirmed" 确保读取已确认的数据
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                owner_pubkey,
                {"mint": usdc_mint},
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
        }

        http_resp = http_request_with_retry(
            cfg=None,
            logger=logger,
            label="使用原始 RPC 请求获取 USDC 余额",
            method="POST",
            url=rpc_url,
            headers={"Content-Type": "application/json"},
            json_body=payload,
            timeout=20,
            treat_insufficient_as_fatal=False,
            max_attempts=retry_max_attempts,
            sleep_s=retry_sleep_s,
        )
        data = http_resp.json()
        result = data.get("result", {})
        value = result.get("value", [])

    total = 0
    for item in value:
        try:
            # 增加容错处理
            if isinstance(item, dict):
                account = item.get("account", {})
                data = account.get("data", {})
                if isinstance(data, dict):
                    parsed = data.get("parsed", {})
                    info = parsed.get("info", {})
                    token_amount = info.get("tokenAmount", {})
                    amount_str = token_amount.get("amount", "0")
                    total += int(amount_str)
            elif hasattr(item, "account"):
                # 处理 typed 响应
                account = item.account
                if hasattr(account, "data") and hasattr(account.data, "parsed"):
                    info = account.data.parsed.get("info", {})
                    token_amount = info.get("tokenAmount", {})
                    amount_str = token_amount.get("amount", "0")
                    total += int(amount_str)
        except Exception as parse_e:  # noqa: BLE001
            logger.debug("解析 USDC 余额时跳过异常账户: %s, 错误: %s", item, parse_e)
    return total


def get_usdc_balance_with_retry(
    client: Client,
    owner_pubkey: str,
    usdc_mint: str,
    logger: logging.Logger,
    rpc_url: str,
    retry_max_attempts: int = 5,
    retry_sleep_s: int = 2,
) -> int:
    """带重试的 USDC 余额查询, 在交易确认后使用. 
    
    在第一步交易确认后, RPC 节点可能还未同步最新状态,
    因此需要多次重试以获取正确的 USDC 余额.
    """
    
    for attempt in range(1, retry_max_attempts + 1):
        try:
            # 使用原始 JSON-RPC 请求, 显式指定 commitment: "confirmed"
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountsByOwner",
                "params": [
                    owner_pubkey,
                    {"mint": usdc_mint},
                    {"encoding": "jsonParsed", "commitment": "confirmed"},
                ],
            }
            
            http_resp = requests.post(
                rpc_url,
                headers={"Content-Type": "application/json"},
                json=payload,
                timeout=20,
            )
            http_resp.raise_for_status()
            data = http_resp.json()
            result = data.get("result", {})
            value = result.get("value", [])
            
            total = 0
            for item in value:
                try:
                    if isinstance(item, dict):
                        amount_str = item["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
                        total += int(amount_str)
                except Exception:  # noqa: BLE001
                    pass
            
            if total > 0:
                logger.debug("第 %d 次重试: 获取 USDC 余额成功: %d", attempt, total)
                return total
            
            # 如果余额为 0, 可能是数据未同步, 继续重试
            if attempt < retry_max_attempts:
                logger.debug(
                    "第 %d 次重试: 获取 USDC 余额为 0, 等待 %d 秒后重试...",
                    attempt,
                    retry_sleep_s,
                )
                time.sleep(retry_sleep_s)
        except Exception as e:  # noqa: BLE001
            logger.warning("第 %d 次重试: 获取 USDC 余额失败: %s", attempt, e)
            if attempt < retry_max_attempts:
                time.sleep(retry_sleep_s)
    
    # 最后一次尝试使用标准函数
    return get_usdc_balance(
        client, owner_pubkey, usdc_mint, logger, rpc_url,
        retry_max_attempts=1, retry_sleep_s=1
    )


def get_wallet_balances(
    client: Client,
    owner_pubkey: str,
    usdc_mint: str,
    logger: logging.Logger,
    rpc_url: Optional[str] = None,
    retry_max_attempts: int = 10,
    retry_sleep_s: int = 2,
) -> tuple[int, int]:
    """查询指定钱包当前的 SOL / USDC 余额. 
    返回 (sol_lamports, usdc_units). 
    """

    owner_pk = Pubkey.from_string(owner_pubkey)

    # 先通过 solana-py 获取 SOL 余额, 如果解析失败则退回到原始 JSON-RPC 调用
    try:
        resp_sol = client.get_balance(owner_pk)

        # 兼容 typed 响应和 dict 响应
        if hasattr(resp_sol, "value"):
            sol_lamports = int(resp_sol.value)
        else:
            sol_lamports = int(resp_sol.get("result", {}).get("value", 0))
    except Exception as e:  # noqa: BLE001
        # 这一层失败通常是响应结构与当前 solana-py 版本的 typed 定义不完全匹配
        # 下面会自动退回到原始 JSON-RPC 调用, 因此这里仅记录为 WARNING
        logger.warning("通过 solana-py 获取 SOL 余额失败, 将退回原始 JSON-RPC 调用: %s", e)

        if rpc_url is None:
            try:
                rpc_url = getattr(getattr(client, "_provider", None), "endpoint_uri", None)
            except Exception:  # noqa: BLE001
                rpc_url = None

        if not rpc_url:
            # 无法获得 RPC 地址, 只能把异常抛出去
            raise

        # 使用原始 JSON-RPC 调用作为兜底, 并增加重试机制(使用通用 HTTP 重试封装)
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBalance",
            "params": [owner_pubkey, {"commitment": "confirmed"}],
        }

        http_resp = http_request_with_retry(
            cfg=None,
            logger=logger,
            label="使用原始 RPC 请求获取 SOL 余额",
            method="POST",
            url=rpc_url,
            headers={"Content-Type": "application/json"},
            json_body=payload,
            timeout=20,
            treat_insufficient_as_fatal=False,
            max_attempts=retry_max_attempts,
            sleep_s=retry_sleep_s,
        )
        data = http_resp.json()
        sol_lamports = int(data.get("result", {}).get("value", 0))

    # 查询 USDC 余额(内部同样带有原始 JSON-RPC 兜底)
    usdc_units = get_usdc_balance(client, owner_pubkey, usdc_mint, logger, rpc_url)
    return sol_lamports, usdc_units


def wait_for_transaction_confirmation(
    client: Client,
    signature: str,
    logger: logging.Logger,
    rpc_url: Optional[str] = None,
    timeout_s: int = 60,
    poll_interval_s: int = 2,
) -> bool:
    """轮询指定交易签名的确认状态, 在给定超时时间内等待其达到 confirmed/finalized. 
    返回 True 表示交易确认成功, False 表示超时或确认失败. 
    """

    # 优先使用 cfg.rpc_url 传入的地址, 否则尝试从 client 中获取
    if rpc_url is None:
        try:
            rpc_url = getattr(getattr(client, "_provider", None), "endpoint_uri", None)
        except Exception:  # noqa: BLE001
            rpc_url = None

    deadline = time.time() + timeout_s
    last_status: Optional[dict] = None

    while time.time() < deadline:
        try:
            status = None

            # 优先走原始 JSON-RPC, 避免 typed 响应兼容性问题
            if rpc_url:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignatureStatuses",
                    "params": [[signature], {"searchTransactionHistory": True}],
                }
                http_resp = requests.post(rpc_url, json=payload, timeout=20)
                http_resp.raise_for_status()
                data = http_resp.json()
                value_list = data.get("result", {}).get("value", [])
                status = value_list[0] if value_list else None
            else:
                # 兜底使用 solana-py 的客户端
                resp = client.get_signature_statuses([signature])
                if hasattr(resp, "value"):
                    value_list = resp.value
                else:
                    value_list = resp.get("result", {}).get("value", [])
                status = value_list[0] if value_list else None

            if status is None:
                logger.debug("交易 %s 尚未出现在节点状态中, 继续等待...", signature)
            else:
                # status 可能是 dict, 也可能是 typed 对象, 这里统一转成 dict 访问
                if not isinstance(status, dict):
                    # 尝试通过属性构造一个简单的 dict 视图
                    status = {
                        "err": getattr(status, "err", None),
                        "confirmationStatus": getattr(status, "confirmation_status", None),
                    }

                last_status = status
                err = status.get("err")
                conf_status = status.get("confirmationStatus") or status.get("confirmation_status")

                if err:
                    logger.error("交易 %s 在链上确认失败: %s", signature, err)
                    return False

                if conf_status in ("confirmed", "finalized"):
                    logger.info("交易 %s 已在链上确认, 当前状态=%s", signature, conf_status)
                    return True

                logger.debug(
                    "交易 %s 当前确认状态=%s, 继续等待...",
                    signature,
                    conf_status,
                )
        except Exception as e:  # noqa: BLE001
            logger.warning("查询交易 %s 确认状态时发生异常: %s", signature, e)

        time.sleep(poll_interval_s)

    logger.error(
        "在超时时间 %d 秒内未能确认交易 %s, 最后状态=%s",
        timeout_s,
        signature,
        last_status,
    )
    return False


# ========================== Jupiter Swap API 函数 ==========================

def swap_get_quote(
    cfg: AppConfig,
    logger: logging.Logger,
    http_session: requests.Session,
    input_mint: str,
    output_mint: str,
    amount: int,
) -> dict:
    """调用 Jupiter Swap API 的 /quote 接口获取报价. 
    
    参数:
        input_mint: 输入代币 Mint 地址
        output_mint: 输出代币 Mint 地址
        amount: 输入金额 (最小单位, 如 lamports)
        
    返回:
        quoteResponse 对象, 用于后续 /swap 请求
    """
    
    quote_url = (
        f"{JUPITER_SWAP_API_BASE}/quote"
        f"?inputMint={input_mint}"
        f"&outputMint={output_mint}"
        f"&amount={amount}"
        f"&slippageBps={cfg.slippage_bps}"
    )
    
    headers = {
        "x-api-key": SWAP_API_KEY,
    }
    
    logger.debug("请求 Swap Quote: %s", quote_url)
    
    resp = http_request_with_retry(
        cfg=cfg,
        logger=logger,
        label=f"获取 {input_mint[:8]}...->{output_mint[:8]}... 报价",
        method="GET",
        url=quote_url,
        session=http_session,
        headers=headers,
        timeout=30,
        treat_insufficient_as_fatal=True,
    )
    
    try:
        quote_response = resp.json()
    except Exception as e:
        logger.error("解析 Quote 响应 JSON 失败: %s, 原始响应: %s", e, resp.text)
        raise RuntimeError(f"解析 Quote 响应 JSON 失败: {e}") from e
    
    # 检查是否有错误
    if quote_response.get("error") or quote_response.get("errorCode"):
        error_msg = quote_response.get("error") or quote_response.get("errorMessage") or "未知错误"
        raise RuntimeError(f"Quote 请求返回错误: {error_msg}")
    
    # 检查 outAmount 是否有效
    out_amount = int(quote_response.get("outAmount", 0))
    if out_amount <= 0:
        raise RuntimeError(f"Quote 返回的 outAmount 无效: {quote_response.get('outAmount')}")
    
    logger.info(
        "Quote 成功: 输入=%d, 预计输出=%d, 滑点=%d bps",
        amount,
        out_amount,
        cfg.slippage_bps,
    )
    
    return quote_response


def swap_build_transaction(
    cfg: AppConfig,
    logger: logging.Logger,
    http_session: requests.Session,
    user_pubkey: str,
    quote_response: dict,
) -> tuple[str, int]:
    """调用 Jupiter Swap API 的 /swap 接口构造交易. 
    
    参数:
        user_pubkey: 用户钱包公钥
        quote_response: /quote 返回的报价对象
        
    返回:
        (swap_transaction_base64, last_valid_block_height)
    """
    
    swap_url = f"{JUPITER_SWAP_API_BASE}/swap"
    
    headers = {
        "Content-Type": "application/json",
        "x-api-key": SWAP_API_KEY,
    }
    
    # 构造请求体
    # 使用方案 A: 直接传入固定的 lamports 值作为 prioritizationFeeLamports
    payload = {
        "userPublicKey": user_pubkey,
        "quoteResponse": quote_response,
        "prioritizationFeeLamports": cfg.priority_fee,  # 直接使用固定 lamports 值
        "dynamicComputeUnitLimit": True,  # 启用动态计算单元限制, 优化费用
        "wrapAndUnwrapSol": True,  # 自动处理 SOL/WSOL 转换
    }
    
    logger.debug(
        "请求 Swap 交易构造: priorityFee=%d lamports, slippage=%d bps",
        cfg.priority_fee,
        cfg.slippage_bps,
    )
    
    resp = http_request_with_retry(
        cfg=cfg,
        logger=logger,
        label="构造 Swap 交易",
        method="POST",
        url=swap_url,
        session=http_session,
        headers=headers,
        json_body=payload,
        timeout=30,
        treat_insufficient_as_fatal=True,
    )
    
    try:
        swap_response = resp.json()
    except Exception as e:
        logger.error("解析 Swap 响应 JSON 失败: %s, 原始响应: %s", e, resp.text)
        raise RuntimeError(f"解析 Swap 响应 JSON 失败: {e}") from e
    
    # 检查是否有错误
    if swap_response.get("error") or swap_response.get("errorCode"):
        error_msg = swap_response.get("error") or swap_response.get("errorMessage") or "未知错误"
        raise RuntimeError(f"Swap 请求返回错误: {error_msg}")
    
    # 提取交易数据
    swap_tx_base64 = swap_response.get("swapTransaction")
    if not swap_tx_base64:
        raise RuntimeError(f"Swap 响应中未找到 swapTransaction 字段: {swap_response}")
    
    last_valid_block_height = swap_response.get("lastValidBlockHeight", 0)
    actual_priority_fee = swap_response.get("prioritizationFeeLamports", cfg.priority_fee)
    
    logger.info(
        "Swap 交易构造成功: 实际优先费=%d lamports, lastValidBlockHeight=%d",
        actual_priority_fee,
        last_valid_block_height,
    )
    
    return swap_tx_base64, last_valid_block_height


def swap_sign_and_send(
    cfg: AppConfig,
    client: Client,
    logger: logging.Logger,
    swap_tx_base64: str,
    keypair: Keypair,
) -> str:
    """签名并通过 Solana RPC 发送交易. 
    
    参数:
        swap_tx_base64: Base64 编码的未签名交易
        keypair: 用户密钥对
        
    返回:
        交易签名 (signature)
    """
    
    # 1. 解码交易
    try:
        tx_bytes = base64.b64decode(swap_tx_base64)
        raw_tx = VersionedTransaction.from_bytes(tx_bytes)
    except Exception as e:
        logger.error("解码 Swap 交易失败: %s", e)
        raise RuntimeError(f"解码 Swap 交易失败: {e}") from e
    
    # 2. 签名交易
    try:
        sig_bytes = keypair.sign_message(
            solders_message.to_bytes_versioned(raw_tx.message)
        )
        signed_tx = VersionedTransaction.populate(raw_tx.message, [sig_bytes])
    except Exception as e:
        logger.error("签名 Swap 交易失败: %s", e)
        raise RuntimeError(f"签名 Swap 交易失败: {e}") from e
    
    logger.debug("交易签名完成")
    
    # 3. 通过 Solana RPC 发送交易
    try:
        signed_tx_bytes = bytes(signed_tx)
        response = client.send_raw_transaction(signed_tx_bytes)
        
        # 提取交易签名
        if hasattr(response, "value"):
            tx_signature = str(response.value)
        else:
            tx_signature = str(response.get("result", ""))
        
        if not tx_signature:
            raise RuntimeError(f"发送交易后未能获取签名: {response}")
        
        logger.info("交易已发送: %s", tx_signature)
        logger.info("Solscan: https://solscan.io/tx/%s", tx_signature)
        
        return tx_signature
    except Exception as e:
        logger.error("发送 Swap 交易失败: %s", e)
        logger.debug("发送交易异常堆栈:\n%s", traceback.format_exc())
        raise RuntimeError(f"发送 Swap 交易失败: {e}") from e


# ========================== 交易执行逻辑 ==========================

def _execute_first_leg(
    cfg: AppConfig,
    client: Client,
    keypair: Keypair,
    logger: logging.Logger,
    tx_logger: logging.Logger,
    stats: dict,
    http_session: requests.Session,
    wallet_pubkey: str,
    sol_mint: str,
    usdc_mint: str,
    trade_amount_sol_display: float,
    amount_sol_lamports: int,
) -> tuple[bool, dict, Optional[str], Optional[int]]:
    """执行第一步 SOL->USDC 交易. 
    
    使用 Jupiter Swap API (GET /quote + POST /swap + RPC send)
    """

    error_reason: Optional[str] = None

    # -------- 第一步: SOL -> USDC --------
    logger.info(
        "一对交易-第 1 步: 准备将本次随机选取的 %.9f SOL 换为 USDC...",
        trade_amount_sol_display,
    )

    try:
        # 1. 获取报价
        quote_response = swap_get_quote(
            cfg=cfg,
            logger=logger,
            http_session=http_session,
            input_mint=sol_mint,
            output_mint=usdc_mint,
            amount=amount_sol_lamports,
        )
        
        out_usdc_amount = int(quote_response.get("outAmount", 0))
        if out_usdc_amount <= 0:
            error_reason = f"SOL->USDC Quote 返回的 outAmount 无效: {quote_response.get('outAmount')}"
            logger.error("%s, 完整响应: %s", error_reason, quote_response)
            return False, stats, error_reason, None

        # 记录本次一对交易中的 SOL/USDC 数量
        stats["sol_spent_lamports"] = amount_sol_lamports
        stats["usdc_received_units"] = out_usdc_amount
        stats["usdc_spent_units"] = out_usdc_amount

        logger.info(
            "SOL->USDC 报价成功: 输入=%d lamports (%.9f SOL), 预计输出=%d USDC units (%.6f USDC)",
            amount_sol_lamports,
            amount_sol_lamports / 1_000_000_000,
            out_usdc_amount,
            out_usdc_amount / 1_000_000,
        )

        # 2. 构造交易
        swap_tx_base64, _ = swap_build_transaction(
            cfg=cfg,
            logger=logger,
            http_session=http_session,
            user_pubkey=wallet_pubkey,
            quote_response=quote_response,
        )

        # 3. 签名并发送交易
        sig1 = swap_sign_and_send(
            cfg=cfg,
            client=client,
            logger=logger,
            swap_tx_base64=swap_tx_base64,
            keypair=keypair,
        )

        logger.info("一对交易-第 1 步 SOL->USDC 交易已提交, 签名: %s", sig1)

        tx_logger.info(
            (
                "步骤=1 类型=SOL_TO_USDC "
                "输入代币=SOL 输入数量_solana=%.9f 输入_lamports=%d "
                "输出代币=USDC 输出数量_usdc=%.6f 输出_units=%d "
                "滑点_bps=%d 优先费_lamports=%d "
                "签名=%s"
            ),
            trade_amount_sol_display,
            amount_sol_lamports,
            out_usdc_amount / 1_000_000,
            out_usdc_amount,
            cfg.slippage_bps,
            cfg.priority_fee,
            sig1,
        )

    except Exception as e:  # noqa: BLE001
        error_reason = f"第一步 SOL->USDC 交易失败: {e}"
        logger.error(error_reason)
        logger.debug("第一步交易异常堆栈:\n%s", traceback.format_exc())
        return False, stats, error_reason, None

    # -------- 等待第一步交易链上确认 --------
    # 这是关键步骤: 必须等第一步交易确认后才能执行第二步
    confirm_timeout = max(cfg.tx_interval_s, 30)
    logger.info(
        "第一步 SOL->USDC 交易已提交, 开始等待最多 %d 秒以确认链上状态...",
        confirm_timeout,
    )

    if not wait_for_transaction_confirmation(
        client=client,
        signature=sig1,
        logger=logger,
        rpc_url=cfg.rpc_url,
        timeout_s=confirm_timeout,
        poll_interval_s=2,
    ):
        error_reason = (
            f"SOL->USDC 交易 {sig1} 在 {confirm_timeout} 秒内未确认或确认失败, 无法执行后续 USDC->SOL 交易. "
        )
        logger.error(error_reason)
        return False, stats, error_reason, None

    logger.info("第一步 SOL->USDC 交易已在链上确认成功!")

    # 在执行第二步前, 短暂等待让 RPC 节点同步数据
    logger.debug("等待 2 秒让 RPC 节点同步数据...")
    time.sleep(2)

    # 查询当前 USDC 余额, 使用带重试的函数
    try:
        current_usdc_balance = get_usdc_balance_with_retry(
            client=client,
            owner_pubkey=wallet_pubkey,
            usdc_mint=usdc_mint,
            logger=logger,
            rpc_url=cfg.rpc_url,
            retry_max_attempts=5,
            retry_sleep_s=2,
        )

        logger.info("当前 USDC 余额(最小单位) = %d (%.6f USDC)", current_usdc_balance, current_usdc_balance / 1_000_000)
        
        if current_usdc_balance <= 0:
            error_reason = f"当前 USDC 余额({current_usdc_balance}) 小于等于 0, 无法执行 USDC->SOL 交易. "
            logger.error(error_reason)
            return False, stats, error_reason, None

        # 本次 USDC->SOL 使用当前钱包中全部 USDC 余额
        effective_usdc_amount = current_usdc_balance

        # 获取 SOL 余额用于日志
        try:
            sol_after_leg1, _ = get_wallet_balances(
                client,
                wallet_pubkey,
                usdc_mint,
                logger,
                cfg.rpc_url,
                cfg.retry_max_attempts,
                cfg.retry_sleep_s,
            )
            tx_logger.info(
                "余额快照 阶段=第一步后 钱包_SOL_lamports=%d 钱包_SOL=%.9f 钱包_USDC_units=%d 钱包_USDC=%.6f",
                sol_after_leg1,
                sol_after_leg1 / 1_000_000_000,
                current_usdc_balance,
                current_usdc_balance / 1_000_000,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("获取第一步后 SOL 余额失败: %s", e)

    except Exception as e:  # noqa: BLE001
        error_reason = f"查询 USDC 余额失败: {e}"
        logger.error(error_reason)
        return False, stats, error_reason, None

    return True, stats, None, effective_usdc_amount


def _execute_second_leg(
    cfg: AppConfig,
    client: Client,
    keypair: Keypair,
    logger: logging.Logger,
    tx_logger: logging.Logger,
    stats: dict,
    http_session: requests.Session,
    wallet_pubkey: str,
    sol_mint: str,
    usdc_mint: str,
    effective_usdc_amount: int,
) -> tuple[bool, dict, Optional[str]]:
    """执行第二步 USDC->SOL 交易. 
    
    使用 Jupiter Swap API (GET /quote + POST /swap + RPC send)
    """

    error_reason: Optional[str] = None

    # -------- 第二步: USDC -> SOL --------
    logger.info(
        "一对交易-第 2 步: 准备将 %d USDC units (%.6f USDC) 换回 SOL...",
        effective_usdc_amount,
        effective_usdc_amount / 1_000_000,
    )

    try:
        # 1. 获取报价
        quote_response = swap_get_quote(
            cfg=cfg,
            logger=logger,
            http_session=http_session,
            input_mint=usdc_mint,
            output_mint=sol_mint,
            amount=effective_usdc_amount,
        )
        
        out_sol_lamports = int(quote_response.get("outAmount", 0))
        if out_sol_lamports <= 0:
            error_reason = f"USDC->SOL Quote 返回的 outAmount 无效: {quote_response.get('outAmount')}"
            logger.error("%s, 完整响应: %s", error_reason, quote_response)
            return False, stats, error_reason

        logger.info(
            "USDC->SOL 报价成功: 输入=%d USDC units (%.6f USDC), 预计输出=%d lamports (%.9f SOL)",
            effective_usdc_amount,
            effective_usdc_amount / 1_000_000,
            out_sol_lamports,
            out_sol_lamports / 1_000_000_000,
        )

        # 2. 构造交易
        swap_tx_base64, _ = swap_build_transaction(
            cfg=cfg,
            logger=logger,
            http_session=http_session,
            user_pubkey=wallet_pubkey,
            quote_response=quote_response,
        )

        # 3. 签名并发送交易
        sig2 = swap_sign_and_send(
            cfg=cfg,
            client=client,
            logger=logger,
            swap_tx_base64=swap_tx_base64,
            keypair=keypair,
        )

        # 第二步成功后, 记录最终买回的 SOL 数量
        stats["sol_bought_lamports"] = out_sol_lamports

        logger.info("一对交易-第 2 步完成, 交易签名: %s", sig2)
        
        tx_logger.info(
            (
                "步骤=2 类型=USDC_TO_SOL "
                "输入代币=USDC 输入数量_usdc=%.6f 输入_units=%d "
                "输出代币=SOL 输出数量_solana=%.9f 输出_lamports=%d "
                "滑点_bps=%d 优先费_lamports=%d "
                "签名=%s"
            ),
            effective_usdc_amount / 1_000_000,
            effective_usdc_amount,
            out_sol_lamports / 1_000_000_000,
            out_sol_lamports,
            cfg.slippage_bps,
            cfg.priority_fee,
            sig2,
        )

        # 一对交易级别的汇总日志, 统计额外损耗/收益
        net_sol_lamports = stats["sol_bought_lamports"] - stats["sol_spent_lamports"]
        net_sol = net_sol_lamports / 1_000_000_000
        tx_logger.info(
            "pair_summary sol_in_lamports=%d sol_out_lamports=%d net_sol_change_lamports=%d net_sol_change=%.9f",
            stats["sol_spent_lamports"],
            stats["sol_bought_lamports"],
            net_sol_lamports,
            net_sol,
        )

        # 等待第二步交易确认 (可选, 但建议等待以获取准确余额)
        confirm_timeout = max(cfg.tx_interval_s, 20)
        logger.info(
            "第二步 USDC->SOL 交易已提交, 开始等待最多 %d 秒以确认链上状态...",
            confirm_timeout,
        )

        if not wait_for_transaction_confirmation(
            client=client,
            signature=sig2,
            logger=logger,
            rpc_url=cfg.rpc_url,
            timeout_s=confirm_timeout,
            poll_interval_s=2,
        ):
            # 第二步确认失败不算整体失败, 只记录警告
            logger.warning(
                "USDC->SOL 交易 %s 在 %d 秒内未能确认, 但交易已提交. ",
                sig2,
                confirm_timeout,
            )

        # 记录第二笔交易完成后的钱包余额
        try:
            sol_after_leg2, usdc_after_leg2 = get_wallet_balances(
                client,
                wallet_pubkey,
                usdc_mint,
                logger,
                cfg.rpc_url,
                cfg.retry_max_attempts,
                cfg.retry_sleep_s,
            )

            tx_logger.info(
                "余额快照 阶段=第二步后 钱包_SOL_lamports=%d 钱包_SOL=%.9f 钱包_USDC_units=%d 钱包_USDC=%.6f",
                sol_after_leg2,
                sol_after_leg2 / 1_000_000_000,
                usdc_after_leg2,
                usdc_after_leg2 / 1_000_000,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("获取第二步后钱包余额失败: %s", e)

    except Exception as e:  # noqa: BLE001
        error_reason = f"第二步 USDC->SOL 交易失败: {e}"
        logger.error(error_reason)
        logger.debug("第二步交易异常堆栈:\n%s", traceback.format_exc())
        return False, stats, error_reason

    return True, stats, None


def _execute_trade_pair_once(
    cfg: AppConfig,
    client: Client,
    keypair: Keypair,
    logger: logging.Logger,
    tx_logger: logging.Logger,
) -> tuple[bool, dict, Optional[str]]:
    """执行"一对交易": SOL -> USDC -> SOL. 

    步骤:
    1. 使用配置中的 trade_amount (SOL 数量) 将 SOL 换成 USDC;
    2. 等待第一步交易在链上确认;
    3. 使用步骤 1 得到的 USDC 金额再换回 SOL. 

    任一步骤失败, 都会立即终止本次"一对交易", 并返回 (False, stats). 
    全部成功时返回 (True, stats). 
    """

    base_empty_stats = {
        "sol_spent_lamports": 0,
        "usdc_received_units": 0,
        "usdc_spent_units": 0,
        "sol_bought_lamports": 0,
    }

    error_reason: Optional[str] = None

    if cfg.network != "mainnet":
        logger.error(
            "当前网络配置为 %s, 仅支持在 mainnet 上执行 SOL/USDC 互换. ",
            cfg.network,
        )
        error_reason = (
            f"当前网络配置为 {cfg.network}, 仅支持在 mainnet 上执行 SOL/USDC 互换. "
        )
        return False, base_empty_stats.copy(), error_reason

    # 默认 mainnet RPC
    rpc_url = cfg.rpc_url
    logger.info(f'rpc_url: {rpc_url}')

    # 使用全局常量
    sol_mint = SOL_MINT
    usdc_mint = USDC_MINT

    # 钱包公钥
    wallet_pubkey = str(keypair.pubkey())

    # 为本次一对交易随机生成 SOL 卖出数量(单位：SOL), 只保留 3 位小数
    trade_amount_sol = random_trade_amount_sol(cfg)
    trade_amount_sol_display = trade_amount_sol

    amount_sol_lamports = int(trade_amount_sol * 1_000_000_000)

    if amount_sol_lamports <= 0:
        logger.error(
            "根据 trade_amount 范围 [%.9f, %.9f] 随机得到的本次 SOL 数量=%.9f, "
            "换算为最小单位数量=%d, 必须大于 0. ",
            cfg.trade_amount_min,
            cfg.trade_amount_max,
            trade_amount_sol_display,
            amount_sol_lamports,
        )
        error_reason = (
            "根据 trade_amount 范围随机得到的本次 SOL 数量换算为最小单位后无效 (<= 0). "
        )
        return False, base_empty_stats.copy(), error_reason

    # 本次一对交易内的统计数据
    stats = base_empty_stats.copy()

    # 创建 requests Session
    http_session = requests.Session()

    # -------- 执行第一步: SOL -> USDC --------
    first_ok, stats, error_reason, effective_usdc_amount = _execute_first_leg(
        cfg,
        client,
        keypair,
        logger,
        tx_logger,
        stats,
        http_session,
        wallet_pubkey,
        sol_mint,
        usdc_mint,
        trade_amount_sol_display,
        amount_sol_lamports,
    )

    if not first_ok:
        return False, stats, error_reason

    # 以实际可用的 USDC 数量作为本次 USDC->SOL 的输入
    stats["usdc_spent_units"] = effective_usdc_amount

    # -------- 执行第二步: USDC -> SOL --------
    second_ok, stats, error_reason = _execute_second_leg(
        cfg,
        client,
        keypair,
        logger,
        tx_logger,
        stats,
        http_session,
        wallet_pubkey,
        sol_mint,
        usdc_mint,
        effective_usdc_amount,
    )

    if not second_ok:
        return False, stats, error_reason

    # 两步都成功
    logger.info("一对交易全部完成！")
    return True, stats, None


def execute_trade_pair(
    client: Client,
    keypair: Keypair,
    cfg: AppConfig,
    logger: logging.Logger,
    tx_logger: logging.Logger,
) -> tuple[bool, dict, Optional[str]]:
    """执行"一对交易" (SOL->USDC->SOL). 

    返回 (success, stats, error_reason):
    - success: 是否成功完成一对交易
    - stats: 参见 _execute_trade_pair_once 的返回说明
    - error_reason: 如果失败, 为本次失败的一句简要原因描述; 成功时为 None
    """

    try:
        return _execute_trade_pair_once(cfg, client, keypair, logger, tx_logger)

    except Exception as e:  # noqa: BLE001
        logger.error("执行一对交易时发生未捕获异常: %s", e)
        logger.debug("一对交易执行异常堆栈：\n%s", traceback.format_exc())
        return False, {
            "sol_spent_lamports": 0,
            "usdc_received_units": 0,
            "usdc_spent_units": 0,
            "sol_bought_lamports": 0,
        }, f"执行一对交易时发生未捕获异常: {e}"


def run_trading_loop(
    cfg: AppConfig,
    client: Client,
    keypair: Keypair,
    logger: logging.Logger,
    tx_logger: logging.Logger,
    wallet_pubkey: str,
) -> None:
    """自动交易循环. 
    
    每轮执行一对交易 (SOL->USDC->SOL)
    """

    logger.info(
        '开始进入自动交易循环, 每一轮都会执行"一对交易" (SOL->USDC->SOL), 按 Ctrl + C 可手动停止程序. '
    )
    logger.info(
        "配置: slippage_bps=%d (%.2f%%), priority_fee=%d lamports (%.9f SOL)",
        cfg.slippage_bps,
        cfg.slippage_bps / 100,
        cfg.priority_fee,
        cfg.priority_fee / 1_000_000_000,
    )

    success_pairs = 0
    unit_label = "一对交易"

    # 记录本轮中最后一次失败的交易的原因(如有)
    last_error_reason: Optional[str] = None

    # 本轮运行的累计统计数据
    total_sol_spent_lamports = 0
    total_usdc_received_units = 0
    total_usdc_spent_units = 0
    total_sol_bought_lamports = 0

    while success_pairs < cfg.max_runs:
        try:
            if not cfg.enable_trading:
                logger.info(
                    "当前配置已关闭真实交易(enable_trading=false), 本轮仅做心跳检查, 视为一对交易成功. "
                )

                pair_ok = True
                pair_stats = {
                    "sol_spent_lamports": 0,
                    "usdc_received_units": 0,
                    "usdc_spent_units": 0,
                    "sol_bought_lamports": 0,
                }
                pair_error = None
            else:
                logger.info("开始执行第 %d 对交易 (SOL->USDC->SOL)...", success_pairs + 1)

                # 每轮交易开始时记录钱包当前余额
                try:
                    sol_before_pair, usdc_before_pair = get_wallet_balances(
                        client,
                        wallet_pubkey,
                        USDC_MINT,
                        logger,
                        cfg.rpc_url,
                        cfg.retry_max_attempts,
                        cfg.retry_sleep_s,
                    )

                    tx_logger.info(
                        "余额快照 阶段=一对开始 钱包_SOL_lamports=%d 钱包_SOL=%.9f 钱包_USDC_units=%d 钱包_USDC=%.6f",
                        sol_before_pair,
                        sol_before_pair / 1_000_000_000,
                        usdc_before_pair,
                        usdc_before_pair / 1_000_000,
                    )

                    # 只有在成功获取余额的情况下才继续执行一轮交易
                    pair_ok, pair_stats, pair_error = execute_trade_pair(
                        client,
                        keypair,
                        cfg,
                        logger,
                        tx_logger,
                    )

                except Exception as e:  # noqa: BLE001
                    logger.error("获取一对交易开始前钱包余额失败: %s", e)
                    pair_ok = False
                    pair_stats = {
                        "sol_spent_lamports": 0,
                        "usdc_received_units": 0,
                        "usdc_spent_units": 0,
                        "sol_bought_lamports": 0,
                    }
                    pair_error = f"获取一对交易开始前钱包余额失败: {e}"

            if pair_ok:
                # 累加本轮统计数据
                total_sol_spent_lamports += int(pair_stats.get("sol_spent_lamports", 0))
                total_usdc_received_units += int(pair_stats.get("usdc_received_units", 0))
                total_usdc_spent_units += int(pair_stats.get("usdc_spent_units", 0))
                total_sol_bought_lamports += int(
                    pair_stats.get("sol_bought_lamports", 0)
                )
                # 成功后清空上一次的失败原因
                last_error_reason = None
            else:
                # 记录本次失败原因, 并在交易日志中输出
                last_error_reason = (
                    pair_error
                    or f"{unit_label}执行失败, 具体原因请查看 app_v2.log 中的上一条 ERROR 日志. "
                )
                tx_logger.info(
                    "本次 %s 失败 原因=%s",
                    unit_label,
                    last_error_reason,
                )

                # 如果是余额不足类错误, 视为致命错误, 立即终止自动交易循环
                if last_error_reason:
                    lower_reason = last_error_reason.lower()
                    if (
                        "余额不足" in last_error_reason
                        or "资金不足" in last_error_reason
                        or "insufficient" in lower_reason
                    ):
                        logger.error(
                            "检测到余额不足相关错误(原因=%s), 自动交易循环将立即终止, 不再继续后续轮次. ",
                            last_error_reason,
                        )
                        break
        except KeyboardInterrupt:
            # 用户主动终止程序
            logger.info("检测到用户中断(Ctrl + C), 程序即将优雅退出. ")
            break
        except Exception as e:  # noqa: BLE001
            # 兜底异常处理, 保证任何异常都有清晰日志
            logger.error("主循环中出现未捕获异常：%s", e)
            logger.debug("主循环未捕获异常堆栈：\n%s", traceback.format_exc())
            break

        if pair_ok:
            success_pairs += 1
            logger.info("第 %d 次 %s 执行成功. ", success_pairs, unit_label)
        else:
            logger.error(
                "本次 %s 执行失败, 不计入成功次数. 本轮将继续尝试下一次 %s. ",
                unit_label,
                unit_label,
            )
            # 失败后不终止整个循环, 继续下一轮
            continue

        if success_pairs >= cfg.max_runs:
            logger.info(
                "已完成配置要求的 %s 成功次数 max_runs=%d, 程序将退出. ",
                unit_label,
                cfg.max_runs,
            )
            break

        # 间隔一段时间后, 在下一笔交易前休眠一段时间
        logger.debug("本次一对交易结束, 休眠 %d 秒后继续下一笔交易. ", cfg.tx_interval_s)
        time.sleep(cfg.tx_interval_s)

    # 本轮运行结束后的统计输出
    if cfg.enable_trading:
        logger.info(
            "本轮统计: 成功 %s 次数=%d, 初始花费 SOL 总数=%.9f SOL (lamports=%d), "
            "获得 USDC 总数≈%.6f USDC (最小单位=%d), 花费 USDC 总数≈%.6f USDC (最小单位=%d), "
            "最终买回 SOL 总数=%.9f SOL (lamports=%d). ",
            unit_label,
            success_pairs,
            total_sol_spent_lamports / 1_000_000_000 if total_sol_spent_lamports else 0.0,
            total_sol_spent_lamports,
            total_usdc_received_units / 1_000_000 if total_usdc_received_units else 0.0,
            total_usdc_received_units,
            total_usdc_spent_units / 1_000_000 if total_usdc_spent_units else 0.0,
            total_usdc_spent_units,
            total_sol_bought_lamports / 1_000_000_000 if total_sol_bought_lamports else 0.0,
            total_sol_bought_lamports,
        )
        tx_logger.info(
            "本轮统计: 成功 %s 次数=%d, 初始花费 SOL 总数=%.9f SOL (lamports=%d), "
            "获得 USDC 总数≈%.6f USDC (最小单位=%d), 花费 USDC 总数≈%.6f USDC (最小单位=%d), "
            "最终买回 SOL 总数=%.9f SOL (lamports=%d). ",
            unit_label,
            success_pairs,
            total_sol_spent_lamports / 1_000_000_000 if total_sol_spent_lamports else 0.0,
            total_sol_spent_lamports,
            total_usdc_received_units / 1_000_000 if total_usdc_received_units else 0.0,
            total_usdc_received_units,
            total_usdc_spent_units / 1_000_000 if total_usdc_spent_units else 0.0,
            total_usdc_spent_units,
            total_sol_bought_lamports / 1_000_000_000 if total_sol_bought_lamports else 0.0,
            total_sol_bought_lamports,
        )

        if last_error_reason:
            logger.error("本轮存在交易失败, 失败原因: %s", last_error_reason)
            tx_logger.info("本轮存在交易失败, 失败原因: %s", last_error_reason)
    else:
        logger.info(
            "本轮统计: 当前 enable_trading=false, 未发送任何真实交易. 成功 %s 次数=%d, 统计的 SOL/USDC 数量均为 0. ",
            unit_label,
            success_pairs,
        )
        tx_logger.info(
            "本轮统计: 当前 enable_trading=false, 未发送任何真实交易. 成功 %s 次数=%d, 统计的 SOL/USDC 数量均为 0. ",
            unit_label,
            success_pairs,
        )


def run_auto_trader() -> None:
    """主入口：加载配置、初始化客户端并循环执行自动交易. """

    logger, tx_logger = setup_logging()
    logger.info("===== 启动 Solana 自动交易程序 (V2 - Swap API) =====")

    # 1. 加载配置
    try:
        cfg = load_config(CONFIG_PATH)
    except Exception as e:  # noqa: BLE001
        # 配置相关错误通常是最常见的问题, 需要给用户极其清晰的提示
        logger.error("加载配置失败：%s", e)
        logger.debug("加载配置异常堆栈：\n%s", traceback.format_exc())
        return

    logger.info(
        "当前配置：network=%s, rpc_url=%s, trade_amount_range=[%.9f, %.9f], tx_interval_s=%d, enable_trading=%s, max_runs=%d",
        cfg.network,
        cfg.rpc_url,
        cfg.trade_amount_min,
        cfg.trade_amount_max,
        cfg.tx_interval_s,
        cfg.enable_trading,
        cfg.max_runs,
    )
    logger.info(
        "Swap API 配置：slippage_bps=%d (%.2f%%), priority_fee=%d lamports (%.6f SOL)",
        cfg.slippage_bps,
        cfg.slippage_bps / 100,
        cfg.priority_fee,
        cfg.priority_fee / 1_000_000_000,
    )

    # 2. 初始化私钥
    try:
        keypair = load_keypair_from_base58(cfg.private_key)
    except Exception as e:  # noqa: BLE001
        logger.error("加载私钥失败：%s", e)
        logger.debug("加载私钥异常堆栈：\n%s", traceback.format_exc())
        return

    wallet_pubkey = str(keypair.pubkey())
    logger.info("私钥加载成功, 钱包地址：%s", wallet_pubkey)


    # 3. 初始化 RPC 客户端
    try:
        client = build_client(cfg)
        # 测试连通性
        version = client.get_version()
        logger.info("RPC 连接成功, 节点版本信息：%s", version)
    except Exception as e:  # noqa: BLE001
        logger.error("初始化 RPC 客户端或测试连接失败：%s", e)
        logger.debug("RPC 初始化异常堆栈：\n%s", traceback.format_exc())
        return

    # 4. 进入交易循环
    run_trading_loop(
        cfg=cfg,
        client=client,
        keypair=keypair,
        logger=logger,
        tx_logger=tx_logger,
        wallet_pubkey=wallet_pubkey,
    )

    logger.info("===== 程序结束, 感谢使用 =====")


if __name__ == "__main__":
    run_auto_trader()
