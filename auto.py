#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Solana 自动交易脚本示例。

功能特性：
- 通过配置支持主网 / 测试网（如需 Devnet 可自行配置 rpc_url）
- 账号、私钥等敏感信息全部通过配置文件加载
- 详细的调试日志（控制台 + 文件）
- 独立的交易记录日志
- 所有错误都输出清晰易懂的日志（包含堆栈方便排查）


使用说明（简要）：
1. 安装依赖：
   pip install -r requirements.txt
2. 复制 config_example.json 为 config.json，并按注释填写你的钱包信息
3. 运行脚本：
   python auto.py

注意：本脚本仅为演示自动交易框架，不构成任何投资建议。
在真实主网上使用前，请务必在测试网充分验证、控制金额、了解风险。
"""

import json
import logging
import logging.handlers
import os
import sys
import time
import traceback
import hashlib
from dataclasses import dataclass
from typing import Optional
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
from solana.transaction import Transaction
# from solders.instruction import Instruction, AccountMeta
# from solders.system_program import ID as SYSTEM_PROGRAM_ID
# from solders.message import MessageV0
# from spl.token.constants import TOKEN_PROGRAM_ID


# ===== 全局常量 =====
# 主网 SOL (WSOL) Mint 地址
SOL_MINT = "So11111111111111111111111111111111111111112"
# 主网 USDC Mint 地址
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# Jupiter Ultra API 基础 URL
# https://portal.jup.ag/api-keys
# JUPITER_ULTRA_API_BASE = "https://ultra-api.jup.ag"
JUPITER_ULTRA_API_BASE = "https://api.jup.ag/ultra/v1"
ULTRA_API_KEY = "72675114-cb71-4b8b-8c21-c429b1082843"
JUPITER_LEGACY_API_BASE = "https://api.jup.ag/swap/v1"


# 默认 RPC URL 常量，便于集中管理和修改
DEFAULT_MAINNET_RPC_URL = "https://solana-rpc.publicnode.com"
DEFAULT_TESTNET_RPC_URL = "https://solana-testnet.api.onfinality.io/public"


# Referral 配置 (用于收取集成商费用)
# referralAccount: 你的推荐账户公钥 (需要先在链上创建)
# referralFee: 费用基点 (50-255 bps, 例如 50 = 0.5%)
# REFERRAL_ACCOUNT_CONFIG = ""  # 填入你的 referralAccount 公钥，留空则自动创建

# # 运行时使用的 referral account (会在程序启动时设置)
# _referral_account: str = ""

# Jupiter Referral Program 配置
# Jupiter Ultra Referral Project 公钥 (固定值)
# JUPITER_REFERRAL_PROJECT_PUBKEY = "DkiqsTrw1u1bYFumumC7sCG2S8K25qc2vemJFHyW2wJc"
# Jupiter Referral Program ID
# JUPITER_REFERRAL_PROGRAM_ID = "REFER4ZgmyYx9c6He5XfaTMiGfdLwRnkV4RPp9t9iF3"
# Referral 账户名称 (用于创建时标识)
# REFERRAL_NAME = "solana_auto_trader"

# 可以通过环境变量 SOLANA_AUTO_CONFIG 覆盖配置文件路径
DEFAULT_CONFIG_PATH = "config.json"
CONFIG_PATH = os.environ.get("SOLANA_AUTO_CONFIG", DEFAULT_CONFIG_PATH)

# -------------------------- 2. 请求Legacy API获取交易指令 --------------------------
def get_swap_transaction():
    """请求Legacy Swap API，获取包含自定义priorityFee的交易指令"""
    headers = {"Content-Type": "application/json"}
    payload = {
        "inputMint": INPUT_MINT,
        "outputMint": OUTPUT_MINT,
        "amount": AMOUNT,
        "slippageBps": SLIPPAGE_BPS,
        "userPublicKey": USER_PUBKEY,
        # 核心：设置自定义优先费用（2种方式二选一，推荐方式1）
        # 方式1：直接设置具体优先费用金额（Lamports）
        "prioritizationFeeLamports": PRIORITY_FEE,
        # 方式2：设置计算单元价格（可选，与prioritizationFeeLamports二选一）
        # "computeUnitPriceMicroLamports": 1000  # 计算单元价格，用于自动计算优先费
    }
    
    try:
        response = requests.post(LEGACY_SWAP_API_URL, headers=headers, json=payload)
        response.raise_for_status()  # 抛出HTTP请求错误
        data = response.json()
        # 提取Base64编码的交易指令
        swap_tx_base64 = data.get("swapTransaction")
        if not swap_tx_base64:
            raise Exception("获取交易指令失败，未返回swapTransaction字段")
        return swap_tx_base64
    except Exception as e:
        print(f"请求交易指令出错：{str(e)}")
        raise

# -------------------------- 3. 解码、签名交易 --------------------------
def sign_transaction(swap_tx_base64):
    """解码交易指令，使用用户钱包签名交易"""
    # 解码Base64交易数据
    tx_data = base64.b64decode(swap_tx_base64)
    # 反序列化为Solana交易对象
    transaction = Transaction.deserialize(tx_data)
    # 使用用户私钥签名交易
    transaction.sign(USER_KEYPAIR)
    # 序列化签名后的交易（用于提交上链）
    signed_tx = transaction.serialize()
    # 转为Base64格式，用于提交API
    signed_tx_base64 = base64.b64encode(signed_tx).decode("utf-8")
    return signed_tx_base64

# -------------------------- 4. 提交交易到Solana网络 --------------------------
def submit_transaction(signed_tx_base64):
    """提交签名后的交易到Solana网络，返回交易ID"""
    try:
        # 提交交易（使用solana-web3.py客户端）
        response = SOLANA_CLIENT.send_raw_transaction(signed_tx_base64)
        tx_id = response.value
        print(f"交易提交成功！交易ID：{tx_id}")
        print(f"交易详情可查看：https://solscan.io/tx/{tx_id}?cluster=devnet")  # 测试网地址，主网替换为cluster=mainnet
        return tx_id
    except Exception as e:
        print(f"提交交易出错：{str(e)}")
        raise

# -------------------------- 5. 执行完整交易流程 --------------------------
def exec_legacy_tx():
    try:
        print("=== 开始执行带priorityFee的Swap交易 ===")
        # 1. 获取交易指令
        swap_tx_base64 = get_swap_transaction()
        print("✅ 成功获取交易指令")
        # 2. 签名交易
        signed_tx_base64 = sign_transaction(swap_tx_base64)
        print("✅ 成功签名交易")
        # 3. 提交交易
        tx_id = submit_transaction(signed_tx_base64)
    except Exception as e:
        print(f"❌ 交易执行失败：{str(e)}")

@dataclass
class AppConfig:
    """应用配置对象，便于在代码中类型提示和访问字段。"""

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
    # referral_fee: int  # Referral 费用基点（如生效，需在 50-255 范围内）
    # enable_referral: bool  # 是否启用 Referral 功能（包括使用 referral_fee 以及创建/获取 referral 账户）
    retry_max_attempts: int  # 外部 HTTP/RPC 调用的最大重试次数
    retry_sleep_s: int  # 外部 HTTP/RPC 重试间隔秒数




def setup_logging(log_dir: str = "logs") -> tuple[logging.Logger, logging.Logger]:
    """初始化日志系统。

    - 主日志 logger：打印到控制台 + 写入 logs/app.log
    - 交易日志 tx_logger：专门写入 logs/transactions.log
    """

    os.makedirs(log_dir, exist_ok=True)

    # 主日志
    logger = logging.getLogger("solana_auto")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    log_format = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    # 控制台日志（DEBUG 级别）
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # 文件日志（包含 DEBUG 级别）
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # 交易日志
    tx_logger = logging.getLogger("solana_auto.tx")
    tx_logger.setLevel(logging.INFO)
    tx_logger.handlers.clear()

    tx_file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "transactions.log"),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    tx_file_handler.setLevel(logging.INFO)
    tx_file_handler.setFormatter(log_format)
    tx_logger.addHandler(tx_file_handler)
    return logger, tx_logger


def load_config(path: str) -> AppConfig:
    """从 JSON 文件加载配置，并做基本校验和默认值处理。"""

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"配置文件不存在：{path}，请先复制 config_example.json 为 {path} 并按说明填写。"
        )

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # 网络类型
    network = raw.get("network", "testnet").lower()

    # RPC URL：可以显式配置，也可以根据 network 自动选择
    rpc_url = raw.get("rpc_url")
    if not rpc_url:
        if network == "mainnet":
            rpc_url = DEFAULT_MAINNET_RPC_URL
        elif network == "testnet":
            rpc_url = DEFAULT_TESTNET_RPC_URL
        else:
            raise ValueError(
                f"未知 network 类型：{network}，请使用 mainnet / testnet 之一。"
            )


    # 必填字段简单校验
    if "private_key" not in raw:
        raise ValueError("配置文件中缺少 private_key 字段（Base58 编码的私钥字符串）。")

    if "to_pubkey" not in raw:
        raise ValueError("配置文件中缺少 to_pubkey 字段（交易接收方地址）。")

    if "max_runs" not in raw:
        raise ValueError("配置文件中缺少 max_runs 字段 (必须是大于 0 的整数)。")

    try:
        max_runs_int = int(raw["max_runs"])
    except (TypeError, ValueError) as exc:
        raise ValueError("配置项 max_runs 必须是正整数 (大于 0)。") from exc

    if max_runs_int <= 0:
        raise ValueError(
            f"配置项 max_runs 的值无效: {max_runs_int}。请填写大于 0 的整数。"
        )

    max_runs = max_runs_int

    trade_mode_raw = "SOL_TO_USDC"

    if "trade_amount_min" not in raw or "trade_amount_max" not in raw:

        raise ValueError(
            "配置文件中缺少 trade_amount_min 或 trade_amount_max 字段 (必须是大于 0 的数字)。"
        )

    try:
        trade_amount_min = float(raw["trade_amount_min"])
        trade_amount_max = float(raw["trade_amount_max"])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "配置项 trade_amount_min / trade_amount_max 必须是数字类型, 且大于 0。"
        ) from exc

    if trade_amount_min <= 0 or trade_amount_max <= 0:
        raise ValueError("配置项 trade_amount_min / trade_amount_max 必须大于 0。")

    if trade_amount_min > trade_amount_max:
        raise ValueError(
            f"配置项 trade_amount_min={trade_amount_min} 大于 trade_amount_max={trade_amount_max}, 请填写正确的范围。"
        )

    # # Referral 费用基点，默认 3
    # referral_fee = int(raw.get("referral_fee", 3))

    # # 是否启用 Referral 功能（包括使用 referral_fee 以及创建/获取 referral 账户）
    # enable_referral = bool(raw.get("enable_referral", False))

    # 外部 HTTP/RPC 调用的重试策略
    retry_max_attempts = int(raw.get("retry_max_attempts", 10))
    retry_sleep_s = int(raw.get("retry_sleep_s", 2))

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
        # referral_fee=referral_fee,
        # enable_referral=enable_referral,
        retry_max_attempts=retry_max_attempts,
        retry_sleep_s=retry_sleep_s,
    )

    return cfg



def _execute_trade_pair_once(
    cfg: AppConfig,
    client: Client,
    keypair: Keypair,
    logger: logging.Logger,
    tx_logger: logging.Logger,
) -> tuple[bool, dict, Optional[str]]:

    """执行"一对交易": SOL -> USDC -> SOL。

    步骤:
    1. 使用配置中的 trade_amount (SOL 数量) 将 SOL 换成 USDC;
    2. 使用步骤 1 得到的 USDC 金额再换回 SOL。

    任一步骤失败, 都会立即终止本次"一对交易", 并返回 (False, stats)。
    全部成功时返回 (True, stats)。

    stats 字段包括:
    - sol_spent_lamports: 本次一对交易开始时花费的 SOL (lamports)
    - usdc_received_units: 第一步获得的 USDC 最小单位数量
    - usdc_spent_units: 第二步花费的 USDC 最小单位数量
    - sol_bought_lamports: 第二步最终买回的 SOL 数量 (lamports)
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
            "当前网络配置为 %s, 仅支持在 mainnet 上执行 SOL/USDC 互换。",
            cfg.network,
        )
        error_reason = (
            f"当前网络配置为 {cfg.network}, 仅支持在 mainnet 上执行 SOL/USDC 互换。"
        )
        return False, base_empty_stats.copy(), error_reason


    # 默认 mainnet RPC, 如 cfg.rpc_url 为空则使用官方 RPC
    rpc_url = cfg.rpc_url
    logger.info(f'rpc_url: {rpc_url}')

    # 使用全局常量
    sol_mint = SOL_MINT
    usdc_mint = USDC_MINT

    # 钱包公钥
    wallet_pubkey = str(keypair.pubkey())

    # 为本次一对交易随机生成 SOL 卖出数量（单位：SOL），只保留 3 位小数
    trade_amount_sol_raw = random.uniform(cfg.trade_amount_min, cfg.trade_amount_max)
    trade_amount_sol = round(trade_amount_sol_raw, 3)
    # 日志展示同样保留 3 位小数
    trade_amount_sol_display = trade_amount_sol


    amount_sol_lamports = int(trade_amount_sol * 1_000_000_000)
    if amount_sol_lamports <= 0:
        logger.error(
            "根据 trade_amount 范围 [%.9f, %.9f] 随机得到的本次 SOL 数量=%.9f, "
            "换算为最小单位数量=%d, 必须大于 0。",
            cfg.trade_amount_min,
            cfg.trade_amount_max,
            trade_amount_sol_display,
            amount_sol_lamports,
        )
        error_reason = (
            "根据 trade_amount 范围随机得到的本次 SOL 数量换算为最小单位后无效 (<= 0)。"
        )
        return False, base_empty_stats.copy(), error_reason


    # 本次一对交易内的统计数据（仅在两步都成功时才会被累计到总统计中）
    stats = base_empty_stats.copy()

    # 创建 requests Session，会自动使用系统代理
    http_session = requests.Session()
    # 设置超时时间
    timeout = 30

    first_ok, stats, error_reason, effective_usdc_amount = _execute_first_leg(
        cfg,
        client,
        keypair,
        logger,
        tx_logger,
        stats,
        http_session,
        timeout,
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

    # -------- 第一步: SOL -> USDC -------- 已在 _execute_first_leg 中完成










    # 以实际可用的 USDC 数量作为本次 USDC->SOL 的输入
    stats["usdc_spent_units"] = effective_usdc_amount

    # 调用第二步执行函数 (USDC -> SOL)
    second_ok, stats, error_reason = _execute_second_leg(
        cfg,
        client,
        keypair,
        logger,
        tx_logger,
        stats,
        http_session,
        timeout,
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




def load_keypair_from_base58(secret: str) -> Keypair:
    """从 Base58 编码的私钥字符串创建 Keypair。

    很多钱包（如 Phantom）导出的私钥是 Base58 字符串，而不是 64 个数字数组。
    这里统一使用 Base58，便于配置和复制粘贴。
    """

    try:
        secret_stripped = secret.strip()
        return Keypair.from_base58_string(secret_stripped)
    except Exception as e:  # noqa: BLE001 - 需要捕获所有错误并给出清晰提示
        raise ValueError(
            "私钥格式错误：请确认是 Base58 编码的 Solana 私钥字符串，且不要包含多余空格或换行。"
        ) from e



def build_client(cfg: AppConfig) -> Client:
    """根据配置创建 Solana RPC 客户端。"""

    return Client(cfg.rpc_url)


def get_usdc_balance(
    client: Client,
    owner_pubkey: str,
    usdc_mint: str,
    logger: logging.Logger,
    rpc_url: Optional[str] = None,
    retry_max_attempts: int = 10,
    retry_sleep_s: int = 2,
) -> int:

    """查询指定钱包当前的 USDC 余额（最小单位）。

    优先通过 solana-py 的 typed 响应获取，如果解析失败，则退回到原始 JSON-RPC 调用。
    """

    owner_pk = Pubkey.from_string(owner_pubkey)
    mint_pk = Pubkey.from_string(usdc_mint)

    # 使用 TokenAccountOpts，以兼容当前 solana-py 版本
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
        logger.warning(
            "通过 solana-py 获取 USDC 余额失败，将退回原始 JSON-RPC 调用: %s",
            e,
        )

        # 如果未显式传入 rpc_url，则尝试从 client 中获取
        if rpc_url is None:
            try:
                rpc_url = getattr(getattr(client, "_provider", None), "endpoint_uri", None)
            except Exception:  # noqa: BLE001
                rpc_url = None

        if not rpc_url:
            # 无法获得 RPC 地址，只能把异常抛出去
            raise

        # 退回到直接使用 JSON-RPC 请求，并增加重试机制
        last_exception: Optional[Exception] = None
        for attempt in range(1, retry_max_attempts + 1):

            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenAccountsByOwner",
                    "params": [
                        owner_pubkey,
                        {"mint": usdc_mint},
                        {"encoding": "jsonParsed"},
                    ],
                }
                http_resp = requests.post(rpc_url, json=payload, timeout=20)
                http_resp.raise_for_status()
                data = http_resp.json()

                result = data.get("result", {})
                value = result.get("value", [])
                last_exception = None
                break
            except Exception as e2:  # noqa: BLE001
                last_exception = e2
                if attempt >= retry_max_attempts:
                    logger.error(
                        "使用原始 RPC 请求获取 USDC 余额失败(已重试 %d 次): %s",
                        attempt,
                        e2,
                    )
                    raise
                logger.warning(
                    "使用原始 RPC 请求获取 USDC 余额失败(尝试 %d/%d): %s",
                    attempt,
                    retry_max_attempts,
                    e2,
                )
                time.sleep(retry_sleep_s)



    total = 0
    for item in value:
        try:
            amount_str = (
                item["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
            )
            total += int(amount_str)
        except Exception:  # noqa: BLE001
            logger.debug("解析 USDC 余额时跳过异常账户: %s", item)

    return total




# ==================== Referral Account 相关函数 ====================

# def derive_referral_account_pda(
#     partner_pubkey: Pubkey,
#     project_pubkey: Pubkey,
# ) -> tuple[Pubkey, int]:
#     """
#     派生 Referral Account 的 PDA 地址。
    
#     Seeds: ["referral", project_pubkey, partner_pubkey]
#     """
#     program_id = Pubkey.from_string(JUPITER_REFERRAL_PROGRAM_ID)
#     seeds = [
#         b"referral",
#         bytes(project_pubkey),
#         bytes(partner_pubkey),
#     ]
#     pda, bump = Pubkey.find_program_address(seeds, program_id)
#     return pda, bump


# def derive_referral_token_account_pda(
#     referral_account_pubkey: Pubkey,
#     mint: Pubkey,
# ) -> tuple[Pubkey, int]:
#     """
#     派生 Referral Token Account 的 PDA 地址。
    
#     Seeds: ["referral_ata", referral_account, mint]
#     """
#     program_id = Pubkey.from_string(JUPITER_REFERRAL_PROGRAM_ID)
#     seeds = [
#         b"referral_ata",
#         bytes(referral_account_pubkey),
#         bytes(mint),
#     ]
#     pda, bump = Pubkey.find_program_address(seeds, program_id)
#     return pda, bump


# def init_referral_account(
#     client: Client,
#     keypair: Keypair,
#     name: str,
#     logger: logging.Logger,
# ) -> Optional[str]:
#     """
#     创建 Referral Account (推荐账户)。
    
#     只需执行一次。如果账户已存在，则跳过创建。
    
#     返回: referral_account 公钥字符串，失败返回 None
#     """
#     partner_pubkey = keypair.pubkey()
#     project_pubkey = Pubkey.from_string(JUPITER_REFERRAL_PROJECT_PUBKEY)
#     program_id = Pubkey.from_string(JUPITER_REFERRAL_PROGRAM_ID)
    
#     # 派生 PDA
#     referral_account_pda, _bump = derive_referral_account_pda(partner_pubkey, project_pubkey)
    
#     logger.info("Referral Account PDA: %s", str(referral_account_pda))
    
#     # 检查账户是否已存在
#     try:
#         account_info = client.get_account_info(referral_account_pda)
#         if account_info.value is not None:
#             logger.info("Referral Account 已存在: %s", str(referral_account_pda))
#             return str(referral_account_pda)
#     except Exception as e:
#         logger.debug("检查 Referral Account 时出错: %s", e)
    
#     # 账户不存在，需要创建
#     logger.info("正在创建 Referral Account: %s", str(referral_account_pda))
    
#     # 构建 initializeReferralAccountWithName 指令
#     # 指令格式参考 Jupiter Referral SDK
#     # discriminator (8 bytes) + name (string with length prefix)
    
#     # initializeReferralAccountWithName 的 discriminator
#     # 这是 Anchor 程序的函数签名哈希的前8字节
#     discriminator = hashlib.sha256(b"global:initialize_referral_account_with_name").digest()[:8]
    
#     # 编码 name 字符串 (Borsh 格式: 4字节长度 + 字符串内容)
#     name_bytes = name.encode('utf-8')
#     name_len = len(name_bytes).to_bytes(4, 'little')
    
#     instruction_data = discriminator + name_len + name_bytes
    
#     # 账户列表
#     accounts = [
#         AccountMeta(pubkey=partner_pubkey, is_signer=True, is_writable=True),  # payer
#         AccountMeta(pubkey=partner_pubkey, is_signer=False, is_writable=False),  # partner
#         AccountMeta(pubkey=project_pubkey, is_signer=False, is_writable=False),  # project
#         AccountMeta(pubkey=referral_account_pda, is_signer=False, is_writable=True),  # referral_account
#         AccountMeta(pubkey=SYSTEM_PROGRAM_ID, is_signer=False, is_writable=False),  # system_program
#     ]
    
#     instruction = Instruction(
#         program_id=program_id,
#         accounts=accounts,
#         data=instruction_data,
#     )
    
#     # 获取最新 blockhash
#     try:
#         blockhash_resp = client.get_latest_blockhash()
#         recent_blockhash = blockhash_resp.value.blockhash
#     except Exception as e:
#         logger.error("获取 blockhash 失败: %s", e)
#         return None
    
#     # 构建交易
#     message = MessageV0.try_compile(
#         payer=partner_pubkey,
#         instructions=[instruction],
#         address_lookup_table_accounts=[],
#         recent_blockhash=recent_blockhash,
#     )
    
#     tx = VersionedTransaction(message, [keypair])
    
#     # 发送交易
#     try:
#         result = client.send_transaction(tx)
#         signature = str(result.value)
#         logger.info("Referral Account 创建交易已发送: %s", signature)
#         logger.info("Solscan: https://solscan.io/tx/%s", signature)
        
#         # 等待确认
#         time.sleep(10)
#         logger.info("Referral Account 创建成功: %s", str(referral_account_pda))
#         return str(referral_account_pda)
        
#     except Exception as e:
#         logger.error("创建 Referral Account 失败: %s", e)
#         logger.debug("异常堆栈:\n%s", traceback.format_exc())
#         return None


# def init_referral_token_account(
#     client: Client,
#     keypair: Keypair,
#     referral_account_pubkey: str,
#     mint: str,
#     logger: logging.Logger,
# ) -> Optional[str]:
#     """
#     创建 Referral Token Account (推荐代币账户)。
    
#     为指定的 Token Mint 创建代币账户，以便收取该种代币的手续费。
#     需要为每种想要收取费用的代币分别创建。
    
#     参数:
#         referral_account_pubkey: 上一步创建的 Referral Account 地址
#         mint: 想要收取费用的代币 Mint 地址 (如 SOL_MINT 或 USDC_MINT)
    
#     返回: referral_token_account 公钥字符串，失败返回 None
#     """
#     payer_pubkey = keypair.pubkey()
#     referral_account = Pubkey.from_string(referral_account_pubkey)
#     mint_pubkey = Pubkey.from_string(mint)
#     program_id = Pubkey.from_string(JUPITER_REFERRAL_PROGRAM_ID)
    
#     # 派生 Token Account PDA
#     referral_token_account_pda, _bump = derive_referral_token_account_pda(
#         referral_account, mint_pubkey
#     )
    
#     logger.info("Referral Token Account PDA for %s: %s", mint, str(referral_token_account_pda))
    
#     # 检查账户是否已存在
#     try:
#         account_info = client.get_account_info(referral_token_account_pda)
#         if account_info.value is not None:
#             logger.info("Referral Token Account 已存在: %s", str(referral_token_account_pda))
#             return str(referral_token_account_pda)
#     except Exception as e:
#         logger.debug("检查 Referral Token Account 时出错: %s", e)
    
#     # 账户不存在，需要创建
#     logger.info("正在创建 Referral Token Account for %s...", mint)
    
#     # 构建 initializeReferralTokenAccountV2 指令
#     discriminator = hashlib.sha256(b"global:initialize_referral_token_account_v2").digest()[:8]
    
#     # 该指令不需要额外数据，只有 discriminator
#     instruction_data = discriminator
    
#     # 账户列表 (参考 SDK 源码)
#     accounts = [
#         AccountMeta(pubkey=payer_pubkey, is_signer=True, is_writable=True),  # payer
#         AccountMeta(pubkey=referral_account, is_signer=False, is_writable=False),  # referral_account
#         AccountMeta(pubkey=referral_token_account_pda, is_signer=False, is_writable=True),  # referral_token_account
#         AccountMeta(pubkey=mint_pubkey, is_signer=False, is_writable=False),  # mint
#         AccountMeta(pubkey=SYSTEM_PROGRAM_ID, is_signer=False, is_writable=False),  # system_program
#         AccountMeta(pubkey=Pubkey.from_string(str(TOKEN_PROGRAM_ID)), is_signer=False, is_writable=False),  # token_program
#     ]
    
#     instruction = Instruction(
#         program_id=program_id,
#         accounts=accounts,
#         data=instruction_data,
#     )
    
#     # 获取最新 blockhash
#     try:
#         blockhash_resp = client.get_latest_blockhash()
#         recent_blockhash = blockhash_resp.value.blockhash
#     except Exception as e:
#         logger.error("获取 blockhash 失败: %s", e)
#         return None
    
#     # 构建交易
#     message = MessageV0.try_compile(
#         payer=payer_pubkey,
#         instructions=[instruction],
#         address_lookup_table_accounts=[],
#         recent_blockhash=recent_blockhash,
#     )
    
#     tx = VersionedTransaction(message, [keypair])
    
#     # 发送交易
#     try:
#         result = client.send_transaction(tx)
#         signature = str(result.value)
#         logger.info("Referral Token Account 创建交易已发送: %s", signature)
#         logger.info("Solscan: https://solscan.io/tx/%s", signature)
        
#         # 等待确认
#         time.sleep(10)
#         logger.info("Referral Token Account 创建成功: %s", str(referral_token_account_pda))
#         return str(referral_token_account_pda)
        
#     except Exception as e:
#         logger.error("创建 Referral Token Account 失败: %s", e)
#         logger.debug("异常堆栈:\n%s", traceback.format_exc())
#         return None


# def setup_referral_accounts(
#     client: Client,
#     keypair: Keypair,
#     logger: logging.Logger,
# ) -> Optional[str]:
#     """
#     设置 Referral 账户 (一次性操作)。
    
#     1. 创建 Referral Account
#     2. 为 SOL 创建 Referral Token Account
#     3. 为 USDC 创建 Referral Token Account
    
#     返回: referral_account 公钥字符串，失败返回 None
#     """
#     logger.info("===== 开始设置 Referral 账户 =====")
    
#     # 1. 创建 Referral Account
#     referral_account = init_referral_account(
#         client, keypair, REFERRAL_NAME, logger
#     )
    
#     if not referral_account:
#         logger.error("创建 Referral Account 失败")
#         return None
    
#     # 2. 为 SOL 创建 Referral Token Account
#     sol_token_account = init_referral_token_account(
#         client, keypair, referral_account, SOL_MINT, logger
#     )
    
#     if not sol_token_account:
#         logger.warning("创建 SOL Referral Token Account 失败，继续...")
    
#     # 3. 为 USDC 创建 Referral Token Account
#     usdc_token_account = init_referral_token_account(
#         client, keypair, referral_account, USDC_MINT, logger
#     )
    
#     if not usdc_token_account:
#         logger.warning("创建 USDC Referral Token Account 失败，继续...")
    
#     logger.info("===== Referral 账户设置完成 =====")
#     logger.info("Referral Account: %s", referral_account)
#     logger.info("请将此地址填入 REFERRAL_ACCOUNT_CONFIG 常量中以便下次直接使用")
    
#     return referral_account


# def get_or_create_referral_account(
#     client: Client,
#     keypair: Keypair,
#     logger: logging.Logger,
# ) -> Optional[str]:
#     """
#     获取或创建 Referral Account。
    
#     如果 REFERRAL_ACCOUNT_CONFIG 已配置，直接返回。
#     否则自动创建并返回新地址。
#     """
#     if REFERRAL_ACCOUNT_CONFIG:
#         logger.info("使用已配置的 Referral Account: %s", REFERRAL_ACCOUNT_CONFIG)
#         return REFERRAL_ACCOUNT_CONFIG

#     # 检查是否可以派生已存在的账户
#     partner_pubkey = keypair.pubkey()
#     project_pubkey = Pubkey.from_string(JUPITER_REFERRAL_PROJECT_PUBKEY)
#     referral_account_pda, _ = derive_referral_account_pda(partner_pubkey, project_pubkey)
    
#     logger.info("检查 Referral Account PDA: %s", str(referral_account_pda))
    
#     try:
#         account_info = client.get_account_info(referral_account_pda)
#         logger.debug("get_account_info 响应: %s", account_info)
#         if account_info.value is not None:
#             logger.info("发现已存在的 Referral Account: %s", str(referral_account_pda))
#             return str(referral_account_pda)
#         else:
#             logger.info("链上查询结果: 账户不存在 (value is None)")
#     except Exception as e:
#         logger.warning("查询 Referral Account 时出错: %s", e)
#         logger.debug("查询异常堆栈:\n%s", traceback.format_exc())
    
#     logger.info("未找到 Referral Account，将自动创建...")
#     return setup_referral_accounts(client, keypair, logger)



def get_wallet_balances(
    client: Client,
    owner_pubkey: str,
    usdc_mint: str,
    logger: logging.Logger,
    rpc_url: Optional[str] = None,
    retry_max_attempts: int = 10,
    retry_sleep_s: int = 2,
) -> tuple[int, int]:

    """查询指定钱包当前的 SOL / USDC 余额。

    返回 (sol_lamports, usdc_units)。
    """

    owner_pk = Pubkey.from_string(owner_pubkey)

    # 先通过 solana-py 获取 SOL 余额，如果解析失败则退回到原始 JSON-RPC 调用
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
        logger.warning("通过 solana-py 获取 SOL 余额失败，将退回原始 JSON-RPC 调用: %s", e)

        if rpc_url is None:
            try:
                rpc_url = getattr(getattr(client, "_provider", None), "endpoint_uri", None)
            except Exception:  # noqa: BLE001
                rpc_url = None

        if not rpc_url:
            # 无法获得 RPC 地址，只能把异常抛出去
            raise

        # 使用原始 JSON-RPC 调用作为兜底，并增加重试机制
        last_exception: Optional[Exception] = None
        for attempt in range(1, retry_max_attempts + 1):

            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getBalance",
                    "params": [owner_pubkey, {"commitment": "processed"}],
                }
                http_resp = requests.post(rpc_url, json=payload, timeout=20)
                http_resp.raise_for_status()
                data = http_resp.json()
                sol_lamports = int(data.get("result", {}).get("value", 0))
                last_exception = None
                break
            except Exception as e2:  # noqa: BLE001
                last_exception = e2
                if attempt >= retry_max_attempts:
                    logger.error(
                        "使用原始 RPC 请求获取 SOL 余额失败(已重试 %d 次): %s",
                        attempt,
                        e2,
                    )
                    raise
                logger.warning(
                    "使用原始 RPC 请求获取 SOL 余额失败(尝试 %d/%d): %s",
                    attempt,
                    retry_max_attempts,
                    e2,
                )
                time.sleep(retry_sleep_s)



    # 查询 USDC 余额（内部同样带有原始 JSON-RPC 兜底）
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
    """轮询指定交易签名的确认状态, 在给定超时时间内等待其达到 confirmed/finalized。

    返回 True 表示交易确认成功, False 表示超时或确认失败。
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
                        "confirmationStatus": getattr(
                            status, "confirmation_status", None
                        ),
                    }

                last_status = status
                err = status.get("err")
                conf_status = status.get("confirmationStatus") or status.get(
                    "confirmation_status"
                )

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



def _send_signed_tx_via_ultra_execute(
    cfg: AppConfig,
    logger: logging.Logger,
    http_session: requests.Session,
    timeout: int,
    execute_payload: dict,
    trade_label: str,
    insufficient_token_name: str,
    enable_decode_retry: bool = False,
) -> tuple[bool, Optional[dict], Optional[str]]:
    """发送已签名的交易到 Jupiter Ultra /execute, 带重试与余额不足检测。

    仅抽取重复结构, 不改变原有业务行为。
    """

    execute_url = f"{JUPITER_ULTRA_API_BASE}/execute"

    max_attempts = cfg.retry_max_attempts
    last_resp_text = ""

    resp = None
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ULTRA_API_KEY,
    }
    for attempt in range(1, max_attempts + 1):
        try:
            resp = http_session.post(
                execute_url,
                json=execute_payload,
                headers=headers,
                timeout=timeout,
            )
        except Exception as e:  # noqa: BLE001
            if attempt >= max_attempts:
                logger.error(
                    "执行 %s 交易请求失败(已重试 %d 次): %s",
                    trade_label,
                    attempt,
                    e,
                )
                error_reason = f"执行 {trade_label} 交易失败(请求异常): {e}"
                return False, None, error_reason
            logger.warning(
                "执行 %s 交易请求失败(尝试 %d/%d): %s",
                trade_label,
                attempt,
                max_attempts,
                e,
            )
        else:
            if resp.status_code == 200:
                break

            last_resp_text = resp.text or ""
            err_msg = last_resp_text

            lower_msg = err_msg.lower()
            if ("insufficient" in lower_msg and "fund" in lower_msg) or (
                "余额不足" in err_msg
            ):
                logger.error(
                    "执行 %s 交易失败，检测到可能的 %s 余额不足: %s",
                    trade_label,
                    insufficient_token_name,
                    err_msg,
                )
                error_reason = (
                    f"执行 {trade_label} 交易失败：{insufficient_token_name} 余额不足或资金不足。"
                )
                return False, None, error_reason

            logger.error(
                "执行 %s 交易失败(尝试 %d/%d), HTTP %d: %s",
                trade_label,
                attempt,
                max_attempts,
                resp.status_code,
                err_msg,
            )

            if (
                enable_decode_retry
                and resp.status_code == 400
                and "Failed to decode signed transaction" in err_msg
                and attempt < max_attempts
            ):
                logger.warning(
                    "检测到 Ultra 返回 'Failed to decode signed transaction', 准备重试 %s execute...",
                    trade_label,
                )
            else:
                if attempt >= max_attempts:
                    error_reason = (
                        f"执行 {trade_label} 交易失败, HTTP {resp.status_code}: {err_msg}"
                    )
                    return False, None, error_reason

        if attempt < max_attempts:
            time.sleep(cfg.retry_sleep_s)

    if resp is None or resp.status_code != 200:
        error_reason = (
            f"执行 {trade_label} 交易失败, HTTP {getattr(resp, 'status_code', '未知')}: {last_resp_text}"
        )
        return False, None, error_reason

    exec_result = resp.json()
    return True, exec_result, None



def _execute_first_leg(

    cfg: AppConfig,
    client: Client,
    keypair: Keypair,
    logger: logging.Logger,
    tx_logger: logging.Logger,
    stats: dict,
    http_session: requests.Session,
    timeout: int,
    wallet_pubkey: str,
    sol_mint: str,
    usdc_mint: str,
    trade_amount_sol_display: float,
    amount_sol_lamports: int,
) -> tuple[bool, dict, Optional[str], Optional[int]]:
    """执行第一步 SOL->USDC 交易。

    保持与原有业务逻辑一致, 仅作结构拆分以便维护。
    """

    error_reason: Optional[str] = None

    # -------- 第一步: SOL -> USDC --------
    logger.info(

        "一对交易-第 1 步: 准备将本次随机选取的 %.9f SOL 换为 USDC...",
        trade_amount_sol_display,
    )

    # 记录第一笔交易前的钱包余额（暂时注释掉，减少 RPC 调用）
    # try:
    #     sol_before_leg1, usdc_before_leg1 = get_wallet_balances(
    #         client, wallet_pubkey, usdc_mint, logger, cfg.rpc_url
    #     )
    #
    #     tx_logger.info(
    #         "余额快照 阶段=第一步前 钱包_SOL_lamports=%d 钱包_SOL=%.9f 钱包_USDC_units=%d 钱包_USDC=%.6f",
    #         sol_before_leg1,
    #         sol_before_leg1 / 1_000_000_000,
    #         usdc_before_leg1,
    #         usdc_before_leg1 / 1_000_000,
    #     )
    #
    # except Exception as e:  # noqa: BLE001
    #     logger.error("获取第一步前钱包余额失败: %s", e)

    # 调用 Jupiter Ultra API 获取订单（包含报价和交易）
    try:
        order_url = (
            f"{JUPITER_ULTRA_API_BASE}/order"
            f"?inputMint={sol_mint}"
            f"&outputMint={usdc_mint}"
            f"&amount={amount_sol_lamports}"
            f"&taker={wallet_pubkey}"
        )

        # # 可选：添加 referral 参数以收取集成商费用
        # if cfg.enable_referral and _referral_account:
        #     order_url += f"&referralAccount={_referral_account}"
        #     order_url += f"&referralFee={cfg.referral_fee}"

        logger.debug("请求 SOL->USDC 订单: %s", order_url)

        headers = {"x-api-key": ULTRA_API_KEY}
        max_attempts = cfg.retry_max_attempts

        resp = None
        last_resp_text = ""
        for attempt in range(1, max_attempts + 1):
            try:
                resp = http_session.get(order_url, headers=headers, timeout=timeout)
            except Exception as e:  # noqa: BLE001
                if attempt >= max_attempts:
                    logger.error(
                        "获取 SOL->USDC 订单失败(已重试 %d 次, 请求异常): %s",
                        attempt,
                        e,
                    )
                    error_reason = f"获取 SOL->USDC 订单失败(请求异常): {e}"
                    return False, stats, error_reason, None
                logger.warning(
                    "获取 SOL->USDC 订单失败(尝试 %d/%d, 请求异常): %s",
                    attempt,
                    max_attempts,
                    e,
                )
            else:
                if resp.status_code == 200:
                    break

                last_resp_text = resp.text or ""
                lower_msg = last_resp_text.lower()
                if ("insufficient" in lower_msg and "fund" in lower_msg) or (
                    "余额不足" in last_resp_text
                ):
                    logger.error(
                        "获取 SOL->USDC 订单失败，检测到可能的 SOL 余额不足: %s",
                        last_resp_text,
                    )
                    error_reason = "SOL 余额不足，无法创建 SOL->USDC 订单。"
                    return False, stats, error_reason, None

                if attempt >= max_attempts:
                    logger.error(
                        "获取 SOL->USDC 订单失败(已重试 %d 次), HTTP %d: %s",
                        attempt,
                        resp.status_code,
                        last_resp_text,
                    )
                    error_reason = (
                        f"获取 SOL->USDC 订单失败, HTTP {resp.status_code}: {last_resp_text}"
                    )
                    return False, stats, error_reason, None

                logger.warning(
                    "获取 SOL->USDC 订单失败(尝试 %d/%d), HTTP %d: %s",
                    attempt,
                    max_attempts,
                    resp.status_code,
                    last_resp_text,
                )

            time.sleep(2)

        if resp is None or resp.status_code != 200:
            error_reason = (
                f"获取 SOL->USDC 订单失败, HTTP {getattr(resp, 'status_code', '未知')}: {last_resp_text}"
            )
            return False, stats, error_reason, None

        order1 = resp.json()
    except Exception as e:  # noqa: BLE001
        logger.error("获取 SOL->USDC 订单失败: %s", e)
        logger.debug("SOL->USDC 订单异常堆栈:\n%s", traceback.format_exc())
        error_reason = f"获取 SOL->USDC 订单失败: {e}"
        return False, stats, error_reason, None

    out_usdc_amount = int(order1.get("outAmount", 0))
    if out_usdc_amount <= 0:
        logger.error(
            "SOL->USDC 订单返回的 outAmount 无效: %s, 完整响应: %s",
            order1.get("outAmount"),
            order1,
        )
        error_reason = (
            f"SOL->USDC 订单返回的 outAmount 无效: {order1.get('outAmount')}"
        )
        return False, stats, error_reason, None

    # 检查 API 是否返回错误
    if order1.get("errorCode") or order1.get("error"):
        error_msg = order1.get("errorMessage") or order1.get("error") or "未知错误"
        logger.error(
            "SOL->USDC 订单返回错误: errorCode=%s, errorMessage=%s",
            order1.get("errorCode"),
            error_msg,
        )
        error_reason = f"SOL->USDC 订单返回错误: {error_msg}"
        return False, stats, error_reason, None

    logger.info(
        "SOL->USDC 订单成功: 输入=%d lamports, 预计输出=%d USDC 最小单位。",
        amount_sol_lamports,
        out_usdc_amount,
    )

    # 记录本次一对交易中的 SOL/USDC 数量
    stats["sol_spent_lamports"] = amount_sol_lamports
    stats["usdc_received_units"] = out_usdc_amount
    stats["usdc_spent_units"] = out_usdc_amount

    # 从订单响应中获取交易数据并签名
    tx1_base64 = order1.get("transaction")
    if not tx1_base64 or tx1_base64 == "":
        error_msg = order1.get("errorMessage") or order1.get("error") or "transaction 字段为空"
        logger.error("SOL->USDC 订单响应中 transaction 为空: %s, 完整响应: %s", error_msg, order1)
        error_reason = f"SOL->USDC 订单响应中 transaction 为空: {error_msg}"
        return False, stats, error_reason, None

    try:
        raw_tx1 = VersionedTransaction.from_bytes(base64.b64decode(tx1_base64))
        sig1_bytes = keypair.sign_message(
            solders_message.to_bytes_versioned(raw_tx1.message)
        )
        signed_tx1 = VersionedTransaction.populate(raw_tx1.message, [sig1_bytes])
    except Exception as e:  # noqa: BLE001
        logger.error("签名 SOL->USDC 交易失败: %s", e)
        logger.debug("SOL->USDC 签名异常堆栈:\n%s", traceback.format_exc())
        error_reason = f"签名 SOL->USDC 交易失败: {e}"
        return False, stats, error_reason, None

    # 发送已签名的交易到 Jupiter Ultra API 执行
    try:
        signed_tx1_base64 = base64.b64encode(bytes(signed_tx1)).decode("utf-8")
        execute_payload = {
            "signedTransaction": signed_tx1_base64,
            "requestId": order1.get("requestId", ""),
        }
        logger.debug(
            "发送 SOL->USDC 交易到 Ultra API execute... len=%d prefix=%s",
            len(signed_tx1_base64),
            signed_tx1_base64[:32],
        )

        ok, exec_result1, error_reason = _send_signed_tx_via_ultra_execute(
            cfg=cfg,
            logger=logger,
            http_session=http_session,
            timeout=timeout,
            execute_payload=execute_payload,
            trade_label="SOL->USDC",
            insufficient_token_name="SOL",
            enable_decode_retry=True,
        )
        if not ok:
            return False, stats, error_reason, None

        sig1 = exec_result1.get("signature") or exec_result1.get("txid")
        if not sig1:
            logger.error("SOL->USDC 执行返回结果中未找到交易签名: %s", exec_result1)
            error_reason = f"SOL->USDC 执行返回结果中未找到交易签名: {exec_result1}"
            return False, stats, error_reason, None

        logger.info("一对交易-第 1 步完成, 交易签名: %s", sig1)

        tx_logger.info(
            (
                "步骤=1 类型=SOL_TO_USDC "
                "输入代币=SOL 输入数量_solana=%.9f 输入_lamports=%d "
                "输出代币=USDC 输出数量_usdc=%.6f 输出_units=%d "
                "签名=%s"
            ),
            trade_amount_sol_display,
            amount_sol_lamports,
            out_usdc_amount / 1_000_000,
            out_usdc_amount,
            sig1,
        )

    except Exception as e:  # noqa: BLE001
        logger.error("发送 SOL->USDC 交易失败: %s", e)
        logger.debug("SOL->USDC 发送异常堆栈:\n%s", traceback.format_exc())
        error_reason = f"发送 SOL->USDC 交易失败: {e}"
        return False, stats, error_reason, None


    # 在第一步 SOL->USDC 完成后, 等待交易在链上确认
    # 通过轮询交易签名的确认状态来判断是否上链成功, 而不是单纯依赖固定时间休眠
    confirm_timeout = max(cfg.tx_interval_s, 20)
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
            f"SOL->USDC 交易 {sig1} 在 {confirm_timeout} 秒内未确认或确认失败, 无法执行后续 USDC->SOL 交易。"
        )
        return False, stats, error_reason, None

    # 在执行第二步前查询当前 USDC 余额, 避免请求超过实际可用数量
    try:
        sol_after_leg1, current_usdc_balance = get_wallet_balances(
            client,
            wallet_pubkey,
            usdc_mint,
            logger,
            cfg.rpc_url,
            cfg.retry_max_attempts,
            cfg.retry_sleep_s,
        )

        logger.info(
            "当前 SOL 余额(最小单位) = %d, 当前 USDC 余额(最小单位) = %d",
            sol_after_leg1,
            current_usdc_balance,
        )
        if current_usdc_balance <= 0:
            logger.error(
                "当前 USDC 余额(%d) 小于等于 0, 无法执行 USDC->SOL 交易。",
                current_usdc_balance,
            )
            error_reason = "当前 USDC 余额不足, 无法执行 USDC->SOL 交易。"
            return False, stats, error_reason, None

        # 每笔交易后的余额快照(第 1 笔之后 / 第 2 笔之前)
        tx_logger.info(
            "余额快照 阶段=第一步后 钱包_SOL_lamports=%d 钱包_SOL=%.9f 钱包_USDC_units=%d 钱包_USDC=%.6f",
            sol_after_leg1,
            sol_after_leg1 / 1_000_000_000,
            current_usdc_balance,
            current_usdc_balance / 1_000_000,
        )

        # 本次 USDC->SOL 使用当前钱包中全部 USDC 余额
        effective_usdc_amount = current_usdc_balance

    except Exception as e:  # noqa: BLE001
        logger.error(
            "查询 USDC/SOL 余额失败: %s, 无法执行 USDC->SOL 交易。",
            e,
        )
        error_reason = f"查询 USDC/SOL 余额失败: {e}"
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
    timeout: int,
    wallet_pubkey: str,
    sol_mint: str,
    usdc_mint: str,
    effective_usdc_amount: int,
) -> tuple[bool, dict, Optional[str]]:
    """执行第二步 USDC->SOL 交易。

    保持与原有业务逻辑一致, 仅作结构拆分以便维护。
    """

    error_reason: Optional[str] = None

    # -------- 第二步: USDC -> SOL --------

    logger.info(
        "一对交易-第 2 步: 准备将上一步获得的 USDC 换回 SOL...",
    )

    # 调用 Jupiter Ultra API 获取订单
    try:
        order_url2 = (
            f"{JUPITER_ULTRA_API_BASE}/order"
            f"?inputMint={usdc_mint}"
            f"&outputMint={sol_mint}"
            f"&amount={effective_usdc_amount}"
            f"&taker={wallet_pubkey}"
        )

        # # 可选：添加 referral 参数以收取集成商费用
        # if cfg.enable_referral and _referral_account:
        #     order_url2 += f"&referralAccount={_referral_account}"
        #     order_url2 += f"&referralFee={cfg.referral_fee}"

        logger.debug("请求 USDC->SOL 订单: %s", order_url2)

        headers = {"x-api-key": ULTRA_API_KEY}
        max_attempts = cfg.retry_max_attempts

        resp = None
        last_resp_text = ""
        for attempt in range(1, max_attempts + 1):
            try:
                resp = http_session.get(order_url2, headers=headers, timeout=timeout)
            except Exception as e:  # noqa: BLE001
                if attempt >= max_attempts:
                    logger.error(
                        "获取 USDC->SOL 订单失败(已重试 %d 次, 请求异常): %s",
                        attempt,
                        e,
                    )
                    error_reason = f"获取 USDC->SOL 订单失败(请求异常): {e}"
                    return False, stats, error_reason
                logger.warning(
                    "获取 USDC->SOL 订单失败(尝试 %d/%d, 请求异常): %s",
                    attempt,
                    max_attempts,
                    e,
                )
            else:
                if resp.status_code == 200:
                    break

                last_resp_text = resp.text or ""
                lower_msg = last_resp_text.lower()
                if ("insufficient" in lower_msg and "fund" in lower_msg) or (
                    "余额不足" in last_resp_text
                ):
                    logger.error(
                        "获取 USDC->SOL 订单失败，检测到可能的 USDC 余额不足: %s",
                        last_resp_text,
                    )
                    error_reason = "USDC 余额不足，无法创建 USDC->SOL 订单。"
                    return False, stats, error_reason

                if attempt >= max_attempts:
                    logger.error(
                        "获取 USDC->SOL 订单失败(已重试 %d 次), HTTP %d: %s",
                        attempt,
                        resp.status_code,
                        last_resp_text,
                    )
                    error_reason = (
                        f"获取 USDC->SOL 订单失败, HTTP {resp.status_code}: {last_resp_text}"
                    )
                    return False, stats, error_reason

                logger.warning(
                    "获取 USDC->SOL 订单失败(尝试 %d/%d), HTTP %d: %s",
                    attempt,
                    max_attempts,
                    resp.status_code,
                    last_resp_text,
                )

            time.sleep(2)

        if resp is None or resp.status_code != 200:
            error_reason = (
                f"获取 USDC->SOL 订单失败, HTTP {getattr(resp, 'status_code', '未知')}: {last_resp_text}"
            )
            return False, stats, error_reason

        order2 = resp.json()

    except Exception as e:  # noqa: BLE001
        logger.error("获取 USDC->SOL 订单失败: %s", e)
        logger.debug("USDC->SOL 订单异常堆栈:\n%s", traceback.format_exc())
        error_reason = f"获取 USDC->SOL 订单失败: {e}"
        return False, stats, error_reason

    out_sol_lamports = int(order2.get("outAmount", 0))
    if out_sol_lamports <= 0:
        logger.error(
            "USDC->SOL 订单返回的 outAmount 无效: %s, 完整响应: %s",
            order2.get("outAmount"),
            order2,
        )
        error_reason = (
            f"USDC->SOL 订单返回的 outAmount 无效: {order2.get('outAmount')}"
        )
        return False, stats, error_reason

    # 检查 API 是否返回错误
    if order2.get("errorCode") or order2.get("error"):
        error_msg = order2.get("errorMessage") or order2.get("error") or "未知错误"
        logger.error(
            "USDC->SOL 订单返回错误: errorCode=%s, errorMessage=%s",
            order2.get("errorCode"),
            error_msg,
        )
        error_reason = f"USDC->SOL 订单返回错误: {error_msg}"
        return False, stats, error_reason

    logger.info(
        "USDC->SOL 订单成功: 输入=%d USDC 最小单位, 预计输出=%.9f SOL (lamports=%d)。",
        effective_usdc_amount,
        out_sol_lamports / 1_000_000_000,
        out_sol_lamports,
    )

    # 从订单响应中获取交易数据并签名
    tx2_base64 = order2.get("transaction")
    if not tx2_base64 or tx2_base64 == "":
        # 可能是资金不足或其他问题
        error_msg = (
            order2.get("errorMessage") or order2.get("error") or "transaction 字段为空"
        )
        logger.error("USDC->SOL 订单响应中 transaction 为空: %s, 完整响应: %s", error_msg, order2)
        error_reason = f"USDC->SOL 订单响应中 transaction 为空: {error_msg}"
        return False, stats, error_reason

    try:
        raw_tx2 = VersionedTransaction.from_bytes(base64.b64decode(tx2_base64))
        sig2_bytes = keypair.sign_message(
            solders_message.to_bytes_versioned(raw_tx2.message)
        )
        signed_tx2 = VersionedTransaction.populate(raw_tx2.message, [sig2_bytes])
    except Exception as e:  # noqa: BLE001
        logger.error("签名 USDC->SOL 交易失败: %s", e)
        logger.debug("USDC->SOL 签名异常堆栈:\n%s", traceback.format_exc())
        error_reason = f"签名 USDC->SOL 交易失败: {e}"
        return False, stats, error_reason

    # 发送已签名的交易到 Jupiter Ultra API 执行
    try:
        signed_tx2_base64 = base64.b64encode(bytes(signed_tx2)).decode("utf-8")
        execute_payload2 = {
            "signedTransaction": signed_tx2_base64,
            "requestId": order2.get("requestId", ""),
        }
        logger.debug("发送 USDC->SOL 交易到 Ultra API execute...")

        ok, exec_result2, error_reason = _send_signed_tx_via_ultra_execute(
            cfg=cfg,
            logger=logger,
            http_session=http_session,
            timeout=timeout,
            execute_payload=execute_payload2,
            trade_label="USDC->SOL",
            insufficient_token_name="USDC",
            enable_decode_retry=False,
        )
        if not ok:
            return False, stats, error_reason

        sig2 = exec_result2.get("signature") or exec_result2.get("txid")
        if not sig2:
            logger.error("USDC->SOL 执行返回结果中未找到交易签名: %s", exec_result2)
            error_reason = f"USDC->SOL 执行返回结果中未找到交易签名: {exec_result2}"
            return False, stats, error_reason


        # 第二步成功后, 记录最终买回的 SOL 数量
        stats["sol_bought_lamports"] = out_sol_lamports

        logger.info("一对交易-第 2 步完成, 交易签名: %s", sig2)
        tx_logger.info(
            (
                "步骤=2 类型=USDC_TO_SOL "
                "输入代币=USDC 输入数量_usdc=%.6f 输入_units=%d "
                "输出代币=SOL 输出数量_solana=%.9f 输出_lamports=%d "
                "签名=%s"
            ),
            effective_usdc_amount / 1_000_000,
            effective_usdc_amount,
            out_sol_lamports / 1_000_000_000,
            out_sol_lamports,
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
                "balance_snapshot context=leg2_after wallet_sol_lamports=%d wallet_sol=%.9f wallet_usdc_units=%d wallet_usdc=%.6f",
                sol_after_leg2,
                sol_after_leg2 / 1_000_000_000,
                usdc_after_leg2,
                usdc_after_leg2 / 1_000_000,
            )
        except Exception as e:  # noqa: BLE001
            logger.error("获取第二步后钱包余额失败: %s", e)

    except Exception as e:  # noqa: BLE001
        logger.error("发送 USDC->SOL 交易失败: %s", e)
        logger.debug("USDC->SOL 发送异常堆栈:\n%s", traceback.format_exc())
        error_reason = f"发送 USDC->SOL 交易失败: {e}"
        return False, stats, error_reason

    return True, stats, None



def execute_trade_pair(


    client: Client,  # 保留参数以兼容现有调用, 当前未直接使用
    keypair: Keypair,
    cfg: AppConfig,
    logger: logging.Logger,
    tx_logger: logging.Logger,
) -> tuple[bool, dict, Optional[str]]:

    """执行"一对交易" (SOL->USDC->SOL)。

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




def run_auto_trader() -> None:
    """主入口：加载配置、初始化客户端并循环执行自动交易。"""

    logger, tx_logger = setup_logging()
    logger.info("===== 启动 Solana 自动交易程序 =====")

    # 1. 加载配置
    try:
        cfg = load_config(CONFIG_PATH)
    except Exception as e:  # noqa: BLE001
        # 配置相关错误通常是最常见的问题，需要给用户极其清晰的提示
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


    # 2. 初始化私钥
    try:
        keypair = load_keypair_from_base58(cfg.private_key)
    except Exception as e:  # noqa: BLE001
        logger.error("加载私钥失败：%s", e)
        logger.debug("加载私钥异常堆栈：\n%s", traceback.format_exc())
        return

    # 6b2U9GpqJn8eXFTRFsV4D9XRf7HAx3YprF2qdWU1oqPt
    wallet_pubkey = str(keypair.pubkey())
    logger.info("私钥加载成功，钱包地址：%s", wallet_pubkey)


    # 3. 初始化 RPC 客户端
    try:
        client = build_client(cfg)
        # 测试连通性
        version = client.get_version()
        logger.info("RPC 连接成功，节点版本信息：%s", version)
    except Exception as e:  # noqa: BLE001
        logger.error("初始化 RPC 客户端或测试连接失败：%s", e)
        logger.debug("RPC 初始化异常堆栈：\n%s", traceback.format_exc())
        return

    # # 3.5 初始化或获取 Referral Account (用于收取集成商费用)
    # global _referral_account
    # if cfg.enable_referral:
    #     if not _referral_account:
    #         referral_account = get_or_create_referral_account(client, keypair, logger)
    #         if referral_account:
    #             _referral_account = referral_account
    #             logger.info("Referral Account 已设置: %s", _referral_account)
    #         else:
    #             logger.error(
    #                 "创建 Referral Account 失败，程序终止。请检查钱包 SOL 余额是否充足（建议至少 0.01 SOL）。"
    #             )
    #             return
    #     else:
    #         logger.info("使用预配置的 Referral Account: %s", _referral_account)
    # else:
    #     logger.info("当前配置已关闭 Referral 功能，不会创建或使用推荐账户。")


    # 4. 自动交易循环
    logger.info('开始进入自动交易循环，每一轮都会执行"一对交易" (SOL->USDC->SOL)，按 Ctrl + C 可手动停止程序。')

    success_pairs = 0

    # 记录本轮中最后一次失败的一对交易的原因（如有）
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
                    "当前配置已关闭真实交易（enable_trading=false），本轮仅做心跳检查, 视为一对交易成功。"
                )
                pair_ok = True
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

                    # 只有在成功获取余额的情况下才继续执行一对交易
                    pair_ok, pair_stats, pair_error = execute_trade_pair(
                        client, keypair, cfg, logger, tx_logger
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
                    total_sol_spent_lamports += int(
                        pair_stats.get("sol_spent_lamports", 0)
                    )
                    total_usdc_received_units += int(
                        pair_stats.get("usdc_received_units", 0)
                    )
                    total_usdc_spent_units += int(
                        pair_stats.get("usdc_spent_units", 0)
                    )
                    total_sol_bought_lamports += int(
                        pair_stats.get("sol_bought_lamports", 0)
                    )
                    # 成功后清空上一次的失败原因
                    last_error_reason = None
                else:
                    # 记录本次失败原因, 并在交易日志中输出
                    last_error_reason = (
                        pair_error
                        or "一对交易执行失败, 具体原因请查看 app.log 中的上一条 ERROR 日志。"
                    )
                    tx_logger.info(
                        "本次一对交易失败 原因=%s",
                        last_error_reason,
                    )



        except KeyboardInterrupt:
            # 用户主动终止程序
            logger.info("检测到用户中断（Ctrl + C），程序即将优雅退出。")
            break
        except Exception as e:  # noqa: BLE001
            # 兜底异常处理，保证任何异常都有清晰日志
            logger.error("主循环中出现未捕获异常：%s", e)
            logger.debug("主循环未捕获异常堆栈：\n%s", traceback.format_exc())
            break

        # 记录一对交易结束后的钱包余额（暂时注释掉，减少 RPC 调用）
        # if cfg.enable_trading:
        #     try:
        #         sol_after_pair, usdc_after_pair = get_wallet_balances(
        #             client,
        #             wallet_pubkey,
        #             USDC_MINT,
        #             logger,
        #             cfg.rpc_url,
        #         )
        #
        #         tx_logger.info(
        #             "余额快照 阶段=一对结束 钱包_SOL_lamports=%d 钱包_SOL=%.9f 钱包_USDC_units=%d 钱包_USDC=%.6f",
        #             sol_after_pair,
        #             sol_after_pair / 1_000_000_000,
        #             usdc_after_pair,
        #             usdc_after_pair / 1_000_000,
        #         )
        #
        #     except Exception as e:  # noqa: BLE001
        #         logger.error("获取一对交易结束后钱包余额失败: %s", e)

        if pair_ok:
            success_pairs += 1
            logger.info("第 %d 对交易执行成功。", success_pairs)
        else:
            logger.error(
                "本次一对交易执行失败, 不计入成功次数。本轮将继续尝试下一对交易（当前为测试模式，不终止循环）。"
            )
            # 测试阶段: 一对交易失败后不再终止整个循环, 而是继续下一轮
            continue



        if success_pairs >= cfg.max_runs:
            logger.info(
                "已完成配置要求的一对交易成功次数 max_runs=%d, 程序将退出。",
                cfg.max_runs,
            )
            break

        # 间隔一段时间后, 在下一笔交易前休眠一段时间
        logger.debug("本次一对交易结束, 休眠 %d 秒后继续下一笔交易。", cfg.tx_interval_s)
        time.sleep(cfg.tx_interval_s)


    # 5. 本轮运行结束后的统计输出
    if cfg.enable_trading:
        logger.info(
            "本轮统计: 成功一对交易次数=%d, 初始花费 SOL 总数=%.9f SOL (lamports=%d), "
            "获得 USDC 总数≈%.6f USDC (最小单位=%d), 花费 USDC 总数≈%.6f USDC (最小单位=%d), "
            "最终买回 SOL 总数=%.9f SOL (lamports=%d)。",
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
            "本轮统计: 成功一对交易次数=%d, 初始花费 SOL 总数=%.9f SOL (lamports=%d), "
            "获得 USDC 总数≈%.6f USDC (最小单位=%d), 花费 USDC 总数≈%.6f USDC (最小单位=%d), "
            "最终买回 SOL 总数=%.9f SOL (lamports=%d)。",
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
            "本轮统计: 当前 enable_trading=false, 未发送任何真实交易。成功一对次数=%d, 统计的 SOL/USDC 数量均为 0。",
            success_pairs,
        )
        tx_logger.info(
            "本轮统计: 当前 enable_trading=false, 未发送任何真实交易。成功一对次数=%d, 统计的 SOL/USDC 数量均为 0。",
            success_pairs,
        )


    logger.info("===== 程序结束，感谢使用 =====")


if __name__ == "__main__":
    run_auto_trader()
