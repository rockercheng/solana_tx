#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Solana 自动交易脚本 - V2 版本 (使用 Jupiter Swap API)

功能特性: 
- 使用 Jupiter Swap API (GET /quote + POST /swap) 替代 Ultra API
- 支持用户自定义 slippage_bps (滑点) 和 priority_fee (优先费)
- 通过配置支持主网 / 测试网(如需 Devnet 可自行配置 rpc_url)
- 账号、私钥等敏感信息全部通过配置文件加载
- 详细的调试日志(控制台 + 文件)
- 独立的交易记录日志
- 所有错误都输出清晰易懂的日志(包含堆栈方便排查)


使用说明(简要): 
1. 安装依赖: 
   pip install -r requirements.txt
2. 复制 config_example.json 为 config.json, 并按注释填写你的钱包信息
3. 运行脚本: 
   python auto_v2.py

注意: 本脚本仅为演示自动交易框架, 不构成任何投资建议. 
在真实主网上使用前, 请务必在测试网充分验证、控制金额、了解风险. 
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import traceback
from dataclasses import dataclass
from typing import Optional, Callable
import base64
import base58
import hashlib
import random
import requests
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders import message as solders_message
from solders.pubkey import Pubkey
from solana.rpc.api import Client
from solana.rpc.types import TokenAccountOpts, TxOpts

# ===== 全局常量 =====
# Associated Token Program ID
ASSOCIATED_TOKEN_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
# Token Program ID
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
# 主网 SOL (WSOL) Mint 地址
SOL_MINT = "So11111111111111111111111111111111111111112"
# 主网 USDC Mint 地址
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# Jupiter Swap API 基础 URL
# https://portal.jup.ag/api-keys
JUPITER_SWAP_API_BASE = "https://api.jup.ag/swap/v1"
SWAP_API_KEY = "72675114-cb71-4b8b-8c21-c429b1082843"

# 默认 RPC URL 常量, 便于集中管理和修改
# 备用 "rpc_url": "https://api.mainnet-beta.solana.com",
DEFAULT_MAINNET_RPC_URL = "https://solana-rpc.publicnode.com"
DEFAULT_TESTNET_RPC_URL = "https://solana-testnet.api.onfinality.io/public"


# 可以通过环境变量 SOLANA_AUTO_CONFIG 覆盖配置文件路径
DEFAULT_CONFIG_PATH = "config.json"
CONFIG_PATH = os.environ.get("SOLANA_AUTO_CONFIG", DEFAULT_CONFIG_PATH)


class RpcUrlManager:
    """RPC URL 管理器，支持多个 URL 轮换和失败统计。"""
    
    def __init__(self, urls: list[str], logger: logging.Logger):
        self.urls = urls
        self.logger = logger
        self.current_index = 0
        # 记录每个 URL 的失败次数
        self.fail_counts: dict[str, int] = {}
    
    @property
    def current_url(self) -> str:
        """获取当前使用的 RPC URL。"""
        return self.urls[self.current_index]
    
    def switch_to_next(self) -> str:
        """切换到下一个 RPC URL，返回新的 URL。"""
        if len(self.urls) <= 1:
            return self.current_url
        
        old_url = self.current_url
        self.current_index = (self.current_index + 1) % len(self.urls)
        new_url = self.current_url
        self.logger.info("切换 RPC URL: %s -> %s", self._short_url(old_url), self._short_url(new_url))
        return new_url
    
    def record_failure(self, url: Optional[str] = None) -> None:
        """记录指定 URL 的失败次数。如果未指定则记录当前 URL。"""
        if url is None:
            url = self.current_url
        self.fail_counts[url] = self.fail_counts.get(url, 0) + 1
    
    def get_fail_stats(self) -> dict[str, int]:
        """获取有失败记录的 URL 统计（过滤掉失败次数为 0 的）。"""
        return {url: count for url, count in self.fail_counts.items() if count > 0}
    
    def _short_url(self, url: str) -> str:
        """返回 URL 的简短形式，便于日志显示。"""
        # 提取域名部分
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return parsed.netloc
        except Exception:
            return url[:50] + "..." if len(url) > 50 else url


@dataclass
class AppConfig:
    """应用配置对象, 便于在代码中类型提示和访问字段. """

    network: str  # mainnet / testnet
    rpc_urls: list[str]  # RPC URL 列表，支持多个 URL 轮换
    private_key: str  # Base58 编码的私钥字符串
    from_pubkey: Optional[str]
    trade_mode: str  # "SOL_TO_USDC" 或 "USDC_TO_SOL"
    trade_amount_min: float
    trade_amount_max: float
    tx_interval_s: int
    enable_trading: bool
    max_runs: int
    retry_max_attempts: int  # 交易失败时的最大重试次数
    retry_sleep_s: int  # 交易重试间隔秒数
    slippage_bps: int  # Swap API 使用的滑点(单位: bps, 50 = 0.5%)
    priority_fee: int  # Swap API 使用的优先费(单位: lamports)
    
    @property
    def rpc_url(self) -> str:
        """兼容属性：返回第一个 RPC URL。"""
        return self.rpc_urls[0] if self.rpc_urls else DEFAULT_MAINNET_RPC_URL


def _backup_log_file_if_exists(log_path: str) -> Optional[str]:
    """如果日志文件存在且不为空, 则备份为带时间戳的文件.
    
    Args:
        log_path: 日志文件路径
        
    Returns:
        备份后的文件路径, 如果没有备份则返回 None
    """
    if not os.path.exists(log_path):
        return None
    
    # 检查文件是否为空
    if os.path.getsize(log_path) == 0:
        return None
    
    # 获取文件的修改时间作为备份时间戳
    mtime = os.path.getmtime(log_path)
    from datetime import datetime
    timestamp = datetime.fromtimestamp(mtime).strftime("%Y%m%d_%H%M%S")
    
    # 构造备份文件名: app_v2.log -> app_v2_20260207_134036.log
    base_name = os.path.basename(log_path)
    dir_name = os.path.dirname(log_path)
    name_part, ext_part = os.path.splitext(base_name)
    backup_name = f"{name_part}_{timestamp}{ext_part}"
    backup_path = os.path.join(dir_name, backup_name)
    
    # 如果备份文件已存在(同一秒内多次运行), 添加序号
    counter = 1
    while os.path.exists(backup_path):
        backup_name = f"{name_part}_{timestamp}_{counter}{ext_part}"
        backup_path = os.path.join(dir_name, backup_name)
        counter += 1
    
    # 重命名原文件为备份文件
    os.rename(log_path, backup_path)
    return backup_path


def setup_logging(log_dir: str = "logs") -> tuple[logging.Logger, logging.Logger]:
    """初始化日志系统. 
    - 主日志 logger: 打印到控制台 + 写入 logs/app_v2.log
    - 交易日志 tx_logger: 专门写入 logs/transactions_v2.log
    - 每次运行前自动备份已存在的日志文件(加日期时间后缀)
    """

    os.makedirs(log_dir, exist_ok=True)
    
    # 备份已存在的日志文件
    app_log_path = os.path.join(log_dir, "app_v2.log")
    tx_log_path = os.path.join(log_dir, "transactions_v2.log")
    
    backup_app = _backup_log_file_if_exists(app_log_path)
    backup_tx = _backup_log_file_if_exists(tx_log_path)

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

    # 文件日志(包含 DEBUG 级别) - 不再使用 RotatingFileHandler, 每次运行使用新文件
    file_handler = logging.FileHandler(
        app_log_path,
        mode="w",  # 覆盖模式, 因为旧文件已备份
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

    tx_file_handler = logging.FileHandler(
        tx_log_path,
        mode="w",  # 覆盖模式, 因为旧文件已备份
        encoding="utf-8",
    )
    tx_file_handler.setLevel(logging.INFO)
    tx_file_handler.setFormatter(log_format)
    tx_logger.addHandler(tx_file_handler)
    
    # 输出备份信息到日志
    if backup_app:
        logger.info("已备份上次运行日志: %s", os.path.basename(backup_app))
    if backup_tx:
        logger.info("已备份上次交易日志: %s", os.path.basename(backup_tx))
    
    return logger, tx_logger


def load_config(path: str) -> AppConfig:
    """从 JSON 文件加载配置, 并做基本校验和默认值处理. """

    if not os.path.exists(path):
        raise FileNotFoundError(f"配置文件不存在: {path}, 请先复制 config_example.json 为 {path} 并按说明填写. ")

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    # 网络类型
    network = raw.get("network", "testnet").lower()

    # RPC URL 列表: 支持 rpc_urls (列表) 或 rpc_url (单个字符串) 两种配置方式
    rpc_urls_raw = raw.get("rpc_urls")
    rpc_url_single = raw.get("rpc_url")
    
    rpc_urls: list[str] = []
    if rpc_urls_raw:
        # 优先使用 rpc_urls 列表
        if isinstance(rpc_urls_raw, list):
            rpc_urls = [url.strip() for url in rpc_urls_raw if url and url.strip()]
        elif isinstance(rpc_urls_raw, str):
            rpc_urls = [rpc_urls_raw.strip()]
    elif rpc_url_single:
        # 兼容旧的 rpc_url 单字符串配置
        rpc_urls = [rpc_url_single.strip()]
    
    # 如果没有配置任何 RPC URL，使用默认值
    if not rpc_urls:
        if network == "mainnet":
            rpc_urls = [DEFAULT_MAINNET_RPC_URL]
        elif network == "testnet":
            rpc_urls = [DEFAULT_TESTNET_RPC_URL]
        else:
            raise ValueError(f"未知 network 类型: {network}, 请使用 mainnet / testnet 之一. ")


    # 必填字段简单校验
    if "private_key" not in raw:
        raise ValueError("配置文件中缺少 private_key 字段(Base58 编码的私钥字符串). ")

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

    # 重试策略配置
    retry_max_attempts = int(raw.get("retry_max_attempts", 10))
    retry_sleep_s = int(raw.get("retry_sleep_s", 2))

    # Swap API 相关配置
    # slippage_bps: 滑点 (单位: bps, 50 = 0.5%, 100 = 1%)
    slippage_bps = int(raw.get("slippage_bps", 20))
    # priority_fee: 优先费 (单位: lamports, 10000 = 0.00001 SOL)
    priority_fee = int(raw.get("priority_fee", 10000))

    cfg = AppConfig(
        network=network,
        rpc_urls=rpc_urls,
        private_key=str(raw["private_key"]),
        from_pubkey=raw.get("from_pubkey") or None,
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
    """在 trade_amount_min / trade_amount_max 范围内随机选择本次交易的 SOL 数量(单位: SOL). """
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
            "私钥格式错误: 请确认是 Base58 编码的 Solana 私钥字符串, 且不要包含多余空格或换行. "
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
                # 如果经历过重试才成功, 额外输出一条成功日志
                if attempt > 1:
                    retry_count = attempt - 1
                    logger.info(
                        "%s在第 %d 次重试后成功(共尝试 %d 次)",
                        label,
                        retry_count,
                        attempt,
                    )
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
                    raise RuntimeError(f"{label}失败: 余额不足或资金不足. ")

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


# ===== ATA 地址缓存 =====
# 缓存已计算的 Associated Token Account 地址，避免重复计算
# key: (owner_pubkey, mint) -> value: ata_address
_ata_address_cache: dict[tuple[str, str], str] = {}


def get_associated_token_address(owner_pubkey: str, mint: str) -> str:
    """计算 Associated Token Account (ATA) 地址.
    
    ATA 是通过 PDA (Program Derived Address) 派生的确定性地址，
    使用 owner + mint + token_program 作为种子。
    
    Args:
        owner_pubkey: 钱包所有者公钥 (Base58)
        mint: 代币 Mint 地址 (Base58)
        
    Returns:
        ATA 地址 (Base58)
    """
    cache_key = (owner_pubkey, mint)
    
    # 检查缓存
    if cache_key in _ata_address_cache:
        return _ata_address_cache[cache_key]
    
    # 计算 PDA
    # Seeds: [owner_pubkey, token_program_id, mint]
    owner_bytes = base58.b58decode(owner_pubkey)
    token_program_bytes = base58.b58decode(TOKEN_PROGRAM_ID)
    mint_bytes = base58.b58decode(mint)
    associated_program_bytes = base58.b58decode(ASSOCIATED_TOKEN_PROGRAM_ID)
    
    # PDA 派生: sha256(seeds + program_id + "ProgramDerivedAddress")
    # 尝试从 bump = 255 开始递减，直到找到一个不在 ed25519 曲线上的点
    for bump in range(255, -1, -1):
        seeds = owner_bytes + token_program_bytes + mint_bytes + bytes([bump]) + associated_program_bytes + b"ProgramDerivedAddress"
        hash_result = hashlib.sha256(seeds).digest()
        
        # 检查是否在曲线上（简化检查：尝试解码为公钥）
        try:
            # 如果能成功创建 Pubkey，说明在曲线上，需要继续尝试
            Pubkey.from_bytes(hash_result)
            continue
        except Exception:
            # 不在曲线上，这是有效的 PDA
            ata_address = base58.b58encode(hash_result).decode("utf-8")
            _ata_address_cache[cache_key] = ata_address
            return ata_address
    
    raise RuntimeError(f"无法计算 ATA 地址: owner={owner_pubkey}, mint={mint}")


def get_ata_address_via_solders(owner_pubkey: str, mint: str) -> str:
    """使用 solders 库计算 ATA 地址（更可靠的方法）.
    
    Args:
        owner_pubkey: 钱包所有者公钥 (Base58)
        mint: 代币 Mint 地址 (Base58)
        
    Returns:
        ATA 地址 (Base58)
    """
    cache_key = (owner_pubkey, mint)
    
    # 检查缓存
    if cache_key in _ata_address_cache:
        return _ata_address_cache[cache_key]
    
    owner_pk = Pubkey.from_string(owner_pubkey)
    mint_pk = Pubkey.from_string(mint)
    token_program_pk = Pubkey.from_string(TOKEN_PROGRAM_ID)
    associated_program_pk = Pubkey.from_string(ASSOCIATED_TOKEN_PROGRAM_ID)
    
    # 使用 Pubkey.find_program_address 计算 PDA
    seeds = [bytes(owner_pk), bytes(token_program_pk), bytes(mint_pk)]
    ata_pk, _bump = Pubkey.find_program_address(seeds, associated_program_pk)
    
    ata_address = str(ata_pk)
    _ata_address_cache[cache_key] = ata_address
    return ata_address


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

    使用 getTokenAccountBalance API 查询 ATA 账户余额.
    ATA 地址会被缓存，避免重复计算.
    """

    # 如果未显式传入 rpc_url, 则尝试从 client 中获取
    if rpc_url is None:
        try:
            rpc_url = getattr(getattr(client, "_provider", None), "endpoint_uri", None)
        except Exception:  # noqa: BLE001
            rpc_url = None

    if not rpc_url:
        # 无法获得 RPC 地址, 只能把异常抛出去
        raise RuntimeError("获取 USDC 余额失败: 无法确定 RPC URL")

    # 计算 ATA 地址（会使用缓存）
    try:
        ata_address = get_ata_address_via_solders(owner_pubkey, usdc_mint)
        logger.debug("USDC ATA 地址: %s (owner=%s)", ata_address, owner_pubkey)
    except Exception as e:
        logger.error("计算 USDC ATA 地址失败: %s", e)
        raise RuntimeError(f"计算 USDC ATA 地址失败: {e}") from e

    # 使用 getTokenAccountBalance 查询余额
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountBalance",
        "params": [
            ata_address,
            {"commitment": "confirmed"},
        ],
    }

    http_resp = http_request_with_retry(
        cfg=None,
        logger=logger,
        label="使用 getTokenAccountBalance 获取 USDC 余额",
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
    
    # 检查是否有错误（账户不存在时会返回错误）
    if "error" in data:
        error_msg = data.get("error", {}).get("message", "未知错误")
        # 账户不存在通常意味着余额为 0
        if "could not find" in error_msg.lower() or "not found" in error_msg.lower():
            logger.debug("USDC ATA 账户不存在，余额为 0: %s", ata_address)
            return 0
        logger.warning("查询 USDC 余额返回错误: %s", error_msg)
        return 0
    
    result = data.get("result", {})
    value = result.get("value", {})
    
    if not value:
        # ATA 账户可能不存在，返回 0
        logger.debug("USDC ATA 账户可能不存在，余额为 0")
        return 0
    
    try:
        amount_str = value.get("amount", "0")
        return int(amount_str)
    except Exception as parse_e:  # noqa: BLE001
        logger.debug("解析 USDC 余额失败: %s, 错误: %s", value, parse_e)
        return 0



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
    
    使用 getTokenAccountBalance API 查询 ATA 账户余额.
    在第 1 步交易确认后, RPC 节点可能还未同步最新状态,
    因此需要多次重试以获取正确的 USDC 余额.
    """
    
    # 计算 ATA 地址（会使用缓存，不会重复计算）
    try:
        ata_address = get_ata_address_via_solders(owner_pubkey, usdc_mint)
    except Exception as e:
        logger.error("计算 USDC ATA 地址失败: %s", e)
        raise RuntimeError(f"计算 USDC ATA 地址失败: {e}") from e
    
    for attempt in range(1, retry_max_attempts + 1):
        try:
            # 使用 getTokenAccountBalance 查询余额
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTokenAccountBalance",
                "params": [
                    ata_address,
                    {"commitment": "confirmed"},
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
            
            # 检查是否有错误（账户不存在时会返回错误）
            if "error" in data:
                error_msg = data.get("error", {}).get("message", "未知错误")
                if "could not find" in error_msg.lower() or "not found" in error_msg.lower():
                    logger.debug("第 %d 次重试: USDC ATA 账户不存在，余额为 0", attempt)
                    if attempt < retry_max_attempts:
                        time.sleep(retry_sleep_s)
                    continue
                logger.warning("第 %d 次重试: 查询 USDC 余额返回错误: %s", attempt, error_msg)
                if attempt < retry_max_attempts:
                    time.sleep(retry_sleep_s)
                continue
            
            result = data.get("result", {})
            value = result.get("value", {})
            
            if not value:
                logger.debug("第 %d 次重试: USDC 余额返回为空，等待重试...", attempt)
                if attempt < retry_max_attempts:
                    time.sleep(retry_sleep_s)
                continue
            
            amount_str = value.get("amount", "0")
            total = int(amount_str)
            
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


def get_sol_balance(
    client: Client,
    owner_pubkey: str,
    logger: logging.Logger,
    rpc_url: Optional[str] = None,
    retry_max_attempts: int = 10,
    retry_sleep_s: int = 2,
) -> int:
    """查询指定钱包当前的 SOL 余额(最小单位 lamports).
    
    直接使用原始 JSON-RPC 调用 getBalance API，避免 solana-py 库的兼容性问题。
    """

    # 如果未显式传入 rpc_url, 则尝试从 client 中获取
    if rpc_url is None:
        try:
            rpc_url = getattr(getattr(client, "_provider", None), "endpoint_uri", None)
        except Exception:  # noqa: BLE001
            rpc_url = None

    if not rpc_url:
        raise RuntimeError("获取 SOL 余额失败: 无法确定 RPC URL")

    # 使用原始 JSON-RPC 调用 getBalance
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [owner_pubkey, {"commitment": "confirmed"}],
    }

    http_resp = http_request_with_retry(
        cfg=None,
        logger=logger,
        label="使用 getBalance 获取 SOL 余额",
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
    
    # 检查是否有错误
    if "error" in data:
        error_msg = data.get("error", {}).get("message", "未知错误")
        logger.warning("查询 SOL 余额返回错误: %s", error_msg)
        return 0
    
    sol_lamports = int(data.get("result", {}).get("value", 0))
    return sol_lamports


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

    sol_lamports = get_sol_balance(
        client,
        owner_pubkey,
        logger,
        rpc_url,
        retry_max_attempts,
        retry_sleep_s,
    )

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
    """调用 Jupiter Swap API 的 /quote 接口获取询价. 
    
    参数:
        input_mint: 输入代币 Mint 地址
        output_mint: 输出代币 Mint 地址
        amount: 输入金额 (最小单位, 如 lamports)
        
    返回:
        quoteResponse 对象, 用于后续 /swap 请求
    """
    
    # 使用更大的滑点进行询价, 以提高交易成功率
    # 至少使用 300 bps (3%) 或用户配置值的 2 倍
    effective_slippage = max(cfg.slippage_bps * 2, 300)
    
    quote_url = (
        f"{JUPITER_SWAP_API_BASE}/quote"
        f"?inputMint={input_mint}"
        f"&outputMint={output_mint}"
        f"&amount={amount}"
        f"&slippageBps={effective_slippage}"
    )
    
    headers = {
        "x-api-key": SWAP_API_KEY,
    }
    
    logger.debug("请求 Swap 询价: %s", quote_url)
    
    resp = http_request_with_retry(
        cfg=cfg,
        logger=logger,
        label=f"获取 {input_mint[:8]}...->{output_mint[:8]}... 询价",
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
        logger.error("解析 询价 响应 JSON 失败: %s, 原始响应: %s", e, resp.text)
        raise RuntimeError(f"解析 询价 响应 JSON 失败: {e}") from e
    
    # 检查是否有错误
    if quote_response.get("error") or quote_response.get("errorCode"):
        error_msg = quote_response.get("error") or quote_response.get("errorMessage") or "未知错误"
        raise RuntimeError(f"询价 请求返回错误: {error_msg}")
    
    # 检查 outAmount 是否有效
    out_amount = int(quote_response.get("outAmount", 0))
    if out_amount <= 0:
        raise RuntimeError(f"询价 返回的 outAmount 无效: {quote_response.get('outAmount')}")
    
    logger.info(
        "询价 成功: 输入=%d, 预计输出=%d, 有效滑点=%d bps (配置=%d bps)",
        amount,
        out_amount,
        effective_slippage,
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
        quote_response: /quote 返回的询价对象
        
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
        # 注意: dynamicSlippage 已被 Jupiter 标记为不再维护, 所以不使用
        # 滑点由 quoteResponse 中的 slippageBps 控制
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


def _extract_solana_error_details(e: Exception, logger: logging.Logger) -> str:
    """从 Solana 交易异常中提取详细错误信息. 
    
    尝试从异常对象中提取:
    - err: 错误代码/类型
    - message: 错误消息
    - logs: 程序日志
    - data: 错误数据
    """
    error_parts = []
    
    # 1. 基础错误字符串
    error_str = str(e)
    
    # 2. 尝试从异常属性中提取详细信息
    try:
        # 检查是否有 err 属性 (常见于 RPC 错误)
        if hasattr(e, "err"):
            err_val = getattr(e, "err", None)
            if err_val:
                error_parts.append(f"err={err_val}")
        
        # 检查是否有 message 属性
        if hasattr(e, "message"):
            msg_val = getattr(e, "message", None)
            if msg_val:
                error_parts.append(f"message={msg_val}")
        
        # 检查是否有 data 属性 (可能包含 logs)
        if hasattr(e, "data"):
            data_val = getattr(e, "data", None)
            if data_val:
                # 如果 data 是字典，提取关键信息
                if isinstance(data_val, dict):
                    if "err" in data_val:
                        error_parts.append(f"data.err={data_val['err']}")
                    if "logs" in data_val:
                        logs = data_val["logs"]
                        # 只取最后几行日志
                        if isinstance(logs, list) and logs:
                            last_logs = logs[-5:] if len(logs) > 5 else logs
                            error_parts.append(f"logs={last_logs}")
                else:
                    error_parts.append(f"data={data_val}")
        
        # 检查是否有 logs 属性
        if hasattr(e, "logs"):
            logs_val = getattr(e, "logs", None)
            if logs_val and isinstance(logs_val, list):
                last_logs = logs_val[-5:] if len(logs_val) > 5 else logs_val
                error_parts.append(f"logs={last_logs}")
        
        # 检查 __dict__ 中是否有额外信息
        if hasattr(e, "__dict__"):
            for key, val in e.__dict__.items():
                if key not in ("err", "message", "data", "logs", "args") and val:
                    error_parts.append(f"{key}={val}")
        
    except Exception as extract_err:  # noqa: BLE001
        logger.debug("提取 Solana 错误详情时发生异常: %s", extract_err)
    
    # 3. 组合错误信息
    if error_parts:
        detail_str = ", ".join(error_parts)
        return f"{error_str} [详情: {detail_str}]"
    else:
        return error_str


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
    
    # 3. 通过原始 JSON-RPC 发送交易（避免 solana-py 的超时问题）
    # 使用 skipPreflight=True 跳过预检查，因为预检查时的价格可能已变化导致滑点错误
    try:
        signed_tx_bytes = bytes(signed_tx)
        signed_tx_base64 = base64.b64encode(signed_tx_bytes).decode("utf-8")
        
        # 获取 RPC URL
        rpc_url = cfg.rpc_url
        if not rpc_url:
            try:
                rpc_url = getattr(getattr(client, "_provider", None), "endpoint_uri", None)
            except Exception:  # noqa: BLE001
                pass
        
        if not rpc_url:
            raise RuntimeError("无法确定 RPC URL")
        
        # 使用原始 JSON-RPC 调用 sendTransaction
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                signed_tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": True,
                    "preflightCommitment": "confirmed",
                    "maxRetries": 3,
                },
            ],
        }
        
        # 使用较长的超时时间（60秒）避免 ReadTimeout
        http_resp = http_request_with_retry(
            cfg=cfg,
            logger=logger,
            label="发送 Swap 交易",
            method="POST",
            url=rpc_url,
            headers={"Content-Type": "application/json"},
            json_body=payload,
            timeout=60,
            treat_insufficient_as_fatal=False,
            max_attempts=3,
            sleep_s=2,
        )
        
        data = http_resp.json()
        
        # 检查是否有错误
        if "error" in data:
            error_info = data.get("error", {})
            error_msg = error_info.get("message", "未知错误")
            error_code = error_info.get("code", "")
            raise RuntimeError(f"RPC 返回错误: {error_msg} (code={error_code})")
        
        tx_signature = data.get("result", "")
        
        if not tx_signature:
            raise RuntimeError(f"发送交易后未能获取签名: {data}")
        
        logger.info("交易已发送: %s", tx_signature)
        logger.info("Solscan: https://solscan.io/tx/%s", tx_signature)
        
        return tx_signature
    except Exception as e:
        # 提取详细错误信息
        error_details = _extract_solana_error_details(e, logger)
        logger.error("发送 Swap 交易失败: %s", error_details)
        raise RuntimeError(f"发送 Swap 交易失败: {error_details}") from e


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
    """执行第 1 步 SOL->USDC 交易. 
    
    使用 Jupiter Swap API (GET /quote + POST /swap + RPC send)
    """

    error_reason: Optional[str] = None

    # -------- 第 1 步: SOL -> USDC --------
    logger.info(
        "第 1 步: 随机选取 %.9f SOL 换为 USDC...",
        trade_amount_sol_display,
    )

    try:
        # 1. 获取询价
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
            error_reason = f"SOL->USDC 询价 返回的 outAmount 无效: {quote_response.get('outAmount')}"
            logger.error("%s, 完整响应: %s", error_reason, quote_response)
            return False, stats, error_reason, None

        # 记录本次一对交易中的 SOL/USDC 数量
        stats["sol_spent_lamports"] = amount_sol_lamports
        stats["usdc_received_units"] = out_usdc_amount
        stats["usdc_spent_units"] = out_usdc_amount

        logger.info(
            "SOL->USDC 询价成功: 输入=%d lamports (%.9f SOL), 预计输出=%d USDC units (%.6f USDC)",
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
        error_reason = f"第 1 步 SOL->USDC 交易失败: {e}"
        logger.error(error_reason)
        return False, stats, error_reason, None

    # -------- 等待第 1 步交易链上确认 --------
    # 这是关键步骤: 必须等第 1 步交易确认后才能执行第 2 步
    confirm_timeout = max(cfg.tx_interval_s, 30)
    logger.info(
        "第 1 步 SOL->USDC 交易已提交, 签名: %s, 开始等待最多 %d 秒以确认链上状态...",
        sig1,
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

    logger.info("第 1 步 SOL->USDC 交易已在链上确认成功!")

    # 在执行第 2 步前, 短暂等待让 RPC 节点同步数据
    logger.debug("等待 1 秒让 RPC 节点同步数据...")
    time.sleep(1)

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

        # 获取 SOL 余额用于日志(只查询 SOL, USDC 直接复用上一步查询结果)
        try:
            sol_after_leg1 = get_sol_balance(
                client,
                wallet_pubkey,
                logger,
                cfg.rpc_url,
                cfg.retry_max_attempts,
                cfg.retry_sleep_s,
            )
            tx_logger.info(
                "余额快照 阶段=第 1 步后 钱包_SOL_lamports=%d 钱包_SOL=%.9f 钱包_USDC_units=%d 钱包_USDC=%.6f",
                sol_after_leg1,
                sol_after_leg1 / 1_000_000_000,
                current_usdc_balance,
                current_usdc_balance / 1_000_000,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning("获取第 1 步后 SOL 余额失败: %s", e)


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
    """执行第 2 步 USDC->SOL 交易. 
    
    使用 Jupiter Swap API (GET /quote + POST /swap + RPC send)
    """

    error_reason: Optional[str] = None

    # -------- 第 2 步: USDC -> SOL --------
    logger.info(
        "第 2 步: 准备将 %d USDC units (%.6f USDC) 换回 SOL...",
        effective_usdc_amount,
        effective_usdc_amount / 1_000_000,
    )

    try:
        # 1. 获取询价
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
            error_reason = f"USDC->SOL 询价 返回的 outAmount 无效: {quote_response.get('outAmount')}"
            logger.error("%s, 完整响应: %s", error_reason, quote_response)
            return False, stats, error_reason

        logger.info(
            "USDC->SOL 询价成功: 输入=%d USDC units (%.6f USDC), 预计输出=%d lamports (%.9f SOL)",
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

        # 第 2 步成功后, 记录最终买回的 SOL 数量
        stats["sol_bought_lamports"] = out_sol_lamports

        logger.info("第 2 步完成, 交易签名: %s", sig2)
        
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

        # 等待第 2 步交易确认 (必须等待确认成功才算交易完成)
        confirm_timeout = max(cfg.tx_interval_s, 20)
        logger.info(
            "第 2 步 USDC->SOL 交易已提交, 开始等待最多 %d 秒以确认链上状态...",
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
            # 第 2 步确认失败, 需要重试
            error_reason = f"USDC->SOL 交易 {sig2} 链上确认失败或超时"
            logger.error(
                "USDC->SOL 交易 %s 在 %d 秒内未能确认成功, 将触发重试. ",
                sig2,
                confirm_timeout,
            )
            return False, stats, error_reason

    except Exception as e:  # noqa: BLE001
        error_reason = f"第 2 步 USDC->SOL 交易失败: {e}"
        logger.error(error_reason)
        return False, stats, error_reason

    return True, stats, None


def _execute_trade_pair_once(
    cfg: AppConfig,
    client: Client,
    keypair: Keypair,
    logger: logging.Logger,
    tx_logger: logging.Logger,
    rpc_manager: RpcUrlManager,
) -> tuple[bool, dict, Optional[str]]:
    """执行"一对交易": SOL -> USDC -> SOL. 

    步骤:
    1. 使用配置中的 trade_amount (SOL 数量) 将 SOL 换成 USDC;
    2. 等待第 1 步交易在链上确认;
    3. 使用步骤 1 得到的 USDC 金额再换回 SOL. 

    任一步骤失败, 都会立即终止本次"一对交易", 并返回 (False, stats). 
    全部成功时返回 (True, stats). 
    """

    base_empty_stats = {
        "sol_spent_lamports": 0,
        "usdc_received_units": 0,
        "usdc_spent_units": 0,
        "sol_bought_lamports": 0,
        # 第 1 步 SOL->USDC 交易统计
        "step1_exec_count": 0,  # 执行次数(含重试)
        "step1_success_count": 0,  # 成功次数
        "step1_fail_count": 0,  # 失败次数
        # 第 2 步 USDC->SOL 交易统计
        "step2_exec_count": 0,  # 执行次数(含重试)
        "step2_success_count": 0,  # 成功次数
        "step2_fail_count": 0,  # 失败次数
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

    # 使用 RPC URL 管理器获取当前 URL
    rpc_url = rpc_manager.current_url
    logger.info(f'当前 RPC URL: {rpc_url}')

    # 使用全局常量
    sol_mint = SOL_MINT
    usdc_mint = USDC_MINT

    # 钱包公钥
    wallet_pubkey = str(keypair.pubkey())

    # 为本次一对交易随机生成 SOL 卖出数量(单位: SOL), 只保留 3 位小数
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

    # -------- 执行第 1 步: SOL -> USDC (带重试) --------
    first_ok = False
    effective_usdc_amount: Optional[int] = None
    for attempt in range(1, cfg.retry_max_attempts + 1):
        stats["step1_exec_count"] += 1  # 记录执行次数
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

        if first_ok:
            stats["step1_success_count"] += 1  # 记录成功次数
            break  # 第 1 步成功，跳出重试循环

        # 第 1 步失败，记录当前 RPC URL 的失败次数
        stats["step1_fail_count"] += 1  # 记录失败次数
        rpc_manager.record_failure()
        
        # 判断是否需要重试
        if attempt < cfg.retry_max_attempts:
            # 切换到下一个 RPC URL
            rpc_manager.switch_to_next()
            logger.warning(
                "第 1 步 SOL->USDC 交易失败(第 %d/%d 次尝试), %d 秒后使用新 RPC 重试...",
                attempt,
                cfg.retry_max_attempts,
                cfg.retry_sleep_s,
            )
            time.sleep(cfg.retry_sleep_s)
        else:
            logger.error(
                "第 1 步 SOL->USDC 交易失败(已尝试 %d 次), 放弃本次一对交易",
                cfg.retry_max_attempts,
            )

    if not first_ok:
        return False, stats, error_reason

    # 以实际可用的 USDC 数量作为本次 USDC->SOL 的输入
    # 此时 effective_usdc_amount 一定不为 None（因为 first_ok 为 True）
    assert effective_usdc_amount is not None, "第 1 步成功但 effective_usdc_amount 为 None"
    stats["usdc_spent_units"] = effective_usdc_amount

    # -------- 执行第 2 步: USDC -> SOL (带重试) --------
    second_ok = False
    for attempt in range(1, cfg.retry_max_attempts + 1):
        stats["step2_exec_count"] += 1  # 记录执行次数
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

        if second_ok:
            stats["step2_success_count"] += 1  # 记录成功次数
            break  # 第 2 步成功，跳出重试循环

        # 第 2 步失败，记录当前 RPC URL 的失败次数
        stats["step2_fail_count"] += 1  # 记录失败次数
        rpc_manager.record_failure()
        
        # 判断是否需要重试
        if attempt < cfg.retry_max_attempts:
            # 切换到下一个 RPC URL
            rpc_manager.switch_to_next()
            logger.warning(
                "第 2 步 USDC->SOL 交易失败(第 %d/%d 次尝试), %d 秒后使用新 RPC 重试...",
                attempt,
                cfg.retry_max_attempts,
                cfg.retry_sleep_s,
            )
            time.sleep(cfg.retry_sleep_s)
        else:
            logger.error(
                "第 2 步 USDC->SOL 交易失败(已尝试 %d 次), 放弃本次一对交易",
                cfg.retry_max_attempts,
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
    rpc_manager: RpcUrlManager,
) -> tuple[bool, dict, Optional[str]]:
    """执行"一对交易" (SOL->USDC->SOL). 

    返回 (success, stats, error_reason):
    - success: 是否成功完成一对交易
    - stats: 参见 _execute_trade_pair_once 的返回说明
    - error_reason: 如果失败, 为本次失败的一句简要原因描述; 成功时为 None
    """

    try:
        return _execute_trade_pair_once(cfg, client, keypair, logger, tx_logger, rpc_manager)

    except Exception as e:  # noqa: BLE001
        logger.error("执行一对交易时发生未捕获异常: %s", e)
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
    logger.info("配置的 RPC URL 列表(%d 个): %s", len(cfg.rpc_urls), cfg.rpc_urls)

    # 创建 RPC URL 管理器
    rpc_manager = RpcUrlManager(cfg.rpc_urls, logger)

    # 记录开始时间，用于计算总执行时长
    start_time = time.time()

    success_pairs = 0
    unit_label = "一对交易"

    # 记录本轮中最后一次失败的交易的原因(如有)
    last_error_reason: Optional[str] = None

    # 本轮运行的累计统计数据
    total_sol_spent_lamports = 0
    total_usdc_received_units = 0
    total_usdc_spent_units = 0
    total_sol_bought_lamports = 0
    # 本轮交易步骤统计
    total_step1_exec_count = 0  # SOL->USDC 执行次数
    total_step1_success_count = 0  # SOL->USDC 成功次数
    total_step1_fail_count = 0  # SOL->USDC 失败次数
    total_step2_exec_count = 0  # USDC->SOL 执行次数
    total_step2_success_count = 0  # USDC->SOL 成功次数
    total_step2_fail_count = 0  # USDC->SOL 失败次数

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
                    "step1_exec_count": 0,
                    "step1_success_count": 0,
                    "step1_fail_count": 0,
                    "step2_exec_count": 0,
                    "step2_success_count": 0,
                    "step2_fail_count": 0,
                }
                pair_error = None
            else:
                logger.info("========== 第 %d 对交易 ==========", success_pairs + 1)
                logger.info("开始执行第 %d 对交易 (SOL->USDC->SOL)...", success_pairs + 1)

                # 每轮交易开始时记录钱包当前 SOL 余额（不查询 USDC，减少 RPC 调用）
                try:
                    sol_before_pair = get_sol_balance(
                        client,
                        wallet_pubkey,
                        logger,
                        cfg.rpc_url,
                        cfg.retry_max_attempts,
                        cfg.retry_sleep_s,
                    )

                    tx_logger.info(
                        "余额快照 阶段=一对开始 钱包_SOL_lamports=%d 钱包_SOL=%.9f",
                        sol_before_pair,
                        sol_before_pair / 1_000_000_000,
                    )

                    # 只有在成功获取余额的情况下才继续执行一轮交易
                    pair_ok, pair_stats, pair_error = execute_trade_pair(
                        client,
                        keypair,
                        cfg,
                        logger,
                        tx_logger,
                        rpc_manager,
                    )

                except Exception as e:  # noqa: BLE001
                    logger.error("获取一对交易开始前钱包余额失败: %s", e)
                    pair_ok = False
                    pair_stats = {
                        "sol_spent_lamports": 0,
                        "usdc_received_units": 0,
                        "usdc_spent_units": 0,
                        "sol_bought_lamports": 0,
                        "step1_exec_count": 0,
                        "step1_success_count": 0,
                        "step1_fail_count": 0,
                        "step2_exec_count": 0,
                        "step2_success_count": 0,
                        "step2_fail_count": 0,
                    }
                    pair_error = f"获取一对交易开始前钱包余额失败: {e}"

            # 无论一对交易成功与否，都累加交易步骤统计
            total_step1_exec_count += int(pair_stats.get("step1_exec_count", 0))
            total_step1_success_count += int(pair_stats.get("step1_success_count", 0))
            total_step1_fail_count += int(pair_stats.get("step1_fail_count", 0))
            total_step2_exec_count += int(pair_stats.get("step2_exec_count", 0))
            total_step2_success_count += int(pair_stats.get("step2_success_count", 0))
            total_step2_fail_count += int(pair_stats.get("step2_fail_count", 0))

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
            logger.error("主循环中出现未捕获异常: %s", e)
            break

        if pair_ok:
            success_pairs += 1
            logger.info(
                "第 %d 次 %s 执行成功 (当前进度: %d/%d). ",
                success_pairs,
                unit_label,
                success_pairs,
                cfg.max_runs,
            )
        else:
            logger.error(
                "本次 %s 执行失败, 不计入成功次数 (当前进度: %d/%d). 本轮将继续尝试下一次 %s. ",
                unit_label,
                success_pairs,
                cfg.max_runs,
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
        logger.debug(
            "本次一对交易结束 (当前进度: %d/%d), 休眠 %d 秒后继续下一笔交易. ",
            success_pairs,
            cfg.max_runs,
            cfg.tx_interval_s,
        )
        time.sleep(cfg.tx_interval_s)

    # 本轮运行结束后的统计输出
    if cfg.enable_trading:
        # 计算 SOL 总变化量 (买回 - 花费)
        sol_net_change_lamports = total_sol_bought_lamports - total_sol_spent_lamports
        sol_net_change_display = sol_net_change_lamports / 1_000_000_000
        # 格式化符号: 正数加 + 号, 负数自带 - 号
        sol_change_sign = "+" if sol_net_change_lamports >= 0 else ""

        logger.info(
            "本轮统计: 成功 %s 次数=%d, "
            "SOL->USDC(执行=%d/成功=%d/失败=%d), USDC->SOL(执行=%d/成功=%d/失败=%d), "
            "初始花费 SOL 总数=%.9f SOL (lamports=%d), "
            "获得 USDC 总数≈%.6f USDC (最小单位=%d), 花费 USDC 总数≈%.6f USDC (最小单位=%d), "
            "最终买回 SOL 总数=%.9f SOL (lamports=%d), SOL 总变化=%s%.9f SOL (lamports=%s%d). ",
            unit_label,
            success_pairs,
            total_step1_exec_count,
            total_step1_success_count,
            total_step1_fail_count,
            total_step2_exec_count,
            total_step2_success_count,
            total_step2_fail_count,
            total_sol_spent_lamports / 1_000_000_000 if total_sol_spent_lamports else 0.0,
            total_sol_spent_lamports,
            total_usdc_received_units / 1_000_000 if total_usdc_received_units else 0.0,
            total_usdc_received_units,
            total_usdc_spent_units / 1_000_000 if total_usdc_spent_units else 0.0,
            total_usdc_spent_units,
            total_sol_bought_lamports / 1_000_000_000 if total_sol_bought_lamports else 0.0,
            total_sol_bought_lamports,
            sol_change_sign,
            sol_net_change_display,
            sol_change_sign,
            sol_net_change_lamports,
        )
        tx_logger.info(
            "本轮统计: 成功 %s 次数=%d, "
            "SOL->USDC(执行=%d/成功=%d/失败=%d), USDC->SOL(执行=%d/成功=%d/失败=%d), "
            "初始花费 SOL 总数=%.9f SOL (lamports=%d), "
            "获得 USDC 总数≈%.6f USDC (最小单位=%d), 花费 USDC 总数≈%.6f USDC (最小单位=%d), "
            "最终买回 SOL 总数=%.9f SOL (lamports=%d), SOL 总变化=%s%.9f SOL (lamports=%s%d). ",
            unit_label,
            success_pairs,
            total_step1_exec_count,
            total_step1_success_count,
            total_step1_fail_count,
            total_step2_exec_count,
            total_step2_success_count,
            total_step2_fail_count,
            total_sol_spent_lamports / 1_000_000_000 if total_sol_spent_lamports else 0.0,
            total_sol_spent_lamports,
            total_usdc_received_units / 1_000_000 if total_usdc_received_units else 0.0,
            total_usdc_received_units,
            total_usdc_spent_units / 1_000_000 if total_usdc_spent_units else 0.0,
            total_usdc_spent_units,
            total_sol_bought_lamports / 1_000_000_000 if total_sol_bought_lamports else 0.0,
            total_sol_bought_lamports,
            sol_change_sign,
            sol_net_change_display,
            sol_change_sign,
            sol_net_change_lamports,
        )

        if last_error_reason:
            logger.error("本轮存在交易失败, 失败原因: %s", last_error_reason)
            tx_logger.info("本轮存在交易失败, 失败原因: %s", last_error_reason)
        
        # 输出各 RPC URL 的失败次数统计
        rpc_fail_stats = rpc_manager.get_fail_stats()
        if rpc_fail_stats:
            fail_details = ", ".join([f"{url}: {count}次" for url, count in rpc_fail_stats.items()])
            logger.info("RPC URL 失败统计: %s", fail_details)
            tx_logger.info("RPC URL 失败统计: %s", fail_details)
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

    # 计算并输出总执行时长
    total_elapsed_time = time.time() - start_time
    hours, remainder = divmod(int(total_elapsed_time), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        duration_str = f"{hours}小时{minutes}分{seconds}秒"
    elif minutes > 0:
        duration_str = f"{minutes}分{seconds}秒"
    else:
        duration_str = f"{seconds}秒"
    
    logger.info("总执行时长: %s (%.2f 秒)", duration_str, total_elapsed_time)
    tx_logger.info("总执行时长: %s (%.2f 秒)", duration_str, total_elapsed_time)

    # 整轮执行结束后，查询并打印钱包最终余额
    try:
        final_sol, final_usdc = get_wallet_balances(
            client,
            wallet_pubkey,
            USDC_MINT,
            logger,
            cfg.rpc_url,
            cfg.retry_max_attempts,
            cfg.retry_sleep_s,
        )
        logger.info(
            "整轮结束后钱包余额: SOL=%.9f (%d lamports), USDC=%.6f (%d 最小单位)",
            final_sol / 1_000_000_000,
            final_sol,
            final_usdc / 1_000_000,
            final_usdc,
        )
        tx_logger.info(
            "整轮结束后钱包余额: SOL=%.9f (%d lamports), USDC=%.6f (%d 最小单位)",
            final_sol / 1_000_000_000,
            final_sol,
            final_usdc / 1_000_000,
            final_usdc,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("查询整轮结束后钱包余额失败: %s", e)


def run_auto_trader() -> None:
    """主入口: 加载配置、初始化客户端并循环执行自动交易. """

    logger, tx_logger = setup_logging()
    logger.info("===================================================")
    logger.info("===== 启动 Solana 自动交易程序 (V2 - Swap API) =====")
    logger.info("===================================================")

    # 1. 加载配置
    try:
        cfg = load_config(CONFIG_PATH)
    except Exception as e:  # noqa: BLE001
        # 配置相关错误通常是最常见的问题, 需要给用户极其清晰的提示
        logger.error("加载配置失败: %s", e)
        logger.debug("加载配置异常堆栈: \n%s", traceback.format_exc())
        return

    logger.info(
        "当前配置: network=%s, rpc_url=%s, trade_amount_range=[%.9f, %.9f], tx_interval_s=%d, enable_trading=%s, max_runs=%d",
        cfg.network,
        cfg.rpc_url,
        cfg.trade_amount_min,
        cfg.trade_amount_max,
        cfg.tx_interval_s,
        cfg.enable_trading,
        cfg.max_runs,
    )
    logger.info(
        "Swap API 配置: slippage_bps=%d (%.2f%%), priority_fee=%d lamports (%.6f SOL)",
        cfg.slippage_bps,
        cfg.slippage_bps / 100,
        cfg.priority_fee,
        cfg.priority_fee / 1_000_000_000,
    )

    # 2. 初始化私钥
    try:
        keypair = load_keypair_from_base58(cfg.private_key)
    except Exception as e:  # noqa: BLE001
        logger.error("加载私钥失败: %s", e)
        logger.debug("加载私钥异常堆栈: \n%s", traceback.format_exc())
        return

    wallet_pubkey = str(keypair.pubkey())
    logger.info("私钥加载成功, 钱包地址: %s", wallet_pubkey)


    # 3. 初始化 RPC 客户端
    try:
        client = build_client(cfg)
        # 测试连通性
        version = client.get_version()
        logger.info("RPC 连接成功, 节点版本信息: %s", version)
    except Exception as e:  # noqa: BLE001
        logger.error("初始化 RPC 客户端或测试连接失败: %s", e)
        logger.debug("RPC 初始化异常堆栈: \n%s", traceback.format_exc())
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
