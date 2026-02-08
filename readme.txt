Solana 自动交易程序 使用说明（面向最终用户）

本程序的最终交付物只有三个文件：
- solana_auto.exe   （主程序，可执行文件）
- config.json       （配置文件，需要由你填写）
- readme.txt        （当前说明文档）

你只需要按照下面步骤操作，不需要安装 Python、不需要了解区块链或程序实现细节。


一、准备文件

1. 将以下三个文件放在同一个目录下：
   - solana_auto.exe
   - config.json
   - readme.txt

2. 如果没有 config.json，可以新建一个文本文件，命名为 config.json，并填入类似下面的内容：

   {
     "network": "testnet",
     "private_key": "你的私钥(Base58字符串)",
     "trade_amount_min": 0.02,
     "trade_amount_max": 0.07,
     "tx_interval_s": 60,
     "enable_trading": false,
     "max_runs": 1,
     "ultra_slippage_bps": 0.03,
     "ultra_fee_amount": 0.000001
   }


   填写完成后保存该文件。


二、配置说明（只保留必须理解的参数）

所有配置都在 config.json 中完成，字段说明如下：

1. network
   - 取值："testnet" 或 "mainnet"。
   - 建议：第一次使用时一定要设置为 "testnet"（测试网），确认行为正确且风险可控后再考虑改为 "mainnet"（主网）。

2. private_key
   - 说明：你的 Solana 钱包私钥，必须是 Base58 编码的私钥字符串（如 Phantom 导出的那种）。
   - 非常重要：
     - 不要将私钥泄露给任何人。
     - 不要把 config.json 上传到网络或公共仓库。
     - 建议只使用单独的小额钱包，用于测试和自动交易，不要使用主资金钱包。

3. rpc_url
   - 说明：Solana RPC 服务器地址。
   - 默认值：rpc_url = "https://solana-rpc.publicnode.com"
   - 备用: "https://api.mainnet-beta.solana.com"
      "https://solana-mainnet.core.chainstack.com/d644d2fa944eb114a6c79756734b3ff5"

4. trade_amount_min / trade_amount_max
   - 说明：每一笔 SOL->USDC 交易卖出的 SOL 数量范围。
   - 单位：SOL。
   - 例如：
     - trade_amount_min = 0.02
     - trade_amount_max = 0.07
   - 程序每次会在这个区间内随机选择一个数量作为本次卖出的 SOL 金额，然后用获得的 USDC 再买回 SOL。

5. tx_interval_s
   - 说明：每笔链上互换交易之间的时间间隔（秒）。
   - 具体行为：
     - 第一步 SOL->USDC 完成后，等待 tx_interval_s 秒，再执行第二步 USDC->SOL；
     - 本次 USDC->SOL 完成后，再等待 tx_interval_s 秒，才开始下一次 SOL->USDC。

6. enable_trading
   - 取值：true / false。
   - false：仅做“心跳检查”，不会真正发起链上交易（用于安全验证配置）。
   - true：会真实发起链上交易，建议只在充分测试并确认理解风险后再开启。

7. max_runs
   - 说明：希望成功完成的“一对交易”次数。
   - 必须是大于 0 的整数，例如：
     - max_runs = 3 表示成功完成 3 次 "SOL->USDC->SOL" 一对交易后自动停止。
   - 如果某一对交易中的任意一步失败，该次不计入成功次数，程序会立即停止，不再继续下一次。

8. ultra_slippage_bps
   - 说明：配置中的数值表示“百分比”，例如 ultra_slippage_bps = 0.03 表示 0.03% 的价格滑点。
   - 默认值：ultra_slippage_bps = 0.03，对应约 0.03% 的价格偏差。
   - 程序会自动按公式：slippage_tolerance = ultra_slippage_bps / 100，将换算后的值作为 HTTP 参数 `slippage_tolerance` 传给 Ultra API（例如 0.03% -> 0.0003）。
   - 该滑点会应用在 SOL->USDC 和 USDC->SOL 两笔 swap 上。

9. ultra_fee_amount
   - 说明：调用 Jupiter Ultra API 时使用的“固定平台费金额”参数，例如 0.000001。
   - 程序会自动按公式：priority_fee = ultra_fee_amount × 10^9，将其换算为 lamports 数量并作为 HTTP 参数 `priority_fee` 传给 Ultra API。
   - 示例：ultra_fee_amount = 0.000001 时，会换算成 priority_fee = 1000（lamports）。
   - 该固定 fee 会同时应用在 SOL->USDC 和 USDC->SOL 两笔 swap 上。



三、如何运行程序

1. 确认 config.json 已按上述说明填写好，并与 solana_auto.exe 在同一目录。

2. 双击运行：
   - 在资源管理器中直接双击 solana_auto.exe 即可启动程序。

3. 或者在命令行中运行：
   - 在该目录中按住 Shift 键点击右键，选择“在此处打开命令行/终端”。
   - 输入：
     solana_auto.exe
   - 回车执行。

4. 停止程序：
   - 在命令行窗口中按 Ctrl + C 即可停止程序。
   - 或者直接关闭程序窗口。


四、查看日志和结果

1. 程序运行时会自动在当前目录下创建 logs 文件夹，其中包括：
   - logs\app.log          ：一般运行日志（包含配置加载、错误信息等）。
   - logs\transactions.log ：交易明细与每轮统计信息。

2. 每一轮运行结束时（按照 max_runs 完成或中途失败终止），日志中会输出本轮统计，包括：
   - 成功的一对交易次数；
   - 花费掉的 SOL 总数；
   - 获得的 USDC 总数；
   - 花费的 USDC 总数；
   - 最终买回的 SOL 总数；
   - 如果本轮存在失败的一对交易，还会额外输出最近一次失败的原因描述（例如: 获取 SOL->USDC 报价失败: ...）。


五、使用建议

1. 强烈建议：
   - 先在测试网 (network = "testnet") + enable_trading = false 模式下跑一轮，确认配置文件填写无误；
   - 再在测试网 + enable_trading = true 下，用小额资金跑几轮，观察行为和日志是否符合预期；
   - 只有在完全理解风险、自行承担后果的前提下，才考虑切换到主网 (network = "mainnet")。

2. 任意时刻如果不确定自己在做什么，请立即停止程序（Ctrl + C 或关闭窗口），重新检查配置，或寻求专业帮助。
