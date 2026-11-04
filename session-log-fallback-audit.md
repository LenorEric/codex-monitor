# Session 日志 fallback 审查

审查时间：2026-11-02（北京时间）。目标窗口为 2026-08-02 20:42 至 2026-11-02 20:42，按原始事件时间筛选。

## 数据范围与限制

- 只读扫描本机 `CODEX_HOME` 默认位置下的 `sessions`、`archived_sessions`，共 164 个 JSONL 文件；本地数据库登记的采集根目录与该目录一致。
- 实际保留的记录从 2026-10-19 开始；没有 2026-08-02 至 2026-10-18 的日志，不能据此证明那段时间或其他机器的日志使用相同格式。
- 日志版本为 `0.162.0-alpha.17.2`（1 个文件）、`0.163.0-alpha.10.2`（156 个）、`0.164.0-alpha.11.1`（7 个）。
- 原始记录计数尚未跨文件去重。审查期间有活动会话继续写入；以下计数对应上述审查时刻。

## 证据与修改

| 审查项 | 原始数据证据 | 处理 |
| --- | --- | --- |
| 累计值 / 增量值互相替代 | 3,787 条用量记录同时具有 `total_token_usage`、`last_token_usage`，单侧缺失为 0 | 用量记录要求两者可解析；事件 ID 只使用累计分项，不再回退到增量 |
| 无用量的 `token_count` | 2 条 `info=null`，同时携带 `rate_limits` | 继续跳过，不视为损坏的用量记录 |
| token 字段别名 | 两组用量均有整数 `input_tokens`、`cached_input_tokens`、`cache_write_input_tokens`、`output_tokens`；其他别名未出现 | 删除 prompt/completion、cache_read/cache_creation 及嵌套 details 别名解析 |
| 会话 ID 别名与文件名回退 | 164 条 `session_meta` 全部具有非空 `id`；56 条 `session_id` 与 `id` 不同 | 只使用 `session_meta.payload.id`，不把父会话 ID 或文件名当成当前会话 ID |
| 模型的多位置查找 | 630 条 `turn_context` 全部具有顶层 `model`，用量记录均已有前置模型上下文；token 记录没有 model/model_name 字段 | 只从 `turn_context.payload.model` 更新模型，移除嵌套 info 和 token 记录中的模型覆盖 |
| token 记录中的服务档位覆盖 | token 记录 payload 仅有 type/info/rate_limits，info 仅有两组用量和 model_context_window | 移除 token 记录中的服务档位覆盖；保留 turn_context 的档位处理 |

字段别名清理同样适用于共享的 `parse_token_usage()`。该函数还解析 CLI `turn.completed.usage`，因此保留可选缓存计数缺失时为零的行为；输入和输出使用规范整数计数。没有依据仅凭 session 日志移除 CLI 可选字段处理或整个 fast 档位功能。

## 保留的记账规则

- 158 个文件具有用量记录，其中 20 个文件首次 `last_token_usage` 与累计值不同。首次事件必须使用真实增量，不能把累计值全额计入；不再按继承历史推测零值或累计值。
- 后续累计分项均未回退时，用当前累计值减上次累计值；重复累计快照不再重复计入 `last_token_usage`。
- 累计回退时使用该记录提供的增量；审查数据未出现回退，但这是计数状态变化的处理规则，不是另一种日志格式。
- 没有有效 metadata、用量缺少必要字段或含无效计数时，不猜测替代身份，交给既有无效来源/健康状态处理，保留已确认账本。

## 验证与持久数据

修改前后在同一批 164 个文件字节上分别重放解析器，共得到 3,671 个非零增量事件。会话身份、全部解析事件（含 token、模型、档位）及事件 ID 完全一致，未发现无效文件。

已有规范事实、备份、数据库和云端记录均未改写；事件 ID 格式保持不变。本次是收紧原始输入读取方式，不要求转换既有持久数据，不增加数据合同版本，也不请求历史重建。

相关单元与处理集成测试通过。完整 Python 测试集执行 712 项，3 项失败；将本次改动的两个运行模块替换为修改前代码后，这 3 项仍全部复现：两项 dashboard HTML 动画断言、一项更新失败后的 requirements 回滚断言。它们不由本次 session 解析清理引入。
