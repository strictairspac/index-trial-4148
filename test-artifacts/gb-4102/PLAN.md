# gb-4102 精度升级实施计划

## 问题
`telemetry.value` 是 MySQL `FLOAT`，新设备上报六位小数，InnoDB 存 FLOAT 后末位漂移
（`42.123456` 读回 `42.1235`，`12345678.123456` 读回 `12345700`）。

## 目标形态
`value DECIMAL(48,6) NOT NULL`。48 位整数位可覆盖 FLOAT 全部有限范围（>3.4e38），
避免 DECIMAL(24,6) 对 1e18 以上读数溢出。六位小数原样存取。

升级期间双列：
- `value FLOAT NOT NULL`：旧读路径 / 旧试验样本仍可读。
- `value_exact DECIMAL(48,6) NULL`：影子列，新写入与回填都写它。
- `value_source`：`legacy` / `exact` / `correction`。
- `value_version`：每次写入自增，供并发/对账复核。

## 阶段
1. `legacy`：只有 FLOAT。
2. `backfilling`：加影子列；按 id 游标分批回填
   `value_exact = CAST(CAST(value AS CHAR) AS DECIMAL(48,6))`
   —— 即“旧库查到的值”（FLOAT 的显示值）补足六位。
   每行 `WHERE value_exact IS NULL AND value_source IS NULL`，行锁串行化，
   并发更正/补录先写 `value_exact` 则回填跳过，旧值不会盖新值。
   游标与计数和批内更新同事务提交，进程中断后从游标续跑。
3. `reconciling`：逐条对账。`source=legacy` 行要求
   `value_exact == 旧读回值`；`source=exact/correction` 行要求非空。
   记录 mismatch 数与 SHA-256。
4. `switched`：在 `GET_LOCK('index_trial_serial')` 内做最终 SQL 完整性检查，
   再 `ALTER TABLE ... MODIFY value FLOAT NULL`、
   `MODIFY value_exact DECIMAL(48,6) NOT NULL`，
   最后单条原子 DDL
   `RENAME COLUMN value TO value_legacy_float, RENAME COLUMN value_exact TO value`。
   单条 ALTER 是原子的；崩溃后按实际列结构自愈。
5. `legacy`（回切）：仅当所有行都能无损转回 FLOAT 时允许，
   否则 409 并给出丢失精度行样例，状态与读数都不变。

## 与索引试验的协调
- 试验排队时把 `precision.version` 写入不可变 snapshot。
- worker 取任务后在同一把 GET_LOCK 内比对当前版本；不一致直接失败并提示新建试验，
  绝不在切换前后混用两版样本。
- 切换需要同一把锁：正在跑的试验持锁期间切换返回 409（或等待超时），
  保证一次试验建索引前后的样本同版。

## 回切判定
`lossy` 当且仅当 `ABS(value) > FLOAT_MAX` 或
`CAST(CAST(CAST(value AS CHAR) AS FLOAT) AS DECIMAL(48,6)) <> value`。
回切前在锁内复算，>0 即拒绝。

## 验证
真实 MySQL 8.0.44 + Redis 7 本地实例；pytest 覆盖：
- 回填遇上更正/补录（旧值不盖新值）
- 回填中断续跑（子进程 SIGKILL 后从持久化游标继续）
- 切换遇上运行中试验（锁冲突返回 409；排队任务版本失配失败）
- 回切被拒后的回读（读数保持六位精度、阶段不变）
