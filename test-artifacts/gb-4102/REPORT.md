# gb-4102 精度升级 · 验证报告

日期 2026-11-21 · 真实 MySQL 8.0.44 + Redis 7.0.15（本机实例，非 Docker）

## 环境说明
沙箱无 Docker，故从官方 tarball 解出 MySQL 8.0.44 (aarch64) 与 Debian 的 Redis 7.0.15
在本机启动，Python 依赖装在 `/home/node/.local`。被测代码是仓库里的真实服务层与
worker 逻辑，未打桩（除索引耗时函数 `_timed` 用于制造“试验持锁中”的窗口）。

## 关键 MySQL 行为（先验证再编码）
- `FLOAT` 读回：`42.123456 → 42.1235`、`12345678.123456 → 12345700`、`31.7 → 31.7`。
- 旧库“查到的值”= `CAST(value AS CHAR)`，回填按此六位补零。
- 回切无损判定必须经显示值：`DECIMAL→FLOAT→CHAR→DECIMAL`；
  直接 `DECIMAL→FLOAT→DECIMAL` 会把 `31.7` 误判为 `31.700001`。
- `ALTER TABLE ... CHANGE COLUMN value value_legacy_float FLOAT NULL,
  CHANGE COLUMN value_exact value DECIMAL(48,6) NOT NULL` 单条原子完成列类型切换。
- 单语句里不能“RENAME 后复用旧名”，故切换与回切用两条 `CHANGE COLUMN`。

## 用例（backend/tests/test_precision_upgrade.py，20 项全过）
| 场景 | 用例 |
|---|---|
| 新读数原样入库回读 | `test_new_reading_stored_and_read_back_verbatim_after_switch` |
| 历史读数按旧读回值回填 | `test_historical_readings_backfilled_from_old_readback` |
| 回填遇上更正（不覆盖） | `test_backfill_does_not_overwrite_concurrent_correction` |
| 回填遇上补录（不覆盖） | `test_backfill_does_not_overwrite_concurrent_supplement` |
| 回填后、切换前更正保留 | `test_correction_after_backfill_and_before_switch_survives` |
| 回填中断续跑 | `test_backfill_resumes_from_persisted_cursor` |
| 批次未提交即崩（游标回退） | `test_backfill_crash_before_commit_rolls_back_to_last_cursor` |
| 真实 SIGKILL 后新进程续跑 | `test_backfill_survives_sigkill_and_resumes` |
| 真实多线程更正与回填竞争 | `test_concurrent_corrections_during_backfill_never_lose_to_old_value` |
| 切换遇上运行中试验（阻塞） | `test_real_trial_holds_lock_and_blocks_switch_then_switch_after` |
| 切换遇上试验锁（注入） | `test_switch_refused_while_trial_holds_lock` |
| 切换遇上回填锁 | `test_switch_blocked_while_backfill_lock_held` |
| 排队任务跨版本被拒 | `test_queued_trial_fails_when_precision_changed_under_it` |
| 试验前后同一版本 | `test_trial_before_and_after_sample_use_one_version` |
| 回切被拒（有损） | `test_rollback_refused_when_precision_would_be_lost` |
| 回切被拒后回读不变 | 同上，断言 `phase` 仍 `switched`、读数逐字节一致 |
| 零损失时才允许回切 | `test_rollback_succeeds_when_every_row_is_lossless` |
| 六位边界/48 位整数位 | `test_six_decimal_canonicalisation_boundaries`、`test_full_48_digit_value_round_trips` |
| 超六位/越界 → 422 | `test_ingest_rejects_bad_precision_with_422_not_500` |
| 旧精度下会有损的更正被拒 | `test_legacy_correction_that_would_drift_is_refused` |

## HTTP 层脚本（真实 uvicorn + JWT）
1. legacy 下六位入库返回 409；viewer 调 `/precision/start` 返回 403。
2. 启动 → 后台线程并发更正 → 回填，10 条更正全部保留。
3. 对账 mismatch=0 → 切换；新读数 `12345678.123456` 原样回读，历史 `31.7 → 31.700000`。
4. 回切返回 409（`lossy_rows=11`），阶段与读数不变。
5. 排队于切换前的试验在切换后执行被拒（版本 v2→v3）；切换后新试验版本 v3、结果等价。

## 实施中修掉的真实缺陷
- PyMySQL 在新精度下返回 `Decimal`，试验证据 JSON 直接序列化报 `TypeError`；
  现统一归一为字符串后计算 SHA-256。
- `Decimal.quantize/不相等比较/abs()` 受默认 28 位上下文影响，48 位读数会被误判越界或误算精度损失；
  改用 `localcontext(prec=60)`、`compare()`、`copy_abs()`。
- `parse_six` 抛 `ValueError` 未转成 HTTP 错误，曾会变成 500；现入口统一转 422。
- 切换/回切未持回填锁，可能撞上执行中的对账或回填；现统一锁序
  `precision_write_guard → precision_backfill → index_trial_serial`。

## 复跑
```sh
bash backend/scripts/run_precision_tests.sh   # 需活的 MySQL/Redis，见脚本注释
```
