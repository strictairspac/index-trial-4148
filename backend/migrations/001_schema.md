# Schema v1

SQLAlchemy metadata creates users, audits, workloads, candidates, experiments, telemetry and precision_state on first startup. Workload/candidate natural names are UNIQUE; experiment snapshots are immutable JSON. Foreign keys prevent orphan references. Index trials affect only the dedicated telemetry table, under MySQL GET_LOCK, and drop the trial index after each execution. No production database connection is accepted.

# Schema v2 — value precision upgrade (gb-4102)

`telemetry.value` was `FLOAT`; six-decimal device readings drifted in the last digit
(`42.123456` came back as `42.1235`). The target column is `DECIMAL(48,6)`.

The upgrade adds its columns with its own DDL (`backend/src/services/precision.py`),
not via the ORM metadata, so a fresh install still starts in the legacy shape:

- `value_exact DECIMAL(48,6) NULL` — shadow column: new writes and backfill land here.
- `value_source VARCHAR(16) NULL` — `legacy` (backfilled), `exact` (new/supplement), `correction`.
- `value_version INT NOT NULL DEFAULT 0` — bumped on every write; supports reconciliation.
- `value_legacy_float FLOAT NULL` — only after the switch; the original FLOAT read path.

`precision_state` (one row, id=1) persists the phase, a monotonically increasing
`version` recorded into each experiment snapshot, the backfill cursor, counters,
the reconcile checksum and the last message.

Phases: `legacy → backfilling → backfilled → reconciled → switching → switched`, and
`switched → rolling_back → legacy` for a lossless rollback. Persisted phase is
self-healed from the real column shape after a crash. Backfill commits its cursor in
the same transaction as each batch, so an interrupted run resumes from the last batch.
Backfill UPDATE is guarded by `value_exact IS NULL AND value_source IS NULL`, so a
concurrent correction/supplement can never be overwritten by an old value. The final
switch is one atomic `ALTER TABLE` under both `precision_write_guard` and
`index_trial_serial`; a trial queued under a different `version` is failed rather than
run across the switch.
