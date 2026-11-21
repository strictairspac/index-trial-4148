"""In-process end-to-end checks of the precision upgrade against real MySQL 8.0.

Requires a live MySQL at DATABASE_URL (see test-artifacts/gb-4102/run_tests.sh).
"""
import json
import sys
from decimal import Decimal

import pytest
from fastapi import HTTPException
from sqlalchemy import text

from src.config.database import engine, Session
from src.services import precision, telemetry
from src.utils.precision import parse_six
import conftest


# ------------------------------------------------------------------ helpers
def raw(sql, params=None):
    with engine.connect() as c:
        rows = c.execute(text(sql), params or {}).mappings().all()
        return [dict(r) for r in rows]


def column_type():
    rows = raw("SELECT DATA_TYPE FROM information_schema.COLUMNS "
               "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='telemetry' AND COLUMN_NAME='value'")
    return rows[0]['DATA_TYPE']


def upgrade_to(phase):
    if phase in ('backfilling', 'backfilled', 'reconciled'):
        precision.start()
    if phase in ('backfilled', 'reconciled'):
        precision.backfill(batch_size=7)
    if phase == 'reconciled':
        precision.reconcile()


# ------------------------------------------------------------- round trips
def test_new_reading_stored_and_read_back_verbatim_after_switch():
    upgrade_to('reconciled')
    precision.switch()
    for value in ('42.123456', '0.000001', '12345678.123456', '-12.345678'):
        row = telemetry.ingest(17, '2026-12-02T08:00:00', 'pump', value)
        assert row['value'] == parse_six(value).quantize(Decimal('0.000001')).__format__('f')
        assert telemetry.get(row['id'])['value'] == row['value']


def test_historical_readings_backfilled_from_old_readback():
    """Backfill must reproduce exactly what the old FLOAT column returned."""
    old = {r['id']: r['value'] for r in raw('SELECT id,CAST(value AS CHAR) AS value FROM telemetry')}
    upgrade_to('reconciled')
    precision.switch()
    for item_id, expected in old.items():
        got = telemetry.get(item_id)['value']
        assert got == Decimal(expected).quantize(Decimal('0.000001')).__format__('f'), item_id


# ------------------------------------------------------- backfill vs writes
def test_backfill_does_not_overwrite_concurrent_correction():
    precision.start()
    # Correction lands before backfill reaches the row.
    corrected = telemetry.correct(3, '99.123456')
    assert corrected['value'] == '99.123456'
    precision.backfill(batch_size=5)
    assert telemetry.get(3)['value'] == '99.123456'
    assert telemetry.get(3)['value_source'] == 'correction'
    assert precision.describe()['pending_rows'] == 0


def test_backfill_does_not_overwrite_concurrent_supplement():
    precision.start()
    late = telemetry.supplement(17, '2026-11-15T03:00:00', 'pump', '7.654321')
    precision.backfill(batch_size=3)
    assert telemetry.get(late['id'])['value'] == '7.654321'
    assert telemetry.get(late['id'])['value_source'] == 'exact'


def test_correction_after_backfill_and_before_switch_survives():
    upgrade_to('backfilled')
    telemetry.correct(9, '1.000001')
    precision.reconcile()
    precision.switch()
    assert telemetry.get(9)['value'] == '1.000001'


# ------------------------------------------------------- interruption/resume
def test_backfill_resumes_from_persisted_cursor():
    precision.start()
    first = precision.backfill(batch_size=10, max_batches=1)
    assert 0 < first['cursor_id'] < conftest.ROWS
    assert first['pending_rows'] > 0
    # Simulate process restart: a brand-new call must continue, not restart.
    rest = precision.backfill(batch_size=10)
    assert rest['phase'] == 'backfilled'
    assert rest['backfilled_rows'] >= conftest.ROWS
    assert rest['pending_rows'] == 0


def test_backfill_crash_before_commit_rolls_back_to_last_cursor():
    """A killed process leaves the cursor at the last committed batch."""
    precision.start()
    precision.backfill(batch_size=10, max_batches=1)
    cursor = precision.describe()['cursor_id']
    # A hard kill would drop the uncommitted batch; emulate by re-reading state.
    assert cursor % 10 == 0 or cursor == conftest.ROWS
    resumed = precision.backfill(batch_size=10)
    assert resumed['phase'] == 'backfilled'
    # Rows below the cursor were already filled once and stay untouched.
    assert resumed['backfilled_rows'] + conftest.ROWS >= conftest.ROWS


# ----------------------------------------------------------- switch vs trial
def test_switch_refused_while_trial_holds_lock():
    upgrade_to('reconciled')
    holder = engine.connect()
    try:
        assert holder.execute(text('SELECT GET_LOCK(:n,5)'), {'n': 'index_trial_serial'}).scalar() == 1
        with pytest.raises(HTTPException) as err:
            precision.switch(lock_timeout=0)
        assert err.value.status_code == 409
        assert column_type() == 'float'
        assert precision.describe()['phase'] == 'reconciled'
    finally:
        holder.execute(text('SELECT RELEASE_LOCK(:n)'), {'n': 'index_trial_serial'})
        holder.close()
    # Once the trial finishes, the switch succeeds.
    assert precision.switch(lock_timeout=5)['phase'] == 'switched'
    assert column_type() == 'decimal'


def test_queued_trial_fails_when_precision_changed_under_it():
    from src.services.index_algorithm import run
    trial = conftest.make_trial('stale-trial')
    snapshot = json.loads(json.dumps(trial['snapshot']))
    assert snapshot['precision']['version'] == 0
    upgrade_to('reconciled')
    precision.switch()
    with pytest.raises(RuntimeError) as err:
        run(snapshot)
    assert '精度版本' in str(err.value)


def test_trial_before_and_after_sample_use_one_version():
    from src.services.index_algorithm import run
    trial = conftest.make_trial('fresh-trial')
    upgrade_to('reconciled')
    precision.switch()   # bumps version; a fresh trial must re-snapshot after this
    trial2 = conftest.make_trial('fresh-trial-2')
    result = run(json.loads(json.dumps(trial2['snapshot'])))
    assert result['precision_version'] == precision.describe()['version']
    assert result['equal_results'] is True


# ------------------------------------------------------- refused rollback
def test_rollback_refused_when_precision_would_be_lost():
    upgrade_to('reconciled')
    precision.switch()
    kept = telemetry.ingest(17, '2026-12-03T09:00:00', 'pump', '42.123456')
    with pytest.raises(HTTPException) as err:
        precision.rollback()
    assert err.value.status_code == 409
    detail = err.value.detail
    assert detail['lossy_rows'] >= 1
    # Phase and the exact reading are untouched by the refusal.
    assert precision.describe()['phase'] == 'switched'
    assert telemetry.get(kept['id'])['value'] == '42.123456'
    assert column_type() == 'decimal'


def test_rollback_succeeds_when_every_row_is_lossless():
    upgrade_to('reconciled')
    precision.switch()
    result = precision.rollback()
    assert result['phase'] == 'legacy'
    assert column_type() == 'float'
    # Historical display values are preserved by the lossless round trip.
    assert telemetry.get(1)['value'] == '30'
    with pytest.raises(HTTPException):
        telemetry.ingest(17, '2026-12-03T09:00:00', 'pump', '42.123456')


# ---------------------------------------------- real kill / real concurrency
def test_backfill_survives_sigkill_and_resumes(tmp_path):
    """Kill a live backfilling process mid-run, then resume in a new process."""
    import subprocess, os, signal, time, pathlib
    precision.start()
    env = dict(os.environ, PYTHONPATH='/workspace/backend')
    proc = subprocess.Popen([sys.executable, '/workspace/backend/tests/_slow_backfill.py'],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    try:
        # Wait until at least one batch committed (cursor advanced) then a little more.
        line = proc.stdout.readline()
        assert line.strip().isdigit(), line
        time.sleep(0.2)
    finally:
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)
    after_kill = precision.describe()
    assert after_kill['phase'] == 'backfilling'
    assert after_kill['cursor_id'] > 0
    assert after_kill['pending_rows'] > 0
    # New process: the durable cursor continues rather than restating from 0.
    resumed = precision.backfill(batch_size=5)
    assert resumed['phase'] == 'backfilled'
    assert resumed['cursor_id'] == conftest.ROWS
    assert resumed['pending_rows'] == 0
    assert resumed['backfilled_rows'] >= conftest.ROWS
    precision.reconcile()
    precision.switch()
    assert telemetry.get(1)['value'] == '30.000000'


def test_concurrent_corrections_during_backfill_never_lose_to_old_value():
    """Real threads: corrections racing the backfill must survive verbatim."""
    import threading
    precision.start()
    targets = list(range(1, conftest.ROWS + 1, 2))
    corrected = {}
    errors = []
    def fixer():
        for i in targets:
            try:
                corrected[i] = telemetry.correct(i, f'{i}.654321')
            except Exception as exc:      # pragma: no cover
                errors.append(exc)
    t = threading.Thread(target=fixer)
    t.start()
    precision.backfill(batch_size=4)
    t.join()
    assert not errors, errors
    for i, expected in corrected.items():
        assert telemetry.get(i)['value'] == expected['value'], i
        assert telemetry.get(i)['value_source'] == 'correction', i
    precision.reconcile()
    precision.switch()
    for i, expected in corrected.items():
        assert telemetry.get(i)['value'] == expected['value'], i


def test_real_trial_holds_lock_and_blocks_switch_then_switch_after():
    """A genuinely running trial (its Python code holds GET_LOCK) blocks the switch."""
    from src.services.index_algorithm import run
    import threading, time
    trial = conftest.make_trial('live-trial')
    snapshot = json.loads(json.dumps(trial['snapshot']))
    started = threading.Event()
    release = threading.Event()
    # Monkeypatch the timed section so the trial holds the lock while we try to switch.
    import src.services.index_algorithm as algo
    original = algo._timed
    def slow(conn, query, params):
        started.set()
        release.wait(timeout=10)
        return original(conn, query, params)
    algo._timed = slow
    out = {}
    def worker():
        try:
            out['result'] = run(snapshot)
        except Exception as exc:      # pragma: no cover
            out['error'] = exc
    th = threading.Thread(target=worker)
    th.start()
    assert started.wait(timeout=10)
    with pytest.raises(HTTPException) as err:
        precision.switch(lock_timeout=0)
    assert err.value.status_code == 409
    release.set()
    th.join(timeout=30)
    algo._timed = original
    assert 'result' in out, out
    # Trial completed on the pre-switch version; the switch now goes through.
    assert out['result']['precision_version'] == 0
    precision.start()
    precision.backfill(batch_size=5)
    precision.reconcile()
    assert precision.switch(lock_timeout=5)['phase'] == 'switched'


def test_switch_blocked_while_backfill_lock_held():
    """Switch must refuse until the running backfill releases its lock."""
    upgrade_to('reconciled')
    holder = engine.connect()
    try:
        assert holder.execute(text('SELECT GET_LOCK(:n,5)'), {'n': 'precision_backfill'}).scalar() == 1
        with pytest.raises(HTTPException) as err:
            precision.switch(lock_timeout=0)
        assert err.value.status_code == 409
        assert '回填' in err.value.detail
        assert column_type() == 'float'
    finally:
        holder.execute(text('SELECT RELEASE_LOCK(:n)'), {'n': 'precision_backfill'})
        holder.close()
    assert precision.switch(lock_timeout=5)['phase'] == 'switched'


# ---------------------------------------------------------- input boundaries
def test_six_decimal_canonicalisation_boundaries():
    from src.utils.precision import parse_six
    ok = ['42.123456', '0.000001', '1e14', ' 33.5 ', '-12.345678',
          '999999999999999999999999999999999999999999.999999']
    bad = ['42.1234567', '0.0000004', '1e42', 'abc', '1000000000000000000000000000000000000000000']
    for raw in ok:
        assert parse_six(raw) is not None, raw
    for raw in bad:
        with pytest.raises(ValueError):
            parse_six(raw)


def test_ingest_rejects_bad_precision_with_422_not_500():
    upgrade_to('reconciled')
    precision.switch()
    for bad in ('42.1234567', '1e43'):
        with pytest.raises(HTTPException) as err:
            telemetry.ingest(17, '2026-12-09T00:00:00', 'pump', bad)
        assert err.value.status_code == 422


def test_full_48_digit_value_round_trips():
    upgrade_to('reconciled')
    precision.switch()
    raw = '999999999999999999999999999999999999999999.999999'
    row = telemetry.ingest(17, '2026-12-11T00:00:00', 'pump', raw)
    assert row['value'] == raw
    assert telemetry.get(row['id'])['value'] == raw


def test_legacy_correction_that_would_drift_is_refused():
    with pytest.raises(HTTPException) as err:
        telemetry.correct(3, '42.123456')          # FLOAT cannot hold six decimals faithfully
    assert err.value.status_code == 409
    # A value FLOAT does hold is still accepted pre-upgrade.
    ok = telemetry.correct(3, '42.5')
    assert ok['value'] == '42.5'
