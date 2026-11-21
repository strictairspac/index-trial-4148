"""Six-decimal precision upgrade for telemetry.value.

State machine (persisted in precision_state):
  legacy -> backfilling -> backfilled -> reconciled -> switching -> switched
  switched -> rolling_back -> legacy (only when no row would lose precision)

The forward switch is a single atomic ALTER TABLE inside the same advisory lock
that serialises index trials, so a trial's before/after sample cannot straddle
the switch. Backfill only fills rows that no concurrent supplement/correction
has claimed (value_exact IS NULL AND value_source IS NULL).
"""
import hashlib
from contextlib import contextmanager
from datetime import datetime
from fastapi import HTTPException
from sqlalchemy import text
from src.config.database import engine
from src.constants.enums import (
    TRIAL_LOCK, PRECISION_LOCK, PRECISION_WRITE_GUARD,
)

FLOAT_LIMIT_TEXT = '3.4028234663852886E38'
BACKFILL_SHAPE_PHASES = ('backfilling', 'backfilled', 'reconciled', 'switching')


# ---------------------------------------------------------------- inspection
def _columns(conn):
    rows = conn.execute(text(
        "SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='telemetry'")).mappings().all()
    if not rows:
        raise RuntimeError('telemetry 表不存在')
    return {r['COLUMN_NAME']: dict(r) for r in rows}


def inspect(conn):
    """Return (shape, value_type) where shape is legacy|backfill_shape|switched."""
    cols = _columns(conn)
    if 'value' not in cols:
        return 'unknown', None
    value_type = cols['value']['DATA_TYPE']
    has_exact = 'value_exact' in cols
    has_legacy = 'value_legacy_float' in cols
    if value_type in ('decimal', 'numeric') and has_legacy and not has_exact:
        return 'switched', value_type
    if value_type in ('float', 'double') and has_exact and not has_legacy:
        return 'backfill_shape', value_type
    if value_type in ('float', 'double') and not has_exact and not has_legacy:
        return 'legacy', value_type
    return 'unknown', value_type


def shape_phase(shape):
    return {'legacy': 'legacy', 'switched': 'switched', 'backfill_shape': 'backfilling'}.get(shape, 'unknown')


# --------------------------------------------------------------------- state
_STATE_COLUMNS = ('phase', 'version', 'target_id', 'cursor_id', 'total_rows',
                  'backfilled_rows', 'reconcile_total', 'reconcile_mismatch',
                  'reconcile_checksum', 'reconcile_at', 'lossy_rows', 'message')


def _ensure_state(conn):
    conn.execute(text(
        "INSERT IGNORE INTO precision_state "
        "(id,phase,version,target_id,cursor_id,total_rows,backfilled_rows,"
        "reconcile_total,reconcile_mismatch,message) "
        "VALUES (1,'legacy',0,NULL,0,0,0,0,0,'')"))
    conn.commit()


def _read_state(conn):
    row = conn.execute(text('SELECT * FROM precision_state WHERE id=1')).mappings().first()
    if not row:
        _ensure_state(conn)
        row = conn.execute(text('SELECT * FROM precision_state WHERE id=1')).mappings().one()
    return dict(row)


def _write_state(conn, **fields):
    if not fields:
        return
    assignments = ','.join(f'`{k}`=:{k}' for k in fields)
    fields['id'] = 1
    conn.execute(text(f'UPDATE precision_state SET {assignments} WHERE id=1'), fields)


def _sync(conn, state):
    """Self-heal persisted state from the real column shape after a crash."""
    shape, value_type = inspect(conn)
    phase = state['phase']
    changed = {}
    if shape == 'switched' and phase != 'switched':
        changed = {'phase': 'switched', 'version': state['version'] + 1,
                   'message': '检测到切换已完成，已恢复状态'}
    elif shape == 'legacy' and phase != 'legacy':
        changed = {'phase': 'legacy', 'version': state['version'] + 1,
                   'message': '检测到旧精度列结构，已恢复为 legacy'}
    elif shape == 'backfill_shape' and phase == 'legacy':
        changed = {'phase': 'backfilling', 'version': state['version'] + 1,
                   'message': '检测到影子列，已恢复回填阶段'}
    elif shape == 'backfill_shape' and phase == 'rolling_back':
        changed = {'message': '回切中断，请再次调用回切完成收尾'}
    elif shape == 'backfill_shape' and phase == 'switching':
        changed = {'message': '切换中断，请再次调用切换完成收尾'}
    if changed:
        _write_state(conn, **changed)
        conn.commit()
        state.update(changed)
    state['shape'] = shape
    state['value_type'] = value_type
    return state


def state(conn=None):
    own = conn is None
    conn = conn or engine.connect()
    try:
        _ensure_state(conn)
        return _sync(conn, _read_state(conn))
    finally:
        if own:
            conn.close()


def current_snapshot():
    """Precision fingerprint recorded into an experiment's immutable snapshot."""
    with engine.connect() as conn:
        s = state(conn)
        return {'version': s['version'], 'phase': s['phase'], 'value_type': s['value_type']}


def verify_trial(conn, snapshot):
    """Called by the worker while it holds the trial lock, before any sample is read.

    A queued trial must run on the exact precision version it was queued under;
    otherwise its before/after sample could straddle the switch.
    """
    expected = (snapshot or {}).get('precision')
    current = state(conn)
    if expected is None:
        if current['phase'] != 'legacy':
            raise RuntimeError('试验排队后精度已升级，样本版本不再一致；请新建试验')
        return current
    if expected.get('version') != current['version']:
        raise RuntimeError(
            f"精度版本已从 v{expected.get('version')} 变为 v{current['version']}，"
            '为避免混用两版样本已终止本次试验；请新建试验')
    return current


# --------------------------------------------------------------------- locks
def _get_lock(conn, name, timeout):
    return conn.execute(text('SELECT GET_LOCK(:n,:t)'), {'n': name, 't': timeout}).scalar() == 1


def _release_lock(conn, name):
    try:
        conn.execute(text('SELECT RELEASE_LOCK(:n)'), {'n': name})
    finally:
        conn.commit()


@contextmanager
def write_guard(timeout=10):
    """Serialise API writes with the switch DDL (advisory lock on one connection)."""
    conn = engine.connect()
    try:
        if not _get_lock(conn, PRECISION_WRITE_GUARD, timeout):
            raise HTTPException(409, '精度切换正在进行，请稍后重试')
        yield conn
    finally:
        try:
            _release_lock(conn, PRECISION_WRITE_GUARD)
        finally:
            conn.close()


# ------------------------------------------------------------------- backfill
def _pending(conn):
    return conn.execute(text(
        'SELECT COUNT(*) FROM telemetry WHERE value_exact IS NULL AND value_source IS NULL')).scalar()


def _payload(conn, s):
    out = {k: s.get(k) for k in _STATE_COLUMNS}
    out['pending_rows'] = _pending(conn) if s.get('shape') == 'backfill_shape' else 0
    out['value_type'] = s.get('value_type')
    out['shape'] = s.get('shape')
    for key in ('reconcile_at', 'updated_at'):
        if isinstance(out.get(key), datetime):
            out[key] = out[key].isoformat()
    return out


def describe():
    with engine.connect() as conn:
        s = state(conn)
        return _payload(conn, s)


def start(actor=None):
    with engine.connect() as conn:
        s = state(conn)
        shape = s['shape']
        if shape == 'switched':
            raise HTTPException(409, '已切换到新精度，不能再启动回填')
        if shape == 'legacy':
            conn.execute(text(
                'ALTER TABLE telemetry '
                'ADD COLUMN value_exact DECIMAL(48,6) NULL, '
                'ADD COLUMN value_source VARCHAR(16) NULL, '
                'ADD COLUMN value_version INT NOT NULL DEFAULT 0, '
                'MODIFY COLUMN value FLOAT NULL'))
            conn.commit()
            s = state(conn)
        elif s['phase'] in ('backfilled', 'reconciled'):
            raise HTTPException(409, '历史读数已回填，请对账后切换；如需补录请使用补录接口')
        total = conn.execute(text('SELECT COUNT(*) FROM telemetry')).scalar()
        _write_state(conn, phase='backfilling', version=s['version'] + 1, target_id=None,
                     cursor_id=0, total_rows=total, backfilled_rows=0,
                     reconcile_total=0, reconcile_mismatch=0, reconcile_checksum=None,
                     reconcile_at=None, lossy_rows=None, message='精度升级已启动，开始回填历史读数')
        conn.commit()
        response = _payload(conn, state(conn))
    return response


def backfill(batch_size=500, max_batches=None, actor=None):
    if batch_size < 1 or batch_size > 5000:
        raise HTTPException(422, 'batch_size 必须在 1 到 5000 之间')
    conn = engine.connect()
    try:
        if not _get_lock(conn, PRECISION_LOCK, 0):
            raise HTTPException(409, '已有回填任务在执行，请等待其结束或续跑')
        s = state(conn)
        if s['shape'] == 'switched':
            raise HTTPException(409, '已切换到新精度，不能回填')
        if s['shape'] == 'legacy':
            raise HTTPException(409, '请先启动精度升级')
        if s['phase'] not in ('backfilling', 'backfilled', 'reconciled'):
            raise HTTPException(409, f"当前阶段 {s['phase']} 不允许回填")
        cursor = s['cursor_id'] or 0
        batches = 0
        restarts = 0
        while True:
            max_id = conn.execute(text('SELECT COALESCE(MAX(id),0) FROM telemetry')).scalar() or 0
            if cursor >= max_id:
                if _pending(conn) == 0 or restarts >= 3:
                    break
                cursor = 0
                restarts += 1
                continue
            batch_max = min(cursor + batch_size, max_id)
            if not _get_lock(conn, PRECISION_WRITE_GUARD, 10):
                raise HTTPException(409, '精度切换正在进行，请稍后重试')
            try:
                result = conn.execute(text(
                    'UPDATE telemetry '
                    "SET value_exact=CAST(CAST(value AS CHAR) AS DECIMAL(48,6)), "
                    "value_source='legacy', value_version=value_version+1 "
                    'WHERE id>:cursor AND id<=:batch_max '
                    'AND value_exact IS NULL AND value_source IS NULL AND value IS NOT NULL'),
                    {'cursor': cursor, 'batch_max': batch_max})
                filled = result.rowcount
                _write_state(conn, cursor_id=batch_max,
                             backfilled_rows=(s['backfilled_rows'] or 0) + filled,
                             message=f'回填中：游标 {batch_max}')
                conn.commit()
            finally:
                _release_lock(conn, PRECISION_WRITE_GUARD)
            s = state(conn)
            cursor = batch_max
            batches += 1
            if s['shape'] == 'switched':
                raise HTTPException(409, '回填期间精度已完成切换，回填已停止')
            if max_batches and batches >= max_batches:
                break
        if _pending(conn) == 0 and cursor >= (conn.execute(text('SELECT COALESCE(MAX(id),0) FROM telemetry')).scalar() or 0):
            _write_state(conn, phase='backfilled', message='历史读数回填完成，等待逐条对账')
            conn.commit()
        return _payload(conn, state(conn))
    finally:
        try:
            _release_lock(conn, PRECISION_LOCK)
        finally:
            conn.close()


# ------------------------------------------------------------------ reconcile
def _mismatch_counts(conn):
    pending = _pending(conn)
    legacy_bad = conn.execute(text(
        'SELECT COUNT(*) FROM telemetry '
        "WHERE (value_source='legacy' OR value_source IS NULL) AND ("
        'value IS NULL OR value_exact IS NULL OR '
        'value_exact <> CAST(CAST(value AS CHAR) AS DECIMAL(48,6)))')).scalar()
    exact_bad = conn.execute(text(
        'SELECT COUNT(*) FROM telemetry '
        "WHERE value_source IN ('exact','correction') AND value_exact IS NULL")).scalar()
    unknown_bad = conn.execute(text(
        'SELECT COUNT(*) FROM telemetry '
        "WHERE value_source IS NOT NULL AND value_source NOT IN ('legacy','exact','correction')")).scalar()
    return {'pending': pending, 'legacy_mismatch': legacy_bad,
            'exact_mismatch': exact_bad, 'unknown_source': unknown_bad,
            'total': pending + legacy_bad + exact_bad + unknown_bad}


def _row_checksum(conn):
    digest = hashlib.sha256()
    total = 0
    last = 0
    while True:
        rows = conn.execute(text(
            'SELECT id,CAST(value_exact AS CHAR) AS ve,value_source,value_version '
            'FROM telemetry WHERE id>:last ORDER BY id LIMIT 1000'), {'last': last}).mappings().all()
        if not rows:
            return digest.hexdigest(), total
        for r in rows:
            digest.update(f"{r['id']}|{r['ve']}|{r['value_source']}|{r['value_version']}\n".encode())
            last = r['id']
            total += 1


def reconcile(actor=None):
    conn = engine.connect()
    try:
        if not _get_lock(conn, PRECISION_LOCK, 0):
            raise HTTPException(409, '回填正在进行，请等待其结束后再对账')
        s = state(conn)
        if s['shape'] != 'backfill_shape':
            raise HTTPException(409, '当前列结构不支持对账，请先完成回填')
        if s['phase'] not in ('backfilled', 'reconciled'):
            raise HTTPException(409, '回填尚未完成，不能对账')
        # state()/_ensure_state leave an open transaction; clear it before SET TRANSACTION.
        conn.commit()
        conn.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'))
        conn.commit()
        conn.execute(text('START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY'))
        counts = _mismatch_counts(conn)
        checksum, scanned = _row_checksum(conn)
        conn.commit()
        mismatch = counts['total']
        ok = mismatch == 0
        _write_state(conn, reconcile_total=scanned, reconcile_mismatch=mismatch,
                     reconcile_checksum=checksum, reconcile_at=datetime.now(),
                     phase='reconciled' if ok else 'backfilled',
                     message=('逐条对账通过，可以原子切换' if ok
                              else f"对账发现 {mismatch} 条不一致，请补齐后重试"))
        conn.commit()
        data = _payload(conn, state(conn))
        data['mismatch'] = counts
        return data
    finally:
        try:
            _release_lock(conn, PRECISION_LOCK)
        finally:
            conn.close()


# --------------------------------------------------------------------- switch
_SWITCH_DDL = ('ALTER TABLE telemetry '
               'CHANGE COLUMN value value_legacy_float FLOAT NULL, '
               'CHANGE COLUMN value_exact value DECIMAL(48,6) NOT NULL')


def switch(actor=None, lock_timeout=5):
    conn = engine.connect()
    guard = False
    backfill = False
    trial = False
    try:
        # Lock order everywhere: write_guard -> backfill -> trial.
        guard = _get_lock(conn, PRECISION_WRITE_GUARD, lock_timeout)
        if not guard:
            raise HTTPException(409, '其他写入正在进行，请稍后重试切换')
        backfill = _get_lock(conn, PRECISION_LOCK, lock_timeout)
        if not backfill:
            raise HTTPException(409, '回填仍在进行，请等回填结束后再切换')
        trial = _get_lock(conn, TRIAL_LOCK, lock_timeout)
        if not trial:
            raise HTTPException(409, '索引试验正在执行，切换会混用两版样本，请等试验结束后重试')
        s = state(conn)
        if s['shape'] == 'switched':
            return _payload(conn, s)
        if s['shape'] != 'backfill_shape':
            raise HTTPException(409, '当前列结构不支持切换，请先完成回填')
        if s['phase'] not in ('reconciled', 'switching'):
            raise HTTPException(409, '请先逐条对账通过后再切换')
        counts = _mismatch_counts(conn)
        if counts['total']:
            _write_state(conn, reconcile_mismatch=counts['total'], phase='backfilled',
                         message=f"切换前复核发现 {counts['total']} 条不一致，已退回对账阶段")
            conn.commit()
            raise HTTPException(409, '逐条对账未通过，已阻止切换；请修正后重新对账')
        checksum, scanned = _row_checksum(conn)
        _write_state(conn, phase='switching', reconcile_total=scanned,
                     reconcile_mismatch=0, reconcile_checksum=checksum,
                     reconcile_at=datetime.now(), lossy_rows=None,
                     message='正在原子切换列类型')
        conn.commit()
        try:
            conn.execute(text(_SWITCH_DDL))
            conn.commit()
        except Exception:
            _write_state(conn, phase='reconciled',
                         message='切换 DDL 失败，列结构未改变，请重试')
            conn.commit()
            raise
        s2 = state(conn)          # _sync already bumped version on shape change
        _write_state(conn, phase='switched', version=max(s2['version'], s['version'] + 1),
                     message='已切换到六位小数精度（DECIMAL(48,6)）')
        conn.commit()
        return _payload(conn, state(conn))
    finally:
        if trial:
            _release_lock(conn, TRIAL_LOCK)
        if backfill:
            _release_lock(conn, PRECISION_LOCK)
        if guard:
            _release_lock(conn, PRECISION_WRITE_GUARD)
        conn.close()


# ------------------------------------------------------------------- rollback
def _lossy_predicate():
    """Rows whose six-decimal value cannot survive a trip through FLOAT display form.

    Going through CHAR compares against how a FLOAT is actually read back
    (31.7 stays 31.700000); a direct DECIMAL<->FLOAT cast would falsely flag it.
    """
    return (f'ABS(value) > {FLOAT_LIMIT_TEXT} OR '
            'CAST(CAST(CAST(CAST(value AS CHAR) AS FLOAT) AS CHAR) AS DECIMAL(48,6)) <> value')


def _lossy_rows(conn):
    return conn.execute(text(f'SELECT COUNT(*) FROM telemetry WHERE {_lossy_predicate()}')).scalar()


def _lossy_samples(conn, limit=5):
    return [dict(r) for r in conn.execute(text(
        f'SELECT id,CAST(value AS CHAR) AS value FROM telemetry WHERE {_lossy_predicate()} '
        'ORDER BY id LIMIT :limit'), {'limit': limit}).mappings().all()]


def rollback(actor=None, lock_timeout=5):
    conn = engine.connect()
    guard = False
    backfill = False
    trial = False
    try:
        guard = _get_lock(conn, PRECISION_WRITE_GUARD, lock_timeout)
        if not guard:
            raise HTTPException(409, '其他写入正在进行，请稍后重试回切')
        backfill = _get_lock(conn, PRECISION_LOCK, lock_timeout)
        if not backfill:
            raise HTTPException(409, '回填仍在进行，请等回填结束后再回切')
        trial = _get_lock(conn, TRIAL_LOCK, lock_timeout)
        if not trial:
            raise HTTPException(409, '索引试验正在执行，回切会混用两版样本，请等试验结束后重试')
        s = state(conn)
        if s['shape'] == 'legacy':
            return _payload(conn, s)
        if s['shape'] == 'backfill_shape' and s['phase'] == 'rolling_back':
            conn.execute(text('ALTER TABLE telemetry '
                              'DROP COLUMN value_exact, DROP COLUMN value_source, '
                              'DROP COLUMN value_version'))
            conn.commit()
            s = state(conn)
            _write_state(conn, phase='legacy', version=s['version'] + 1,
                         target_id=None, cursor_id=0, backfilled_rows=0,
                         reconcile_total=0, reconcile_mismatch=0, reconcile_checksum=None,
                         reconcile_at=None, lossy_rows=0,
                         message='回切收尾完成，已恢复 FLOAT 精度')
            conn.commit()
            return _payload(conn, state(conn))
        if s['shape'] != 'switched':
            raise HTTPException(409, '当前不在新精度，无法回切')
        lossy = _lossy_rows(conn)
        if lossy:
            samples = _lossy_samples(conn)
            _write_state(conn, lossy_rows=lossy,
                         message=f'{lossy} 条读数无法无损转回 FLOAT，已拒绝回切')
            conn.commit()
            data = _payload(conn, state(conn))
            data['lossy_samples'] = samples
            raise HTTPException(409, {
                'detail': f'{lossy} 条读数切回旧精度会丢失精度，已拒绝回切',
                'lossy_rows': lossy, 'samples': samples})
        _write_state(conn, phase='rolling_back', lossy_rows=0,
                     message='正在无损切回 FLOAT 精度')
        conn.commit()
        conn.execute(text(
            'UPDATE telemetry SET value_legacy_float=CAST(value AS FLOAT) '
            'WHERE value_legacy_float IS NULL'))
        conn.commit()
        conn.execute(text(
            'ALTER TABLE telemetry '
            'CHANGE COLUMN value value_exact DECIMAL(48,6) NULL, '
            'CHANGE COLUMN value_legacy_float value FLOAT NOT NULL'))
        conn.commit()
        conn.execute(text('ALTER TABLE telemetry '
                          'DROP COLUMN value_exact, DROP COLUMN value_source, '
                          'DROP COLUMN value_version'))
        conn.commit()
        s = state(conn)
        _write_state(conn, phase='legacy', version=s['version'] + 1,
                     target_id=None, cursor_id=0, backfilled_rows=0,
                     reconcile_total=0, reconcile_mismatch=0, reconcile_checksum=None,
                     reconcile_at=None, lossy_rows=0,
                     message='已无损回切到 FLOAT 精度')
        conn.commit()
        return _payload(conn, state(conn))
    finally:
        if trial:
            _release_lock(conn, TRIAL_LOCK)
        if backfill:
            _release_lock(conn, PRECISION_LOCK)
        if guard:
            _release_lock(conn, PRECISION_WRITE_GUARD)
        conn.close()


def ensure_state():
    with engine.connect() as conn:
        _ensure_state(conn)
