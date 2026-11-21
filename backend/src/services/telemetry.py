"""Telemetry ingestion, correction and reads that stay correct across the upgrade.

Writes are canonicalised to six decimals before touching the DB, so new readings
are stored verbatim and read back verbatim whether the column is still FLOAT
(backfill shape: value_exact holds the exact form) or already DECIMAL.
All writes take precision_write_guard, which the switch DDL also holds; the
backfill's UPDATE re-checks value_exact IS NULL so a legacy value can never
overwrite a concurrent correction/supplement.
"""
from fastapi import HTTPException
from sqlalchemy import text
from src.config.database import engine
from src.services.precision import write_guard, state, shape_phase
from src.utils.precision import parse_six, fmt_six

_DEVICES = ('pump', 'motor', 'valve')


def _canonical(raw):
    try:
        return parse_six(raw)
    except ValueError as exc:
        raise HTTPException(422, str(exc))


def _select_expr(shape):
    if shape == 'switched':
        return ('CAST(value AS CHAR) AS value, value_source, value_version')
    if shape == 'backfill_shape':
        return ('CAST(value AS CHAR) AS value, '
                "CASE WHEN value_source IN ('exact','correction') "
                'THEN CAST(value_exact AS CHAR) END AS value_exact, '
                'value_source, value_version')
    return 'CAST(value AS CHAR) AS value, NULL AS value_source, NULL AS value_version, NULL AS value_exact'


def _readback(row, shape):
    if shape == 'backfill_shape' and row.get('value_exact') is not None:
        return fmt_six(row['value_exact'])
    if shape == 'switched':
        return fmt_six(row['value'])
    # legacy / unbackfilled: report what the old FLOAT actually returns
    return row['value']


def _project(conn, shape, where, params):
    return conn.execute(text(
        f'SELECT id,sensor_id,recorded_at,device_type,{_select_expr(shape)} '
        f'FROM telemetry WHERE {where}'), params).mappings().all()


def _one(conn, shape, item_id):
    rows = _project(conn, shape, 'id=:id', {'id': item_id})
    if not rows:
        raise HTTPException(404, '未找到遥测记录')
    data = dict(rows[0])
    data['value'] = _readback(data, shape)
    return data


def _current_shape(conn=None):
    if conn is not None:
        return state(conn)['shape']
    with engine.connect() as c:
        return state(c)['shape']


def _insert(conn, shape, sensor_id, recorded_at, device_type, value):
    params = {'sensor_id': sensor_id, 'recorded_at': recorded_at,
              'device_type': device_type, 'value': str(value)}
    if shape == 'backfill_shape':
        conn.execute(text(
            'INSERT INTO telemetry(sensor_id,recorded_at,device_type,value,value_exact,value_source,value_version) '
            "VALUES (:sensor_id,:recorded_at,:device_type,CAST(:value AS FLOAT),:value,'exact',1)"), params)
    else:
        conn.execute(text(
            'INSERT INTO telemetry(sensor_id,recorded_at,device_type,value,value_source,value_version) '
            "VALUES (:sensor_id,:recorded_at,:device_type,:value,'exact',1)"), params)
    return conn.execute(text('SELECT LAST_INSERT_ID()')).scalar()


def ingest(sensor_id, recorded_at, device_type, value):
    if device_type not in _DEVICES:
        raise HTTPException(422, '设备类型无效')
    if _current_shape() == 'legacy':
        raise HTTPException(409, '精度升级尚未启动，六位小数无法保真入库；请先启动精度升级')
    canonical = _canonical(value)
    with write_guard() as conn:
        shape = _current_shape(conn)
        if shape == 'legacy':
            raise HTTPException(409, '精度升级尚未启动，六位小数无法保真入库；请先启动精度升级')
        new_id = _insert(conn, shape, sensor_id, recorded_at, device_type, canonical)
        conn.commit()
        return _one(conn, shape, new_id)


def supplement(sensor_id, recorded_at, device_type, value):
    """Late-arriving reading; same path as ingest once the upgrade has started."""
    return ingest(sensor_id, recorded_at, device_type, value)


def correct(item_id, value):
    canonical = _canonical(value)
    with write_guard() as conn:
        shape = _current_shape(conn)
        if not conn.execute(text('SELECT 1 FROM telemetry WHERE id=:id'), {'id': item_id}).first():
            raise HTTPException(404, '未找到遥测记录')
        if shape == 'backfill_shape':
            conn.execute(text(
                'UPDATE telemetry SET value=CAST(:v AS FLOAT), value_exact=:v, '
                "value_source='correction', value_version=value_version+1 WHERE id=:id"),
                {'v': str(canonical), 'id': item_id})
        elif shape == 'switched':
            conn.execute(text(
                "UPDATE telemetry SET value=:v, value_source='correction', "
                'value_version=value_version+1 WHERE id=:id'),
                {'v': str(canonical), 'id': item_id})
        else:
            # Legacy FLOAT column: reject a value it cannot hold rather than round it,
            # which is the same drift the upgrade exists to remove.
            survives = conn.execute(text(
                'SELECT CAST(CAST(CAST(:v AS FLOAT) AS CHAR) AS DECIMAL(48,6))=:v'),
                {'v': str(canonical)}).scalar()
            if not survives:
                raise HTTPException(409, '精度升级尚未启动，六位小数更正会丢失精度；请先启动精度升级')
            conn.execute(text('UPDATE telemetry SET value=:v WHERE id=:id'),
                         {'v': str(canonical), 'id': item_id})
        conn.commit()
        return _one(conn, shape, item_id)


def get(item_id):
    with engine.connect() as conn:
        return _one(conn, _current_shape(conn), item_id)


def listing(sensor_id=None, limit=100):
    limit = max(1, min(int(limit), 500))
    with engine.connect() as conn:
        s = state(conn)
        shape = s['shape']
        where, params = '1=1', {'limit': limit}
        if sensor_id is not None:
            where, params['sensor_id'] = 'sensor_id=:sensor_id', sensor_id
        rows = conn.execute(text(
            f'SELECT id,sensor_id,recorded_at,device_type,{_select_expr(shape)} '
            f'FROM telemetry WHERE {where} '
            'ORDER BY id DESC LIMIT :limit'), params).mappings().all()
        items = []
        for r in rows:
            d = dict(r)
            d['value'] = _readback(d, shape)
            items.append(d)
        return {'phase': s['phase'], 'column_type': s['value_type'],
                'shape': shape, 'precision_version': s['version'], 'items': items}
