import os
import sys

sys.path.insert(0, '/workspace/backend')

os.environ.setdefault('DATABASE_URL', 'mysql+pymysql://trial:trialpass@127.0.0.1:17741/indextrial_test')
os.environ.setdefault('REDIS_URL', 'redis://127.0.0.1:16379/1')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('SEED_DEMO', 'false')

import pytest
from sqlalchemy import text

from src.config.database import Base, engine, Session
import src.models.identity       # noqa: F401
import src.models.workload       # noqa: F401
import src.models.candidate      # noqa: F401
import src.models.experiment     # noqa: F401
import src.models.telemetry      # noqa: F401
import src.models.precision      # noqa: F401

ROWS = 40


def reset_schema():
    with engine.begin() as c:
        c.execute(text('SET FOREIGN_KEY_CHECKS=0'))
        for t in Base.metadata.sorted_tables:
            c.execute(text(f'DROP TABLE IF EXISTS `{t.name}`'))
        c.execute(text('SET FOREIGN_KEY_CHECKS=1'))
    Base.metadata.create_all(engine)


def seed_legacy_rows(n=ROWS):
    """Rows carrying values a FLOAT can display exactly (no six-decimal loss yet)."""
    with Session() as s:
        s.add(__import__('src.models.precision', fromlist=['PrecisionState'])
              .PrecisionState(id=1, phase='legacy', version=0, cursor_id=0,
                              total_rows=0, backfilled_rows=0))
        s.commit()
    with engine.begin() as c:
        values = [round(30 + (i * 17 % 900) / 10, 2) for i in range(n)]
        c.execute(text(
            'INSERT INTO telemetry(id,sensor_id,recorded_at,device_type,value) '
            'VALUES (:id,:sensor,:ts,:dev,:v)'),
            [{'id': i + 1, 'sensor': i % 100 + 1,
              'ts': f'2026-11-{(i % 28) + 1:02d}T00:00:00',
              'dev': ['pump', 'motor', 'valve'][i % 3], 'v': v}
             for i, v in enumerate(values)])


@pytest.fixture(autouse=True)
def clean_db():
    reset_schema()
    seed_legacy_rows()
    yield
    with engine.connect() as c:
        for lock in ('index_trial_serial', 'precision_backfill', 'precision_write_guard'):
            c.execute(text('SELECT RELEASE_LOCK(:n)'), {'n': lock})
        c.commit()


def make_trial(name='t', candidate_id=1):
    """Queue an experiment through the real service (records a precision snapshot)."""
    from src.models.workload import Workload
    from src.models.candidate import Candidate
    from src.schemas.business import ExperimentInput
    from src.services import experiments
    with Session() as s:
        w = s.query(Workload).first()
        if not w:
            w = Workload(name='w-s', sensor_id=17, start_at='2026-11-01T00:00:00',
                         end_at='2026-11-30T23:59:59', description='')
            s.add(w)
            s.flush()
            s.add(Candidate(name='c-s', workload_id=w.id,
                            columns=['sensor_id', 'recorded_at'], note=''))
            s.commit()
        cid = s.query(Candidate).first().id
    return experiments.create(ExperimentInput(name=name, candidate_id=cid))
