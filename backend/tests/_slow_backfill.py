"""Child process used by the interruption test: backfills slowly so the parent can SIGKILL it."""
import os
import sys
import time

sys.path.insert(0, '/workspace/backend')
os.environ.setdefault('DATABASE_URL', 'mysql+pymysql://trial:trialpass@127.0.0.1:17741/indextrial_test')
os.environ.setdefault('REDIS_URL', 'redis://127.0.0.1:16379/1')
os.environ.setdefault('JWT_SECRET', 'test-secret')

from src.services import precision

if __name__ == '__main__':
    while True:
        state = precision.backfill(batch_size=3, max_batches=1)
        print(state['cursor_id'], flush=True)
        time.sleep(0.05)
