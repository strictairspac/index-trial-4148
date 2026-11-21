#!/usr/bin/env bash
# Run the precision-upgrade tests against a live MySQL 8.0 + Redis 7.
#
# This environment had no Docker, so the suite was run against a locally
# extracted MySQL 8.0.44 / Redis 7.0.15. Point the vars at any equivalent server.
set -euo pipefail
export PYTHONPATH="${PYTHONPATH:-/workspace/backend}:/workspace/backend/tests"
export DATABASE_URL="${DATABASE_URL:-mysql+pymysql://trial:trialpass@127.0.0.1:17741/indextrial_test}"
export REDIS_URL="${REDIS_URL:-redis://127.0.0.1:16379/1}"
export JWT_SECRET="${JWT_SECRET:-test-secret}"
cd /workspace/backend
python3 -m pytest tests/ -v "$@"
