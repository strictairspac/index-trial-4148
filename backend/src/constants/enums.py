ROLES=('admin','operator','viewer')
STATUSES=('queued','running','completed','failed')
COLUMNS=('sensor_id','recorded_at','device_type','value')
PRECISION_PHASES=('legacy','backfilling','backfilled','reconciled','switching','switched','rolling_back')
VALUE_SOURCES=('legacy','exact','correction')
TRIAL_LOCK='index_trial_serial'
PRECISION_LOCK='precision_backfill'
PRECISION_WRITE_GUARD='precision_write_guard'
