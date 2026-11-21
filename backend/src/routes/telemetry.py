from fastapi import APIRouter,Depends,HTTPException
from src.middlewares.auth import current,writer,admin
from src.schemas.business import TelemetryInput,CorrectionInput,BackfillInput
from src.services import precision,telemetry
from src.services.audit import log
router=APIRouter()

@router.get('/telemetry')
def listing(sensor_id:int|None=None,limit:int=100,u=Depends(current)):
 return telemetry.listing(sensor_id,limit)

@router.post('/telemetry')
def ingest(data:TelemetryInput,u=Depends(writer)):
 row=telemetry.ingest(data.sensor_id,data.recorded_at,data.device_type,data.value)
 log(u.email,'telemetry.ingest',str(row['id']));return row

@router.post('/telemetry/supplement')
def supplement(data:TelemetryInput,u=Depends(writer)):
 row=telemetry.supplement(data.sensor_id,data.recorded_at,data.device_type,data.value)
 log(u.email,'telemetry.supplement',str(row['id']));return row

@router.get('/telemetry/{item_id}')
def detail(item_id:int,u=Depends(current)):
 return telemetry.get(item_id)

@router.put('/telemetry/{item_id}')
def correct(item_id:int,data:CorrectionInput,u=Depends(writer)):
 row=telemetry.correct(item_id,data.value)
 log(u.email,'telemetry.correct',f"{item_id}={data.value}");return row

@router.get('/precision')
def precision_state(u=Depends(current)):
 return precision.describe()

@router.post('/precision/start')
def start(u=Depends(admin)):
 row=precision.start();log(u.email,'precision.start',row['message']);return row

@router.post('/precision/backfill')
def backfill(data:BackfillInput,u=Depends(admin)):
 row=precision.backfill(data.batch_size,data.max_batches)
 log(u.email,'precision.backfill',f"cursor={row['cursor_id']} rows={row['backfilled_rows']}");return row

@router.post('/precision/reconcile')
def reconcile(u=Depends(admin)):
 row=precision.reconcile();log(u.email,'precision.reconcile',f"checked={row['reconcile_total']} mismatch={row['reconcile_mismatch']}");return row

@router.post('/precision/switch')
def switch(u=Depends(admin)):
 row=precision.switch();log(u.email,'precision.switch',row['message']);return row

@router.post('/precision/rollback')
def rollback(u=Depends(admin)):
 row=precision.rollback();log(u.email,'precision.rollback',row['message']);return row
