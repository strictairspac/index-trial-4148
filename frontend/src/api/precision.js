import {api,post} from './client.js';
export const precision={
 state:()=>api('/precision'),
 start:()=>post('/precision/start',{}),
 backfill:(body={})=>post('/precision/backfill',body),
 reconcile:()=>post('/precision/reconcile',{}),
 switch:()=>post('/precision/switch',{}),
 rollback:()=>post('/precision/rollback',{}),
};
export const telemetry={
 list:(sensorId)=>api('/telemetry'+(sensorId?`?sensor_id=${encodeURIComponent(sensorId)}`:'')),
 get:(id)=>api('/telemetry/'+id),
 ingest:(d)=>post('/telemetry',d),
 supplement:(d)=>post('/telemetry/supplement',d),
 correct:(id,value)=>api('/telemetry/'+id,{method:'PUT',body:JSON.stringify({value})}),
};
