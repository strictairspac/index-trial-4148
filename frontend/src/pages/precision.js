import {esc} from '../utils/html.js';import {table} from '../components/common/table.js';

const PHASES={legacy:'旧精度 FLOAT',backfilling:'回填中',backfilled:'待对账',reconciled:'已对账',switching:'切换中',switched:'六位小数 DECIMAL',rolling_back:'回切中'};
export function precisionPage(state,rows){
 if(!state)return '<div class="empty"><h2>无法读取精度状态</h2><p>请确认后端可用后刷新。</p></div>';
 const phase=PHASES[state.phase]||state.phase;
 const metric=(label,value)=>`<div><small>${esc(label)}</small><strong>${esc(value)}</strong></div>`;
 const canStart=state.phase==='legacy';
 const canBackfill=['backfilling','backfilled','reconciled'].includes(state.phase)&&state.pending_rows>0;
 const canReconcile=state.phase==='backfilled'||state.phase==='reconciled';
 const canSwitch=state.phase==='reconciled';
 const canRollback=state.phase==='switched';
 return `<div class="result-head"><div><span class="eyebrow">PRECISION / gb-4102 · v${esc(state.version)}</span><h2>读数精度升级 ${phase}</h2></div><button data-precision="rollback" ${canRollback?'':'disabled'}>切回旧精度</button></div>
 <div class="feedback ${state.phase==='switched'?'':'error'}">${esc(state.message||'')}</div>
 <div class="metrics">${metric('阶段',phase)}${metric('当前列类型',state.value_type||'—')}${metric('待回填行',state.pending_rows??0)}${metric('已回填行',state.backfilled_rows??0)}${metric('对账不一致',state.reconcile_mismatch??0)}${metric('回切损失行',state.lossy_rows??'—')}</div>
 <div class="evidence"><section><h3>升级步骤</h3>
  <p>历史读数按旧库读回值回填；新读数原样入库。回填可中断续跑，切换为原子操作，回切仅在零损失时允许。</p>
  <div class="row-actions"><button data-precision="start" ${canStart?'':'disabled'}>1. 启动回填</button><button data-precision="backfill" ${canBackfill?'':'disabled'}>2. 继续回填</button><button data-precision="reconcile" ${canReconcile?'':'disabled'}>3. 逐条对账</button><button data-precision="switch" ${canSwitch?'':'disabled'}>4. 原子切换</button></div>
  <h3>对账校验</h3><p>${state.reconcile_checksum?`<code class="checksum">${esc(state.reconcile_checksum)}</code>（${esc(state.reconcile_total??0)} 行）`:'尚未对账。'}</p>
 </section><section><h3>读数抽样（最新 10 条）</h3>
  ${table(['编号','读数（原样）','来源','版本'],(rows||[]).slice(0,10).map(r=>[r.id,`<code>${esc(r.value)}</code>`,esc(r.value_source||'—'),esc(r.value_version??'—')]))}
 </section></div>`;
}
