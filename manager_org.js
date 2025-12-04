// PATCH: anti-cache fetch helpers (всегда бьём кэш и в браузере, и в trycloudflare)
const _ts = () => Date.now().toString();
const _withTs = (u) => u + (u.includes("?") ? "&" : "?") + "t=" + _ts();

async function jget(u){
  const r = await fetch(_withTs(u), { cache: "no-store", headers: { "Cache-Control": "no-cache" } });
  if(!r.ok) throw new Error(`HTTP ${r.status} GET ${u}`);
  return await r.json();
}
async function jpost(u, b){
  const r = await fetch(_withTs(u), {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json", "Cache-Control": "no-cache" },
    body: JSON.stringify(b)
  });
  if(!r.ok) throw new Error(`HTTP ${r.status} POST ${u}`);
  return await r.json();
}
async function jdel(u){
  const r = await fetch(_withTs(u), { method: "DELETE", cache: "no-store", headers: { "Cache-Control": "no-cache" } });
  if(!r.ok) throw new Error(`HTTP ${r.status} DELETE ${u}`);
  return await r.json();
}


function drawThreads(map){
  const tb = document.querySelector('#t-table tbody'); tb.innerHTML='';
  const rows = Object.entries(map||{}).sort((a,b)=>a[0].localeCompare(b[0]));
  for(const [fio,tid] of rows){
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${fio}</td><td>${tid}</td><td><button class="btn t-del" data-fio="${fio}">Удалить</button></td>`;
    tb.appendChild(tr);
  }
  tb.querySelectorAll('.t-del').forEach(btn=>{
    btn.onclick = async ()=>{
      if(!confirm('Удалить тред для '+btn.dataset.fio+'?')) return;
      const j = await jdel('/api/org/threads/'+encodeURIComponent(btn.dataset.fio));
      if(!j.ok) alert('Ошибка: '+(j.error||'unknown'));
      await loadAll();
      setTimeout(loadAll, 300);

    }
  });
}

function drawBrigades(map){
  const tb = document.querySelector('#b-table tbody'); tb.innerHTML='';
  const rows = Object.entries(map||{}).sort((a,b)=>a[0].localeCompare(b[0]));
  for(const [fio,br] of rows){
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${fio}</td><td>${br}</td><td><button class="btn b-del" data-fio="${fio}">Удалить</button></td>`;
    tb.appendChild(tr);
  }
  tb.querySelectorAll('.b-del').forEach(btn=>{
    btn.onclick = async ()=>{
      if(!confirm('Удалить бригадное назначение для '+btn.dataset.fio+'?')) return;
      const j = await jdel('/api/org/brigades/'+encodeURIComponent(btn.dataset.fio));
      if(!j.ok) alert('Ошибка: '+(j.error||'unknown'));
      await loadAll();
      setTimeout(loadAll, 300);
    }
  });
}

async function drawEmployees(list){
  const tb = document.querySelector('#e-table tbody'); if(!tb) return;
  tb.innerHTML='';
  (list||[]).sort((a,b)=>a.fio.localeCompare(b.fio)).forEach(it=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${it.fio}</td><td>${it.tg_user_id}</td>
      <td><button class="btn e-del" data-uid="${it.tg_user_id}">Удалить</button></td>`;
    tb.appendChild(tr);
  });
  tb.querySelectorAll('.e-del').forEach(btn=>{
    btn.onclick = async ()=>{
      if(!confirm('Удалить запись uid='+btn.dataset.uid+'?')) return;
      const j = await jdel('/api/org/employees/'+btn.dataset.uid);
      if(!j.ok) alert(j.error||'Ошибка');
      await loadAll();
    };
  });

}

async function loadAll(){
  const [threads, brigades, chat, employees] = await Promise.all([
    jget('/api/org/threads'),
    jget('/api/org/brigades'),
    jget('/api/org/group_chat_id'),
    jget('/api/org/employees')
  ]);
  drawThreads(threads);
  drawBrigades(brigades);
  const g = document.getElementById('g-id');
  if(g) g.value = (chat && chat.group_chat_id) ? chat.group_chat_id : '';
  drawEmployees(employees);
}

document.addEventListener('DOMContentLoaded', ()=>{
  const tSave = document.getElementById('t-save');
  if(tSave){
    tSave.onclick = async ()=>{
      const fio = document.getElementById('t-fio').value.trim();
      const tid = Number(document.getElementById('t-thread').value.trim());
      if(!fio || !tid){ alert('Укажите ФИО и числовой thread_id'); return; }
      const j = await jpost('/api/org/threads', {fio, thread_id: tid});
      if(!j.ok) alert('Ошибка: '+(j.error||'unknown'));
      await loadAll();
    };
  }

  const bSave = document.getElementById('b-save');
  if(bSave){
    bSave.onclick = async ()=>{
      const fio = document.getElementById('b-fio').value.trim();
      const name = document.getElementById('b-name').value.trim();
      if(!fio){ alert('Укажите ФИО'); return; }
      const j = await jpost('/api/org/brigades', {fio, name});
      if(!j.ok) alert('Ошибка: '+(j.error||'unknown'));
      await loadAll();
    };
  }

  const gSave = document.getElementById('g-save');
  if(gSave){
    gSave.onclick = async ()=>{
      const id = Number(document.getElementById('g-id').value.trim());
      const j = await jpost('/api/org/group_chat_id', {group_chat_id: id});
      if(!j.ok) alert('Ошибка: '+(j.error||'unknown'));
      await loadAll();
    };
  }

  const eSave = document.getElementById('e-save');
  if(eSave){
    eSave.onclick = async ()=>{
      const fio = document.getElementById('e-fio').value.trim();
      const uid = Number(document.getElementById('e-uid').value.trim());
      if(!fio || !uid){ alert('Укажите ФИО и числовой tg_user_id'); return; }
      const j = await jpost('/api/org/employees', { fio, tg_user_id: uid });
      if(!j.ok) alert(j.error || 'Ошибка');
      await loadAll();
    };
  }

  loadAll();
});
