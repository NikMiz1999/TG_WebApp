async function fetchEmployees(){
  const r = await fetch('/api/online/employees');
  if(!r.ok) return [];
  return await r.json();
}
function renderList(items){
  const wrap = document.getElementById('employees');
  wrap.innerHTML = '';
  if(!items.length){
    const div = document.createElement('div');
    div.className = 'empty';
    div.textContent = 'Нет данных по персональным отметкам за сегодня.';
    wrap.appendChild(div);
    return;
  }
  for(const e of items){
    const card = document.createElement('div');
    card.className = 'emp ' + e.fresh_status;
    const title = document.createElement('div');
    title.className = 'fio';
    title.textContent = e.employee_id;
    const meta = document.createElement('div');
    const dt = new Date(e.last_ts * 1000);
    meta.className = 'meta';
    meta.textContent = `Последняя точка: ${dt.toLocaleTimeString()} · ${e.last_lat.toFixed(5)}, ${e.last_lon.toFixed(5)} · ±${Math.round(e.last_accuracy||0)}м`;
    const openOnMap = document.createElement('a');
    openOnMap.href = `/online`;
    openOnMap.target = '_blank';
    openOnMap.textContent = 'Открыть карту';
    const actions = document.createElement('div');
    actions.className = 'actions';
    actions.appendChild(openOnMap);
    card.appendChild(title);
    card.appendChild(meta);
    card.appendChild(actions);
    wrap.appendChild(card);
  }
}
function applyFilters(items){
  const status = document.getElementById('status-filter').value;
  const q = (document.getElementById('search').value || '').trim().toLowerCase();
  return items.filter(e => {
    const okStatus = (status === 'all') || (e.fresh_status === status);
    const okSearch = !q || (e.employee_id.toLowerCase().includes(q));
    return okStatus && okSearch;
  });
}
async function refresh(){
  try{
    const data = await fetchEmployees();
    renderList(applyFilters(data));
  }catch(e){
    console.error(e);
    const wrap = document.getElementById('employees');
    wrap.innerHTML = '<div class="error">Ошибка загрузки /api/online/employees</div>';
  }
}
document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('refresh').addEventListener('click', refresh);
  document.getElementById('status-filter').addEventListener('change', refresh);
  document.getElementById('search').addEventListener('input', ()=>{ refresh(); });
  refresh();
  setInterval(refresh, 15000);
});