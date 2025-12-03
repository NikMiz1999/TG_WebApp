let map, markers = {}, polyline;
async function loadEmployees(){
  const res = await fetch('/api/online/employees');
  const data = await res.json();
  const list = document.getElementById('list');
  list.innerHTML = '';
  data.forEach(emp=>{
    const div = document.createElement('div');
    div.className = 'emp ' + emp.fresh_status;
    div.textContent = emp.employee_id + ' (' + emp.fresh_status + ')';
    div.onclick = ()=> showTrack(emp.employee_id);
    list.appendChild(div);
  });
}
async function showTrack(employee_id){
  const today = new Date().toISOString().slice(0,10);
  const res = await fetch('/api/online/track?employee_id='+employee_id+'&date='+today);
  const data = await res.json();
  if(polyline) map.removeLayer(polyline);
  if(markers[employee_id]) map.removeLayer(markers[employee_id]);
  if(data.points.length===0) return;
  const latlngs = data.points.map(p=>[p.lat,p.lon]);
  polyline = L.polyline(latlngs, {color:'blue'}).addTo(map);
  const last = latlngs[latlngs.length-1];
  markers[employee_id] = L.marker(last).addTo(map).bindPopup(employee_id);
  map.fitBounds(polyline.getBounds());
}
window.onload = ()=>{
  map = L.map('map').setView([55.75,37.61], 10);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {maxZoom:19}).addTo(map);
  loadEmployees();
  setInterval(loadEmployees,10000);
}
