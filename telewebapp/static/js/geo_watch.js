// Псевдо-лайв трекинг пока страница открыта
(function(){
  let watchId = null;
  let lastSent = 0;

  async function send(lat, lon, acc) {
    try {
      const form = new FormData();
      form.append("lat", String(lat));
      form.append("lon", String(lon));
      if (typeof acc === "number") form.append("acc", String(acc));
      // ts не обязателен; сервер поставит свой
      await fetch("/api/geo/ping", { method: "POST", body: form, credentials: "same-origin" });
    } catch (e) {
      // тихо игнорируем: сеть может пропасть
      // console.warn("[geo ping]", e);
    }
  }

  function startWatch() {
    if (!navigator.geolocation || watchId !== null) return;
    watchId = navigator.geolocation.watchPosition(
      (pos) => {
        const coords = pos.coords || {};
        const now = Date.now();
        // не чаще раза в 10 сек, чтобы не грузить сеть/сервер
        if (now - lastSent < 10_000) return;
        lastSent = now;
        send(coords.latitude, coords.longitude, coords.accuracy);
      },
      (err) => {
        // пользователь мог запретить гео на уровне браузера
        // console.warn("[geo error]", err);
      },
      {
        enableHighAccuracy: true,
        maximumAge: 5_000,
        timeout: 15_000
      }
    );
  }

  function stopWatch() {
    if (watchId !== null) {
      navigator.geolocation.clearWatch(watchId);
      watchId = null;
    }
  }

  // Экспорт в глобал для явного вызова из шаблона
  window.__geoWatch = { startWatch, stopWatch };
})();
