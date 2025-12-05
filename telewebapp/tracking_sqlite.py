# ================================================================
# tracking_sqlite.py
#
# Назначение модуля:
# ------------------
# 1. Хранить геоточки сотрудников (live_points) в локальной SQLite-базе.
# 2. Хранить информацию о сменах (shifts): когда смена открыта/закрыта.
# 3. Хранить последнюю "валидную" точку по каждому сотруднику (last_point),
#    чтобы быстро отрисовывать онлайн-карту.
# 4. Фильтровать "плохие" точки:
#    - по слишком плохой точности (accuracy)
#    - по нереалистичной скорости перемещения (прыжки по карте).
#
# Этот модуль вызывается из app.py:
#   - open_shift(...)  — открыть смену
#   - close_shift(...) — закрыть смену
#   - insert_point(...) — сохранить новую точку
#   - get_last_points() — отдать список последних позиций для онлайн-карты
#   - get_track(...)    — отдать трек сотрудника за день
#   - cleanup_old()     — подчистить старые записи
#
# База по умолчанию: live_tracking.db (в файле рядом с приложением).
# ================================================================

import sqlite3, time, datetime, os
from typing import List, Dict, Any

# ------------------------------------------------
# Конфигурация через переменные окружения
# ------------------------------------------------

# Путь до файла SQLite-БД.
# Можно переопределить через TRACK_DB_PATH, по умолчанию "live_tracking.db".
DB_PATH = os.getenv("TRACK_DB_PATH", "live_tracking.db")

# Максимально допустимая погрешность GPS (accuracy), в метрах.
# Если точность хуже (значение больше) — точка помечается как is_valid = 0.
MAX_ACCURACY = float(os.getenv("MAX_ACCURACY", 200))

# Максимально допустимая скорость "скачка" между точками, в км/ч.
# Если оценочная скорость > MAX_JUMP_SPEED → точка считается подозрительной и is_valid = 0.
MAX_JUMP_SPEED = float(os.getenv("MAX_JUMP_SPEED", 150))

# Сколько дней хранить историю в БД.
# Старые точки удаляются функцией cleanup_old() (retention).
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", 30))


# ------------------------------------------------
# БАЗОВАЯ РАБОТА С SQLite
# ------------------------------------------------

def _connect():
    """
    Создаёт подключение к SQLite-БД по пути DB_PATH.
    Включает row_factory = sqlite3.Row, чтобы строки
    можно было читать как словарь по имени колонки: row["lat"], row["ts"] и т.п.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """
    Инициализация структуры БД.

    Если таблиц ещё нет — создаёт их:
      • live_points — сырые и фильтрованные геоточки
      • shifts      — информация о сменах
      • last_point  — последняя валидная точка по каждому сотруднику

    Таблицы:

    live_points:
      - id          — автоинкремент, первичный ключ
      - employee_id — строковый идентификатор сотрудника (обычно ФИО)
      - tg_user_id  — Telegram user_id (числовой, для связи)
      - shift_id    — идентификатор смены (например, "20251205-Иванов Иван")
      - ts          — timestamp (UNIX-время в секундах)
      - lat, lon    — координаты
      - accuracy    — точность (метры), может быть NULL
      - source      — откуда точка: "webapp", "start", "live" и т.п.
      - speed_kmh   — оценочная скорость между предыдущей валидной точкой и текущей
      - is_valid    — 1 если точка прошла фильтры, 0 если отбракована

    shifts:
      - shift_id    — строковый ID смены (PRIMARY KEY)
      - employee_id — сотрудник
      - start_ts    — время открытия смены
      - end_ts      — время закрытия смены (может быть NULL)
      - active      — 1 если смена активна, 0 если закрыта

    last_point:
      - employee_id — сотрудник (PRIMARY KEY)
      - ts, lat, lon, accuracy, source — параметры самой свежей валидной точки

    Индексы:
      - idx_lp_emp_ts   — быстрый поиск последних точек по сотруднику
      - idx_lp_shift_ts — выборка трека смены
      - idx_lp_fresh    — свежие валидные точки по сотруднику
    """
    conn = _connect()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS live_points (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          employee_id TEXT NOT NULL,
          tg_user_id INTEGER NOT NULL,
          shift_id TEXT NOT NULL,
          ts INTEGER NOT NULL,
          lat REAL NOT NULL,
          lon REAL NOT NULL,
          accuracy REAL,
          source TEXT NOT NULL,
          speed_kmh REAL,
          is_valid INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_lp_emp_ts ON live_points(employee_id, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_lp_shift_ts ON live_points(shift_id, ts);
        CREATE INDEX IF NOT EXISTS idx_lp_fresh ON live_points(employee_id, is_valid, ts DESC);

        CREATE TABLE IF NOT EXISTS shifts (
          shift_id TEXT PRIMARY KEY,
          employee_id TEXT NOT NULL,
          start_ts INTEGER NOT NULL,
          end_ts INTEGER,
          active INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS last_point (
          employee_id TEXT PRIMARY KEY,
          ts INTEGER NOT NULL,
          lat REAL NOT NULL,
          lon REAL NOT NULL,
          accuracy REAL,
          source TEXT NOT NULL
        );
        """
    )
    conn.commit()
    conn.close()


# ------------------------------------------------
# УПРАВЛЕНИЕ СМЕНАМИ
# ------------------------------------------------

def open_shift(employee_id: str):
    """
    Открывает (или переоткрывает) смену для сотрудника.

    Логика:
      - Инициализируем БД (если ещё не создана).
      - Формируем shift_id вида "YYYYMMDD-employee_id" (дата + идентификатор сотрудника).
      - Записываем в таблицу shifts:
           shift_id, employee_id, start_ts = сейчас, active = 1
        INSERT OR REPLACE:
          - если смена на сегодняшнюю дату уже была — она переоткрывается с новым start_ts.
          - если не было — создаётся новая запись.

    Параметры:
      employee_id — строковый идентификатор сотрудника (чаще всего ФИО).
    """
    init_db()
    conn = _connect()
    ts = int(time.time())
    shift_id = datetime.date.today().strftime("%Y%m%d") + "-" + employee_id
    conn.execute(
        "INSERT OR REPLACE INTO shifts(shift_id, employee_id, start_ts, active) VALUES(?, ?, ?, 1)",
        (shift_id, employee_id, ts),
    )
    conn.commit()
    conn.close()


def close_shift(employee_id: str):
    """
    Закрывает смену сотрудника на текущий день.

    Логика:
      - Формируем shift_id по текущей дате и employee_id (так же, как в open_shift).
      - Обновляем запись в shifts:
          end_ts = сейчас,
          active = 0
      - Если смены не было — UPDATE просто не затронет ни одной строки.

    Параметры:
      employee_id — строковый идентификатор сотрудника (ФИО).
    """
    conn = _connect()
    shift_id = datetime.date.today().strftime("%Y%m%d") + "-" + employee_id
    ts = int(time.time())
    conn.execute(
        "UPDATE shifts SET end_ts=?, active=0 WHERE shift_id=? AND employee_id=?",
        (ts, shift_id, employee_id),
    )
    conn.commit()
    conn.close()


# ------------------------------------------------
# ЗАПИСЬ ТОЧКИ — insert_point(...)
# ------------------------------------------------

def insert_point(
    employee_id: str,
    tg_user_id: int,
    lat: float,
    lon: float,
    accuracy: float,
    source: str = "live",
):
    """
    Сохраняет новую геоточку сотрудника в БД и обновляет "последнюю" точку (last_point).

    Параметры:
      employee_id — идентификатор сотрудника (ФИО/строка)
      tg_user_id  — Telegram user_id (число)
      lat, lon    — координаты (широта и долгота)
      accuracy    — точность геопозиции (метры). Может быть 0 или None.
      source      — источник данных:
                       "live"     — обычный пинг,
                       "webapp"   — фоновые пинги из geo_watch.js,
                       "start"    — точка при начале смены и т.п.

    Важные шаги:
      1. init_db() — чтобы таблицы точно были созданы.
      2. Проверяем, есть ли активная смена для этого сотрудника:
           - Если смена не открыта → точка НЕ сохраняется (ничего не делаем).
      3. Фильтрация по точности:
           - Если accuracy > MAX_ACCURACY → is_valid = 0 (считаем точку "сомнительной").
      4. Фильтрация по "прыжкам":
           - Находим последнюю валидную точку сотрудника (is_valid = 1).
           - Считаем оценочную скорость km/h между предыдущей и новой:
               dist = геометрическое расстояние по широте/долготе в метрах (очень грубо)
               dt   = разница по времени в секундах
               speed_kmh = (dist / dt) * 3.6
           - Если speed_kmh > MAX_JUMP_SPEED → точку считаем невалидной (is_valid = 0).
      5. Вставляем строку в live_points.
      6. Если is_valid = 1 → обновляем last_point:
           - либо вставляем новую запись, либо обновляем существующую,
             но только если новая ts >= старой (по WHERE в ON CONFLICT).
    """
    init_db()
    ts = int(time.time())
    shift_id = datetime.date.today().strftime("%Y%m%d") + "-" + employee_id

    conn = _connect()
    cur = conn.cursor()

    # 1. Проверяем, есть ли активная смена
    cur.execute(
        "SELECT active FROM shifts WHERE shift_id=? AND employee_id=?",
        (shift_id, employee_id),
    )
    row = cur.fetchone()
    if not row or row["active"] != 1:
        # Если смена не активна — просто не пишем точку и выходим.
        conn.close()
        return

    # По умолчанию считаем точку валидной
    is_valid = 1

    # 2. Фильтрация по точности
    # Если accuracy не None/0 и > MAX_ACCURACY, помечаем точку как невалидную
    if accuracy and accuracy > MAX_ACCURACY:
        is_valid = 0

    # 3. Берём последнюю валидную точку (is_valid=1) по этому сотруднику
    cur.execute(
        "SELECT ts, lat, lon FROM live_points WHERE employee_id=? AND is_valid=1 ORDER BY ts DESC LIMIT 1",
        (employee_id,),
    )
    last = cur.fetchone()

    speed_kmh = None  # оценочная скорость между last и текущей точкой
    if last:
        dt = ts - last["ts"]        # разница по времени (сек)
        if dt > 0:
            # Грубое расстояние в метрах (лат/лон в градусах, 111 000 ~ метров на градус)
            dist = ((lat - last["lat"]) ** 2 + (lon - last["lon"]) ** 2) ** 0.5 * 111_000
            speed_kmh = (dist / dt) * 3.6
            # Если скорость > MAX_JUMP_SPEED — считаем это "телепортом" и отбраковываем точку
            if speed_kmh > MAX_JUMP_SPEED:
                is_valid = 0

    # 4. Вставляем запись в live_points
    cur.execute(
        """
        INSERT INTO live_points(
          employee_id, tg_user_id, shift_id, ts, lat, lon, accuracy,
          source, speed_kmh, is_valid
        ) VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (employee_id, tg_user_id, shift_id, ts, lat, lon, accuracy, source, speed_kmh, is_valid),
    )

    # 5. Если точка признана валидной — обновляем last_point
    if is_valid:
        # ON CONFLICT(employee_id) DO UPDATE:
        #   - если для сотрудника уже была запись в last_point,
        #     обновляем её только если новая ts >= старой ts.
        cur.execute(
            """
            INSERT INTO last_point(employee_id, ts, lat, lon, accuracy, source)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(employee_id) DO UPDATE SET
              ts=excluded.ts,
              lat=excluded.lat,
              lon=excluded.lon,
              accuracy=excluded.accuracy,
              source=excluded.source
            WHERE excluded.ts >= last_point.ts
            """,
            (employee_id, ts, lat, lon, accuracy, source),
        )

    conn.commit()
    conn.close()


# ------------------------------------------------
# ПОЛУЧИТЬ СПИСОК ПОСЛЕДНИХ ТОЧЕК — get_last_points()
# ------------------------------------------------

def get_last_points() -> List[Dict[str, Any]]:
    """
    Возвращает список последних точек по всем сотрудникам из таблицы last_point.

    Формат результата (список словарей):
      [
        {
          "employee_id": "Иванов Иван Иванович",
          "last_ts":  1700000000,    # timestamp
          "last_lat": 55.12345,
          "last_lon": 37.12345,
          "last_accuracy": 12.0,
          "fresh_status": "green" | "yellow" | "red"
        },
        ...
      ]

    fresh_status — цвет "свежести":
      - "green"  — точка моложе  5 минут   (age <= 300 сек)
      - "yellow" — точка моложе 30 минут   (age <= 1800 сек)
      - "red"    — точка старше  30 минут  (age  > 1800 сек)

    Использование:
      - эндпоинт /api/online/employees в app.py → отдаёт это фронтенду (online.js)
      - online.js раскрашивает сотрудников в интерфейсе по цветам.
    """
    init_db()
    conn = _connect()
    cur = conn.cursor()

    # Забираем все записи из last_point
    rows = cur.execute("SELECT * FROM last_point").fetchall()
    now = int(time.time())
    res: List[Dict[str, Any]] = []

    for r in rows:
        age = now - r["ts"]  # возраст точки, секунды

        # Определяем цвет "свежести"
        if age <= 300:
            fresh = "green"   # совсем свежая
        elif age <= 1800:
            fresh = "yellow"  # не совсем свежая, но ещё ок
        else:
            fresh = "red"     # устаревшая точка

        res.append(
            dict(
                employee_id=r["employee_id"],
                last_ts=r["ts"],
                last_lat=r["lat"],
                last_lon=r["lon"],
                last_accuracy=r["accuracy"],
                fresh_status=fresh,
            )
        )

    conn.close()
    return res


# ------------------------------------------------
# ПОЛУЧИТЬ ТРЕК ЗА ДЕНЬ — get_track(...)
# ------------------------------------------------

def get_track(employee_id: str, date: str) -> Dict[str, Any]:
    """
    Возвращает трек (последовательность точек) сотрудника за конкретную дату.

    Параметры:
      employee_id — строковый идентификатор сотрудника (ФИО/ID).
      date        — строка в формате "YYYY-MM-DD" (например, "2025-12-05").

    Логика:
      - Считаем начало дня:  date 00:00:00 → start (timestamp)
      - Считаем конец дня:   start + 86400 секунд → end
      - Выбираем из live_points все валидные точки (is_valid=1)
        по employee_id в диапазоне [start, end), упорядоченные по ts.

    Формат ответа:
      {
        "employee_id": "...",
        "date": "YYYY-MM-DD",
        "points": [
          {"ts": ..., "lat": ..., "lon": ..., "accuracy": ...},
          ...
        ]
      }

    Использование:
      - эндпоинт /api/live/track в app.py
      - фронт online.js запрашивает этот JSON и рисует полилинию (трек) на карте.
    """
    init_db()
    conn = _connect()

    # Парсим дату "YYYY-MM-DD" в datetime и получаем timestamp начала дня
    start = int(
        time.mktime(
            datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()
        )
    )
    end = start + 86400  # конец суток (не включая)

    rows = conn.execute(
        """
        SELECT ts, lat, lon, accuracy
        FROM live_points
        WHERE employee_id=? AND is_valid=1 AND ts BETWEEN ? AND ?
        ORDER BY ts
        """,
        (employee_id, start, end),
    ).fetchall()

    conn.close()

    return {
        "employee_id": employee_id,
        "date": date,
        "points": [
            dict(ts=r["ts"], lat=r["lat"], lon=r["lon"], accuracy=r["accuracy"])
            for r in rows
        ],
    }


# ------------------------------------------------
# ОЧИСТКА СТАРЫХ ДАННЫХ — cleanup_old()
# ------------------------------------------------

def cleanup_old():
    """
    Удаляет устаревшие точки из БД по RETENTION_DAYS.

    Логика:
      - cutoff = сейчас - RETENTION_DAYS * 86400 секунд
      - из live_points удаляются все строки, где ts < cutoff
      - из last_point удаляются все строки, где ts < cutoff

    Зачем это нужно:
      - чтобы база не разрасталась бесконечно из-за старых треков.
      - актуальные данные по сотрудникам обычно нужны за последний месяц.

    Использование:
      - может вызываться вручную (скрипт/cron),
      - можно привязать к периодическому запуску в фоновом задачнике.
    """
    init_db()
    conn = _connect()
    cutoff = int(time.time()) - RETENTION_DAYS * 86400

    conn.execute("DELETE FROM live_points WHERE ts < ?", (cutoff,))
    conn.execute("DELETE FROM last_point WHERE ts < ?", (cutoff,))

    conn.commit()
    conn.close()
