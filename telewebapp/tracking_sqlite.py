import sqlite3, time, datetime, os
from typing import List, Dict, Any

DB_PATH = os.getenv("TRACK_DB_PATH", "live_tracking.db")
MAX_ACCURACY = float(os.getenv("MAX_ACCURACY", 200))
MAX_JUMP_SPEED = float(os.getenv("MAX_JUMP_SPEED", 150))
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", 30))

def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
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

def open_shift(employee_id: str):
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
    conn = _connect()
    shift_id = datetime.date.today().strftime("%Y%m%d") + "-" + employee_id
    ts = int(time.time())
    conn.execute("UPDATE shifts SET end_ts=?, active=0 WHERE shift_id=? AND employee_id=?", (ts, shift_id, employee_id))
    conn.commit()
    conn.close()

def insert_point(employee_id: str, tg_user_id: int, lat: float, lon: float, accuracy: float, source: str="live"):
    init_db()
    ts = int(time.time())
    shift_id = datetime.date.today().strftime("%Y%m%d") + "-" + employee_id
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT active FROM shifts WHERE shift_id=? AND employee_id=?", (shift_id, employee_id))
    row = cur.fetchone()
    if not row or row["active"] != 1:
        conn.close()
        return
    is_valid = 1
    if accuracy and accuracy > MAX_ACCURACY:
        is_valid = 0
    cur.execute("SELECT ts, lat, lon FROM live_points WHERE employee_id=? AND is_valid=1 ORDER BY ts DESC LIMIT 1", (employee_id,))
    last = cur.fetchone()
    speed_kmh = None
    if last:
        dt = ts - last["ts"]
        if dt > 0:
            dist = ((lat - last["lat"]) ** 2 + (lon - last["lon"]) ** 2) ** 0.5 * 111_000
            speed_kmh = (dist / dt) * 3.6
            if speed_kmh > MAX_JUMP_SPEED:
                is_valid = 0
    cur.execute(
        "INSERT INTO live_points(employee_id, tg_user_id, shift_id, ts, lat, lon, accuracy, source, speed_kmh, is_valid) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (employee_id, tg_user_id, shift_id, ts, lat, lon, accuracy, source, speed_kmh, is_valid),
    )
    if is_valid:
        cur.execute(
            "INSERT INTO last_point(employee_id, ts, lat, lon, accuracy, source) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(employee_id) DO UPDATE SET ts=excluded.ts, lat=excluded.lat, lon=excluded.lon, "
            "accuracy=excluded.accuracy, source=excluded.source WHERE excluded.ts >= last_point.ts",
            (employee_id, ts, lat, lon, accuracy, source),
        )
    conn.commit()
    conn.close()

def get_last_points() -> List[Dict[str, Any]]:
    init_db()
    conn = _connect()
    cur = conn.cursor()
    rows = cur.execute("SELECT * FROM last_point").fetchall()
    now = int(time.time())
    res = []
    for r in rows:
        age = now - r["ts"]
        if age <= 300:
            fresh = "green"
        elif age <= 1800:
            fresh = "yellow"
        else:
            fresh = "red"
        res.append(dict(employee_id=r["employee_id"], last_ts=r["ts"], last_lat=r["lat"], last_lon=r["lon"], last_accuracy=r["accuracy"], fresh_status=fresh))
    conn.close()
    return res

def get_track(employee_id: str, date: str) -> Dict[str, Any]:
    init_db()
    conn = _connect()
    start = int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    end = start + 86400
    rows = conn.execute("SELECT ts, lat, lon, accuracy FROM live_points WHERE employee_id=? AND is_valid=1 AND ts BETWEEN ? AND ? ORDER BY ts", (employee_id, start, end)).fetchall()
    conn.close()
    return {"employee_id": employee_id, "date": date, "points": [dict(ts=r["ts"], lat=r["lat"], lon=r["lon"], accuracy=r["accuracy"]) for r in rows]}

def cleanup_old():
    init_db()
    conn = _connect()
    cutoff = int(time.time()) - RETENTION_DAYS * 86400
    conn.execute("DELETE FROM live_points WHERE ts < ?", (cutoff,))
    conn.execute("DELETE FROM last_point WHERE ts < ?", (cutoff,))
    conn.commit()
    conn.close()
