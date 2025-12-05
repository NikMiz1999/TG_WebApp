import os, io, re, requests, json
import asyncio
import hmac, hashlib

import datetime
from zoneinfo import ZoneInfo

# Часовой пояс по умолчанию — Москва (можно переопределить через переменную окружения TZ)
MSK = ZoneInfo(os.getenv("TZ", "Europe/Moscow"))

def today_local_iso() -> str:
    """
    Возвращает сегодняшнюю дату в локальном (московском) часовом поясе в формате YYYY-MM-DD.
    Используется для идемпотентности /not_return (чтобы не дублировать уведомления в один день).
    """
    return datetime.datetime.now(MSK).date().isoformat()


from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from zoneinfo import ZoneInfo
from datetime import timezone, timedelta
from typing import List, Optional
from fastapi import HTTPException, Request
from typing import Optional

# Импорт функций для работы с локальной SQLite-БД геотрекинга
from tracking_sqlite import open_shift, close_shift, insert_point, get_last_points, get_track
from starlette.middleware.base import BaseHTTPMiddleware

def _to_float(v: Optional[str]) -> Optional[float]:
    """
    Аккуратное приведение строки к float.
    - Пустые строки и None -> None
    - Замена запятой на точку, чтобы поддерживать '55,123'.
    Используется для парсинга координат и точности геолокации.
    """
    if v is None:
        return None
    v = v.strip()
    if v == "":
        return None
    try:
        return float(v.replace(",", "."))
    except ValueError:
        return None


import gspread
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials

# Базовые данные по сотрудникам и месяцам из emp_map.py
from emp_map import EMPLOYEE_THREADS, BRIGADES, RU_MONTHS, GROUP_CHAT_ID

# JSON-конфиг поверх emp_map — только переопределения (org_store.py работает с org.json)
from org_store import (
    threads_map, brigades_map, get_group_chat_id, set_group_chat_id,
    set_thread, delete_thread, set_brigade, delete_brigade_mapping,
    as_ids_map,   # ← карта uid -> ФИО из org.json
)


# ======================
#  ИНИЦИАЛИЗАЦИЯ ОРГСТРУКТУРЫ В ПАМЯТИ
# ======================

# JSON поверх дефолтов из emp_map.py
EMPLOYEE_THREADS = threads_map(default=EMPLOYEE_THREADS)  # {fio -> thread_id} — в какой тред Телеграма писать
BRIGADES         = brigades_map(default=BRIGADES)         # {fio -> brigade_name} — членство в бригаде
GROUP_CHAT_ID    = get_group_chat_id(default=GROUP_CHAT_ID)

# Для авто-аутентификации по Telegram user_id (из org.json)
USER_ID_TO_FIO: dict[int, str] = as_ids_map()             # {tg_user_id -> fio}
FIO_TO_USER_ID: dict[str, int] = {fio: uid for uid, fio in USER_ID_TO_FIO.items()}  # обратная карта


# ======================
#  ИНИЦИАЛИЗАЦИЯ FASTAPI И СЕССИЙ
# ======================

app = FastAPI()

# Middleware для cookie-сессий.
# Здесь мы храним uid и fio сотрудника после авторизации через WebApp/бота.
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "devsecret"),
    same_site="lax",
    max_age=60*60*24*30,      # срок жизни сессии — 30 дней
    session_cookie="tw_sess_v3",  # НОВОЕ имя cookie → старые сессии перестанут применяться
)

@app.middleware("http")
async def nocache_org_endpoints(request, call_next):
    """
    Middleware: запрещаем кеширование ответов для /api/org/*,
    чтобы UI админки оргструктуры всегда видел свежие данные.
    """
    resp = await call_next(request)
    p = request.url.path
    if p.startswith("/api/org/"):
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp

@app.middleware("http")
async def purge_orphan_session(request, call_next):
    """
    Middleware: чистим "битые" сессии.
    Случай: uid лежит в сессии, но пользователь уже удалён из org.json/emp_map → сбрасываем сессию.
    Плюс: пробуем авторизоваться по параметрам ?uid=...&sig=... (fallback для WebApp-ссылки от бота).
    """
    # Если SessionMiddleware ещё не обернул запрос – пропускаем
    if "session" not in request.scope:
        return await call_next(request)

    # 1) Чистка устаревшей сессии по uid
    try:
        uid_raw = request.session.get("uid")
        uid = int(uid_raw) if uid_raw is not None else 0
        ids_map_now = as_ids_map()              # читаем всегда актуальную карту uid->fio
        if uid == 0 or uid not in ids_map_now:  # не существует → сбрасываем
            request.session.pop("uid", None)
            request.session.pop("fio", None)
    except Exception:
        # Любые сбои не должны ломать обработку запроса
        pass

    # 2) Fallback-логин из URL: ?uid=...&sig=HMAC_SHA256(BOT_TOKEN, str(uid))
    # Это сценарий, когда пользователь приходит по ссылке из Telegram WebApp.
    try:
        params = request.query_params
        uid_q = params.get("uid")
        sig_q = params.get("sig")
        bot_token = os.getenv("BOT_TOKEN", "")

        if uid_q and sig_q and bot_token:
            payload = str(int(uid_q))  # нормализуем uid
            good_sig = hmac.new(
                bot_token.encode("utf-8"),
                payload.encode("utf-8"),
                hashlib.sha256
            ).hexdigest()

            # Подпись валидна → авторизуем ТОЛЬКО если uid реально есть в org.json
            if hmac.compare_digest(sig_q, good_sig):
                ids_map_fresh = as_ids_map()
                uid_int = int(uid_q)
                if uid_int in ids_map_fresh:
                    request.session["uid"] = uid_int
                    request.session["fio"] = ids_map_fresh[uid_int]
                # если uid отсутствует — ничего не делаем (остаётся denied/без авторизации)
    except Exception:
        # Любые ошибки тут не должны ложить сервер
        pass

    return await call_next(request)



# ======================
#  ГЛОБАЛЬНЫЕ НАСТРОЙКИ И КОНСТАНТЫ
# ======================

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
# ID группового чата (в котором живут треды бригад) — либо из env, либо из org.json/emp_map
GROUP_ID = int(os.getenv("GROUP_CHAT_ID", str(GROUP_CHAT_ID)))
# ID таблицы Google Sheets с табелем (по умолчанию — конкретный ключ)
TIMESHEET_ID = os.getenv("TIMESHEET_ID", "1J212D9-n0eS5DnEST7JqObeE2S1umHCSRURjhntq4R8")

# уведомления админу — только в ЛС
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "0"))

# Цвет для подсветки вручную отредактированных ячеек (табель) — мягкий красный
MANUAL_RED = (1.0, 0.80, 0.80)

def current_user(request: Request) -> tuple[int, str]:
    """
    Достаёт текущего пользователя из сессии:
    - читает uid из session["uid"]
    - проверяет, что uid есть в актуальной карте as_ids_map() (org.json)
    - нормализует fio в сессии (на случай переименования)
    Если что-то не так — выбрасывает HTTPException, а вызывающий обработчик делает Redirect.
    """
    uid_raw = request.session.get("uid")
    if uid_raw is None:
        raise HTTPException(status_code=401, detail="Нет Telegram-сессии — войдите через кнопку в боте")
    try:
        uid = int(uid_raw)
    except Exception:
        raise HTTPException(status_code=401, detail="Некорректный uid в сессии")

    # ✅ всегда берём актуальную карту из org.json
    ids_map_now = as_ids_map()
    fio = ids_map_now.get(uid)
    if not fio:
        # uid есть в сессии, но уже удалён/неизвестен в org.json
        raise HTTPException(status_code=403, detail="Доступ запрещён: пользователь отсутствует в списке")

    # Нормализуем fio в сессии (на случай переименований)
    request.session["fio"] = fio
    return uid, fio


def color_cell_a1(a1: str, r: float, g: float, b: float):
    """
    Окраска ячейки Google Sheets в формате A1 (например, "C5") в конкретный цвет RGB.
    Используется для визуальной подсветки статусов (ручная правка, больничный и т.п.).
    """
    sheet.format(a1, {"backgroundColor": {"red": r, "green": g, "blue": b}})

def mark_manual_red(a1: str):
    """
    Подсветка ячейки "ручной правки" (когда руководитель руками меняет табель через /adjust).
    """
    color_cell_a1(a1, *MANUAL_RED)

def get_msk():
    """
    Возврат объект timezone для Москвы.
    Если zoneinfo не сработал — возвращаем timedelta +3 часа вручную.
    """
    try:
        return ZoneInfo("Europe/Moscow")
    except Exception:
        return timezone(timedelta(hours=3))

# Авторизация к Google Sheets через сервисный аккаунт
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(TIMESHEET_ID).sheet1   # здесь мы работаем с первым листом табеля




# Подключение статики и шаблонов Jinja2
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# >>> ADDED: страница отказа + общий гард авторизации
from fastapi.responses import RedirectResponse

@app.api_route("/denied", methods=["GET","HEAD"])
def denied(request: Request):
    """
    Простая страница "Вас не зарегистрировали / доступ запрещён".
    Используется, когда:
      - uid есть, но нет в org.json
      - подпись sig невалидна
      - нет сессии Telegram.
    """
    return templates.TemplateResponse("denied.html", {"request": request})

def require_auth(request: Request):
    """
    Гард для персональных экранов (страницы, которые нельзя открыть без Telegram-авторизации).
    Проверяет:
    - есть ли uid в сессии
    - присутствует ли uid в актуальной карте as_ids_map()
    При проблеме возвращает RedirectResponse('/denied'), иначе None.
    """
    try:
        uid_raw = request.session.get("uid")
        uid = int(uid_raw) if uid_raw is not None else 0

        # ✅ всегда проверяем против свежей карты
        ids_map_now = as_ids_map()
        if uid == 0 or uid not in ids_map_now:
            return RedirectResponse(url="/denied", status_code=302)

        # если fio в сессии нет/устарело — нормализуем
        if not request.session.get("fio"):
            request.session["fio"] = ids_map_now[uid]
    except Exception:
        return RedirectResponse(url="/denied", status_code=302)

    return None

# <<< ADDED


@app.get("/health", response_class=HTMLResponse)
def health():
    """
    Простой health-check эндпоинт. Используется для мониторинга, проверки "жив ли сервер".
    """
    return "ok"


# ======================
#  ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ РАБОТЫ С ТАБЕЛЕМ
# ======================

def get_employee_names():
    """
    Возвращает список ФИО из первой колонки Google Sheets,
    отфильтрованный по шаблону "Фамилия Имя Отчество".
    """
    colA = sheet.col_values(1)
    pat = re.compile(r"^([А-ЯЁ][а-яё]+ ){2}[А-ЯЁ][а-яё]+.*$")
    return [v for v in colA if pat.match(v)]

def find_row_by_fio(fio: str) -> int:
    """
    Находит номер строки в таблице по ФИО (в колонке A).
    Если не найдено — выбрасывает ValueError.
    """
    colA = sheet.col_values(1)
    for idx, cell in enumerate(colA, start=1):
        if cell == fio:
            return idx
    raise ValueError(f"ФИО «{fio}» не найдено в колонке A")

def find_col_by_date(dt: datetime.date) -> int:
    """
    Находит номер колонки для конкретной даты (dt) в табеле.
    Логика:
      - ищем строку с названием месяца
      - следующая строка — числа дней
      - от этого блока находим нужный столбец для dt.day
    Если не найдено — выбрасываем ValueError.
    """
    month_row = day_row = None
    target = RU_MONTHS[dt.month - 1]  # название месяца в родительном падеже, как в шапке листа
    # ищем строку, где в ряду есть название месяца
    for r in range(1, 7):
        vals = sheet.row_values(r)
        cnt = sum(1 for x in vals if x and x.strip().lower().startswith(target))
        if cnt >= 1:
            month_row = r
            day_row = r + 1
            break
    if month_row is None:
        raise ValueError("Не найдена строка с месяцами")

    months = sheet.row_values(month_row)
    days = sheet.row_values(day_row)
    max_len = max(len(months), len(days))
    months += [""] * (max_len - len(months))
    days += [""] * (max_len - len(days))

    def month_at(j: int) -> str:
        """
        Возвращает название месяца слева от позиции j (включая её).
        Нужно, потому что в шапке месяц обычно написан один раз.
        """
        for i in range(j, -1, -1):
            m = (months[i] or "").strip().lower()
            if m:
                return m
        return ""

    # находим позиции всех "1" (первых чисел) — начало месяцев
    one_positions = [idx for idx, d in enumerate(days) if re.fullmatch(r"1", str(d).strip())]
    start = None
    for idx in one_positions:
        if month_at(idx) == target:
            start = idx
            break
    if start is None:
        raise ValueError(f"Не найдено начало месяца {target}")

    # определяем конец текущего месяца (до следующей "1")
    end = len(days)
    for j in range(start + 1, len(days)):
        if re.fullmatch(r"1", str(days[j]).strip()):
            end = j
            break

    # ищем номер столбца, где день = dt.day и месяц тот же
    for j in range(start, end):
        if month_at(j) != target:
            continue
        m = re.search(r"(\d+)", str(days[j]))
        if m and int(m.group(1)) == dt.day:
            return j + 1  # индексация столбцов с 1

    raise ValueError(f"Столбец для {dt.isoformat()} не найден")

def is_hhmm(val: str) -> bool:
    """
    Проверка, выглядит ли строка как время в формате HH:MM.
    """
    return bool(re.fullmatch(r"\d{1,2}:\d{2}", (val or "").strip()))

def send_photo_to_thread(file_bytes: bytes, thread_id: int, caption: str):
    """
    Отправка фото с подписью в конкретный тред Telegram.
    Используется для отметок смен: фото + подпись + гео.
    """
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    files = {"photo": ("photo.jpg", file_bytes)}
    data = {"chat_id": GROUP_ID, "message_thread_id": thread_id, "caption": caption}
    r = requests.post(url, data=data, files=files, timeout=25)
    r.raise_for_status()

def send_message(chat_id: int, text: str):
    """
    Отправка обычного текстового сообщения в чат/ЛС Telegram.
    Используется для уведомлений админу.
    """
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": chat_id, "text": text}
    r = requests.post(url, data=data, timeout=15)
    r.raise_for_status()

def send_message_to_thread(thread_id: int, text: str):
    """
    Отправка текстового сообщения в конкретный тред (ветку) в групповом чате.
    Используется для уведомлений бригадира.
    """
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": GROUP_ID, "message_thread_id": thread_id, "text": text}
    r = requests.post(url, data=data, timeout=15)
    r.raise_for_status()

def notify_admin(text: str):
    """
    Удобная обёртка для уведомлений администратора в ЛС.
    Если ADMIN_CHAT_ID не задан — просто логгируем в stdout.
    """
    if not ADMIN_CHAT_ID:
        print("[notify_admin] skipped: ADMIN_CHAT_ID is not set")
        return
    try:
        send_message(ADMIN_CHAT_ID, text)
    except Exception as e:
        print(f"[notify_admin] failed: {e}")


def reload_org_in_memory():
    """
    Перечитать org.json и пересобрать все мапы в памяти.
    Вызывается после операций /api/org/*, чтобы изменения сразу применялись без перезапуска приложения.
    """
    global EMPLOYEE_THREADS, BRIGADES, GROUP_CHAT_ID, USER_ID_TO_FIO, FIO_TO_USER_ID
    # читаем актуальные данные из org.json поверх дефолтов из emp_map.py
    EMPLOYEE_THREADS = threads_map(default=EMPLOYEE_THREADS)
    BRIGADES         = brigades_map(default=BRIGADES)
    GROUP_CHAT_ID    = get_group_chat_id(default=GROUP_CHAT_ID)
    USER_ID_TO_FIO   = as_ids_map()
    FIO_TO_USER_ID   = {fio: uid for uid, fio in USER_ID_TO_FIO.items()}






# ======================
#  АВТОРИЗАЦИЯ И ВЫХОД
# ======================

@app.get("/logout")
def logout(request: Request):
    """
    Полный выход из системы:
    - очищает сессию
    - перекидывает на корень "/"
    """
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)


# ============================================================
#  /check  — ОДИНОЧНАЯ ОТМЕТКА СОТРУДНИКА (с обязательной геолокацией)
# ============================================================

# ====== ОБЯЗАТЕЛЬНАЯ ГЕОЛОКАЦИЯ для start/end (принимаем поля и валидируем) ======
@app.post("/check", response_class=HTMLResponse)
async def check(
    request: Request,
    action: str = Form(...),           # тип действия: "start", "end", "left", "sick"
    photo: UploadFile = File(...),     # фото, обязательное для отметки
    lat: str | None = Form(default=None),   # широта (строка, потом приводим к float)
    lon: str | None = Form(default=None),   # долгота
    acc: str | None = Form(default=None),   # точность гео (метры)
    geo_ts: Optional[str] = Form(default=None),  # timestamp гео (необязательно)
    dates_confirmed: str | None = Form(default=None),  # флаг подтверждения дат для "left"
    ret_date: str | None = Form(default=None),         # дата возвращения (для "left")
    dep_date: str | None = Form(default=None),         # дата следующего отъезда (для "left")
    not_return: str | None = Form(default="0"),        # "1" если "не приеду" в модальном окне
):
    """
    Основной обработчик одиночной отметки смены.
    Сюда прилетает форма с check.html:
    - action (тип события)
    - фото
    - геопозиция
    - даты (для сценария "уехал")
    """
    # Проверяем, что пользователь авторизован через Telegram (есть валидная сессия)
    try:
        uid, fio = current_user(request)
    except HTTPException:
        # Если нет — отправляем на корень (там либо /denied, либо авторизация)
        return RedirectResponse(url="/", status_code=302)

    if not fio:
        return RedirectResponse(url="/", status_code=302)

    # Для action="left" (уехал) обязательно должны быть либо подтверждённые даты,
    # либо отмечен флаг "не приеду"
    if action == "left" and dates_confirmed != "1":
        return templates.TemplateResponse(
            "check.html",
            {
                "request": request,
                "fio": fio,
                "message": "Сначала укажите даты или нажмите «Не приеду».",
                "error": True,
                "show_modal": True
            },
            status_code=400,
        )

    # На всякий случай ещё раз берём fio из сессии
    fio = request.session.get("fio")
    if not fio:
        return RedirectResponse(url="/", status_code=302)

    # Ветка/тред для данного сотрудника (куда отправлять отметку)
    thread_id = EMPLOYEE_THREADS.get(fio)

    # приведение гео к float (пустые строки -> None)
    lat_f = _to_float(lat)
    lon_f = _to_float(lon)
    acc_f = _to_float(acc)

    if not thread_id:
        # Критическая ситуация: нет привязки сотрудника к треду
        return templates.TemplateResponse(
            "check.html",
            {
                "request": request,
                "fio": fio,
                "message": "❌ Для выбранного ФИО не найдена ветка в Telegram.",
                "error": True,
                "show_modal": False
            },
        )

    # Серверная проверка: для start/end геолокация обязательна
    if action in ("start", "end") and (lat_f is None or lon_f is None):
        # Флаг, который читает фронт (включить geo_watch)
        request.session["geo_watch_enable"] = True

        return templates.TemplateResponse(
            "check.html",
            {
                "request": request,
                "fio": fio,
                "message": "❌ Для начала/конца дня требуется геолокация. Разрешите доступ и повторите.",
                "error": True,
                "show_modal": False
            },
        )

    # Читаем байты изображения из UploadFile
    img_bytes = await photo.read()
    msk = get_msk()
    now_local = datetime.datetime.now(msk)   # локальное (московское) время сейчас
    today = now_local.date()
    date_str = today.isoformat()
    show_modal = False

    # Локальные вспомогательные функции, завязанные на today/now_local

    def is_hhmm_local(s: str) -> bool:
        """
        Локальный формат времени: HH:MM.
        Используется для проверки, что в ячейке табеля лежит старт смены.
        """
        return bool(re.fullmatch(r"\d{1,2}:\d{2}", (s or "").strip()))

    def compute_rounded_hours(start_hhmm: str) -> int:
        """
        Рассчитать целое количество часов между стартом и текущим временем
        с округлением:
          - если остаток минут > 20 → +1 час.
        Используется в старых версиях логики, здесь лежит как helper.
        """
        sh, sm = map(int, start_hhmm.split(":"))
        start_dt = datetime.datetime.combine(today, datetime.time(sh, sm))
        end_dt = now_local.replace(tzinfo=None)
        if end_dt < start_dt:
            # если конец "перекатился" через полночь — добавляем день
            end_dt += datetime.timedelta(days=1)
        delta = end_dt - start_dt
        hrs = delta.seconds // 3600
        mins = (delta.seconds % 3600) // 60
        return hrs + (1 if mins > 20 else 0)

    # получаем ячейку на сегодня для данного ФИО
    try:
        row = find_row_by_fio(fio)
        col = find_col_by_date(today)
        cell_val = (sheet.cell(row, col).value or "").strip()
    except Exception as e:
        # Любая ошибка с табелем → показываем человеку понятное сообщение
        return templates.TemplateResponse(
            "check.html",
            {
                "request": request,
                "fio": fio,
                "message": f"❌ {e}",
                "error": True,
                "show_modal": False
            },
        )

    caption = ""  # подпись к фото / текст для Telegram

    try:
        # ======== СЦЕНАРИЙ "НАЧАЛ СМЕНУ" ========
        if action == "start":
            # Если в ячейке уже что-то есть — не даём начать смену ещё раз
            if cell_val != "":
                raise RuntimeError("Нельзя начать: на сегодня уже есть запись.")
            time_str = now_local.strftime("%H:%M")
            sheet.update_cell(row, col, time_str)
            caption = f"📸 {fio} начал рабочий день: {time_str} ({date_str})"
            # Включаем режим постоянного геотрекинга
            request.session["geo_watch_enable"] = True

            # Открываем смену в локальной БД геотрекинга (tracking_sqlite)
            try:
                open_shift(fio)
            except Exception as e:
                print(f"[open_shift warn] {e}")
            # Сохраняем первую точку как "start", если гео есть
            try:
                insert_point(fio, 0, lat_f or 0, lon_f or 0, acc_f or 0, source="start")
            except Exception as e:
                print(f"[insert_point start warn] {e}")

        # ======== СЦЕНАРИЙ "ЗАКОНЧИЛ СМЕНУ" ========
        elif action == "end":
            # Нельзя завершить, если не было старта HH:MM
            if not is_hhmm_local(cell_val):
                raise RuntimeError("Нельзя завершить: нет старта за сегодня.")
            # считаем количество минут (без округления) между стартом и сейчас
            mins = minutes_between(cell_val, None, today, get_msk())
            # записываем итого в формате H:HH:MM
            sheet.update_cell(row, col, fmt_final(mins))
            caption = f"... Отработано {mins//60:02d}:{mins%60:02d}"
            # выключаем geo_watch (смена завершена)
            request.session["geo_watch_enable"] = False

        # ======== СЦЕНАРИЙ "УЕХАЛ" (смена, отъезд/возврат) ========
        elif action == "left":
            # 1) Обязательное решение из модалки: либо даты, либо "не приеду"
            chose_not_return = (not_return == "1")
            if not chose_not_return and (not ret_date or not dep_date):
                raise RuntimeError("Для «Уехал» укажите обе даты или нажмите «Не приеду».")

            # Валидация формата дат (YYYY-MM-DD)
            if ret_date:
                try:
                    datetime.date.fromisoformat(ret_date)
                except Exception:
                    raise RuntimeError("Неверная дата возвращения (ожидается YYYY-MM-DD).")
            if dep_date:
                try:
                    datetime.date.fromisoformat(dep_date)
                except Exception:
                    raise RuntimeError("Неверная дата следующего отъезда (ожидается YYYY-MM-DD).")

            # 2) Обновляем сегодняшнюю ячейку (итого часов за день)
            if cell_val != "" and not is_hhmm_local(cell_val):
                raise RuntimeError("Нельзя уехать: смена уже завершена или стоит другая отметка.")

            if is_hhmm_local(cell_val):
                # уже был старт: считаем фактические минуты
                mins_now = minutes_between(cell_val, None, today, get_msk())
                # логика: если меньше 8 часов — до 8ч, но можно добавить "коридор" +4 часа
                final_mins = mins_now if mins_now >= 8*60 else min(8*60, mins_now + 4*60)
                sheet.update_cell(row, col, fmt_final(final_mins))
            else:
                # старта не было — считаем как минимум 4 часа
                sheet.update_cell(row, col, fmt_final(4*60))

            # Красная/оранжевая подсветка ячейки (уехал)
            try:
                a1 = rowcol_to_a1(row, col)
                sheet.format(a1, {"backgroundColor": {"red": 1.00, "green": 0.93, "blue": 0.80}})
            except Exception:
                pass

            # 3) Проставляем даты (или «не приеду») ДО отправки фото/уведомлений
            if chose_not_return:
                # Сценарий: "не приеду" — шлём текст в тред бригадира
                try:
                    send_message_to_thread(thread_id, f"⚠️ {fio}: не приеду")
                except Exception as e:
                    print(f"[not_return warn] {e}")
            else:
                # Возвращение ↩ (голубой фон в табеле)
                try:
                    rd = datetime.date.fromisoformat(ret_date)
                    rrow = find_row_by_fio(fio)
                    rcol = find_col_by_date(rd)
                    ra1 = rowcol_to_a1(rrow, rcol)
                    sheet.update_cell(rrow, rcol, "")
                    sheet.format(ra1, {"backgroundColor": {"red": 0.80, "green": 0.90, "blue": 1.0}})
                    send_message_to_thread(thread_id, f"📅 {fio} вернётся: {rd.isoformat()}")
                except Exception as e:
                    print(f"[return_date warn] {e}")

                # Следующий отъезд ↘ (песочный фон)
                try:
                    nd = datetime.date.fromisoformat(dep_date)
                    nrow = find_row_by_fio(fio)
                    ncol = find_col_by_date(nd)
                    na1 = rowcol_to_a1(nrow, ncol)
                    sheet.update_cell(nrow, ncol, "")
                    sheet.format(na1, {"backgroundColor": {"red": 1.0, "green": 0.97, "blue": 0.80}})
                    send_message_to_thread(thread_id, f"📅 {fio} следующий отъезд: {nd.isoformat()}")
                except Exception as e:
                    print(f"[departure_date warn] {e}")

            # 4) Формируем подпись для фото (общий текст "уехал")
            caption = f"🚗 {fio} уехал ({date_str})"
            show_modal = False
            request.session["geo_watch_enable"] = False

        # ======== СЦЕНАРИЙ "БОЛЬНИЧНЫЙ" ========
        elif action == "sick":
            # Нельзя поверх завершённой смены ставить больничный
            if cell_val != "" and not is_hhmm_local(cell_val):
                raise RuntimeError("Нельзя поставить больничный: смена уже завершена или стоит другая отметка.")
            if is_hhmm_local(cell_val):
                # был старт — считаем фактическое время и минимум 6 часов
                mins_now = minutes_between(cell_val, None, today, get_msk())
                final_mins = max(6*60, mins_now)
                sheet.update_cell(row, col, fmt_final(final_mins))
            else:
                # старта не было — ставим 6 часов
                sheet.update_cell(row, col, fmt_final(6*60))
            try:
                # зелёная подсветка больничного
                a1 = rowcol_to_a1(row, col)
                sheet.format(a1, {"backgroundColor": {"red": 0.85, "green": 1.00, "blue": 0.85}})
            except Exception:
                pass
            caption = f"💊 {fio} на больничном ({date_str})"
            request.session["geo_watch_enable"] = False

        else:
            raise RuntimeError("Неизвестное действие")

    except Exception as e:
        # Любая бизнес-ошибка (логика проверки, табель) → аккуратно отображаем на форме
        return templates.TemplateResponse(
            "check.html",
            {
                "request": request,
                "fio": fio,
                "message": f"❌ {e}",
                "error": True,
                "show_modal": False
            },
        )

    # добавляем гео-хвост к подписи, если координаты пришли
    geo_suffix = ""
    if lat_f is not None and lon_f is not None:
        try:
            acc_txt = f" (±{int(round(acc_f))}м)" if acc_f is not None else ""
            geo_suffix = f"\n📍 {lat_f:.5f},{lon_f:.5f}{acc_txt}\nhttps://maps.google.com/?q={lat_f},{lon_f}"
        except Exception:
            pass

    caption = caption + geo_suffix

    # отправляем фото в тред бригадира
    try:
        send_photo_to_thread(img_bytes, thread_id, caption)
        msg = "✅ Отметка сохранена и фото отправлено."
        return templates.TemplateResponse(
            "check.html",
            {
                "request": request,
                "fio": fio,
                "message": msg,
                "error": False,
                "show_modal": False,
                "geo_watch": bool(request.session.get("geo_watch_enable"))
            },
        )
    except Exception as e:
        # Если не смогли отправить фото — говорим пользователю, что именно не так
        return templates.TemplateResponse(
            "check.html",
            {
                "request": request,
                "fio": fio,
                "message": f"❌ Не удалось отправить фото в Telegram: {e}",
                "error": True,
                "show_modal": False
            },
        )
        

# ============================================================
#  /brigade и /brigade_check — БРИГАДНЫЕ ОТМЕТКИ (один мастер → несколько людей)
# ============================================================

@app.get("/brigade", response_class=HTMLResponse)
def brigade(request: Request):
    """
    Страница для бригадного учёта:
    - показывает список коллег по бригаде
    - позволяет мастеру выбрать сразу несколько ФИО для массовой отметки.
    """
    guard = require_auth(request)
    if guard: 
        return guard
    fio = request.session["fio"]

    # Находим свою бригаду и всех коллег по ней
    my_team = BRIGADES.get(fio)
    if my_team:
        candidates = [name for name, team in BRIGADES.items() if team == my_team]
    else:
        # если бригада не задана — показываем всех
        candidates = list(EMPLOYEE_THREADS.keys())

    # С себя самого убираем из списка выбора
    if fio in candidates:
        candidates.remove(fio)

    candidates.sort()
    return templates.TemplateResponse("brigade.html", {"request": request, "fio": fio, "candidates": candidates})

# ====== ОБЯЗАТЕЛЬНАЯ ГЕОЛОКАЦИЯ для бригадного start/end ======
@app.post("/brigade_check", response_class=HTMLResponse)
async def brigade_check(
    request: Request,
    action: str = Form(...),                       # "start" | "end"
    employees: Optional[List[str]] = Form(default=None),  # список выбранных сотрудников
    photo: UploadFile = File(...),
    lat: float | None = Form(default=None),
    lon: float | None = Form(default=None),
    acc: float | None = Form(default=None),
    geo_ts: str | None = Form(default=None),
):
    """
    Массовая отметка для выбранной группы сотрудников:
    один мастер делает фото и ставит всем "start"/"end" сразу.
    Геолокация также обязательна для start/end.
    """
    # ⬇️ добавляем общий гард (единое поведение, как в /brigade)
    guard = require_auth(request)
    if guard:
        return guard

    fio = request.session["fio"]  

    # --- если не выбраны сотрудники, прерываем обработку ---
    if not employees:
        # Передаём через сессию flash-сообщение, которое покажем на /brigade
        request.session["brigade_flash"] = {
            "summary": "❌ Не выбраны сотрудники.",
            "details": []
        }
        return RedirectResponse(url="/brigade", status_code=302)
   
    # серверная страховка для start/end — геолокация обязательна
    if action in ("start", "end") and (lat is None or lon is None):
        request.session["brigade_flash"] = {
            "summary": "❌ Нужна геолокация для начала/конца бригады.",
            "details": []
        }
        return RedirectResponse(url="/brigade", status_code=302)

    # мини-хелперы, локальные для этого обработчика
    def is_hhmm_local(s: str) -> bool:
        return bool(re.fullmatch(r"\d{1,2}:\d{2}", (s or "").strip()))

    def compute_rounded_hours(start_hhmm: str, now_local: datetime.datetime, today: datetime.date) -> int:
        sh, sm = map(int, start_hhmm.split(":"))
        start_dt = datetime.datetime.combine(today, datetime.time(sh, sm))
        end_dt = now_local.replace(tzinfo=None)
        if end_dt < start_dt:
            end_dt += datetime.timedelta(days=1)
        delta = end_dt - start_dt
        hrs = delta.seconds // 3600
        mins = (delta.seconds % 3600) // 60
        return hrs + (1 if mins > 20 else 0)

    img_bytes = await photo.read()
    msk = get_msk()
    now_local = datetime.datetime.now(msk)
    today = now_local.date()
    date_str = today.isoformat()

    # Формируем общий гео-хвост для всех сотрудников
    geo_suffix = ""
    try:
        lat_f = float(lat) if lat is not None else None
        lon_f = float(lon) if lon is not None else None
        acc_f = float(acc) if acc is not None else None
        if lat_f is not None and lon_f is not None:
            acc_txt = f" (±{int(round(acc_f))}м)" if acc_f is not None else ""
            geo_suffix = f"\n📍 {lat_f:.5f},{lon_f:.5f}{acc_txt}\nhttps://maps.google.com/?q={lat_f},{lon_f}"
    except Exception:
        geo_suffix = ""

    results = []    # [(fio, "ok"/"err", msg)] — результат по каждому человеку

    # Проходим по каждому выбранному сотруднику и повторяем логику start/end
    for person in employees:
        try:
            row = find_row_by_fio(person)
            col = find_col_by_date(today)
            current = (sheet.cell(row, col).value or "").strip()

            if action == "start":
                if current != "":
                    # делаем сообщение явным, но логику НЕ меняем
                    raise RuntimeError(f"уже есть запись за сегодня: «{current}»")
                t = now_local.strftime("%H:%M")
                sheet.update_cell(row, col, t)
                caption = f"👥 {person}: начало рабочего дня {t} ({date_str})"

            elif action == "end":
                if not is_hhmm_local(current):
                    raise RuntimeError(f"нельзя завершить — нет старта (сейчас в ячейке: «{current or 'пусто'}»)")
                mins = minutes_between(current, None, today, get_msk())
                sheet.update_cell(row, col, fmt_final(mins))
                caption = f"... Отработано {mins//60:02d}:{mins%60:02d}"
            else:
                raise RuntimeError("неизвестное действие")

            caption = caption + geo_suffix

            thread_id = EMPLOYEE_THREADS.get(person)
            if not thread_id:
                raise RuntimeError("нет треда в Telegram для этого ФИО")

            send_photo_to_thread(img_bytes, thread_id, caption)
            results.append((person, "ok", "✅"))

        except Exception as e:
            results.append((person, "err", f"❌ {e}"))

    # Подводим итоги по бригаде
    ok_count = sum(1 for _, s, _ in results if s == "ok")
    err_count = sum(1 for _, s, _ in results if s == "err")
    summary = f"Готово: {ok_count} ок, {err_count} ошибок."

    request.session["brigade_flash"] = {
        "summary": summary,
        "details": [f"{p}: {m}" for p, _, m in results]
    }
    return RedirectResponse(url="/brigade", status_code=302)


# ============================================================
#  ВСПОМОГАТЕЛЬНЫЕ ЭНДПОИНТЫ ДЛЯ ДАТ "ВЕРНЁТСЯ" / "СЛЕД. ОТЪЕЗД"
# ============================================================

@app.post("/return_date")
async def return_date(request: Request, date: str = Form(...)):
    """
    Обновление даты возвращения сотрудника (цветной маркер в табеле).
    Вызывается из фронта через AJAX.
    """
    fio = request.session.get("fio")
    if not fio:
        return JSONResponse({"ok": False, "error": "no session"}, status_code=400)
    try:
        dt = datetime.date.fromisoformat(date)
        row = find_row_by_fio(fio)
        col = find_col_by_date(dt)
        a1  = rowcol_to_a1(row, col)

        sheet.update_cell(row, col, "")
        sheet.format(a1, {"backgroundColor": {"red": 0.80, "green": 0.90, "blue": 1.0}})

        thread_id = EMPLOYEE_THREADS.get(fio)
        if thread_id:
            send_message_to_thread(thread_id, f"📅 {fio} вернётся: {dt.isoformat()}")
        return {"ok": True, "cell": a1}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

@app.post("/departure_date")
async def departure_date(request: Request, date: str = Form(...)):
    """
    Обновление даты следующего отъезда сотрудника (отдельный цвет в табеле).
    Вызывается из фронта.
    """
    fio = request.session.get("fio")
    if not fio:
        return JSONResponse({"ok": False, "error": "no session"}, status_code=400)
    try:
        dt = datetime.date.fromisoformat(date)
        row = find_row_by_fio(fio)
        col = find_col_by_date(dt)
        a1  = rowcol_to_a1(row, col)

        sheet.update_cell(row, col, "")
        sheet.format(a1, {"backgroundColor": {"red": 1.0, "green": 0.97, "blue": 0.80}})

        thread_id = EMPLOYEE_THREADS.get(fio)
        if thread_id:
            send_message_to_thread(thread_id, f"📅 {fio} следующий отъезд: {dt.isoformat()}")
        return {"ok": True, "cell": a1}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


# ============================================================
#  /adjust и /adjust_* — РУЧНЫЕ ПРАВКИ ТАБЕЛЯ РУКОВОДИТЕЛЕМ
# ============================================================

@app.get("/adjust", response_class=HTMLResponse)
def adjust(request: Request):
    """
    Страница ручной корректировки табеля руководителем.
    - показывает текущего пользователя (fio)
    - список коллег по бригаде
    - форму для изменения времени и статусов.
    """
    try:
        uid, fio = current_user(request)
    except HTTPException:
        return RedirectResponse(url="/", status_code=302)

    if not fio:
        return RedirectResponse(url="/", status_code=302)

    # строим список коллег по бригаде
    my_team = BRIGADES.get(fio)
    if my_team:
        teammates = [name for name, team in BRIGADES.items() if team == my_team]
    else:
        teammates = list(EMPLOYEE_THREADS.keys())

    if fio in teammates:
        teammates.remove(fio)
    teammates = [fio] + sorted(teammates)

    today = datetime.date.today()
    return templates.TemplateResponse(
        "adjust.html",
        {
            "request": request,
            "fio": fio,
            "teammates": teammates,
            "today": today.isoformat()
        }
    )

@app.post("/adjust_time", response_class=HTMLResponse)
async def adjust_time(
    request: Request,
    person: str = Form(...),           # чью строчку правим
    date: str = Form(...),             # YYYY-MM-DD — дата смены
    start_time: str = Form(default=""),# новое время начала (опц.)
    end_time: str = Form(default=""),  # новое время конца (опц.)
):
    """
    Ручная правка времени (start/end) в табеле:
    - либо задаём оба значения → пересчёт итога
    - либо только старт
    - либо только конец (если уже был старт).
    """
    try:
        uid, fio = current_user(request)
    except HTTPException:
        return RedirectResponse(url="/", status_code=302)

    if not fio:
        return RedirectResponse(url="/", status_code=302)

    msk = get_msk()
    try:
        target = datetime.date.fromisoformat(date)
        row = find_row_by_fio(person)
        col = find_col_by_date(target)
        a1  = rowcol_to_a1(row, col)
        current = (sheet.cell(row, col).value or "").strip()

        st = start_time.strip()
        en = end_time.strip()
        # проверка формата HH:MM
        if st and not TIME_RE.match(st):
            raise RuntimeError("Неверный формат начала (ожидается HH:MM)")
        if en and not TIME_RE.match(en):
            raise RuntimeError("Неверный формат конца (ожидается HH:MM)")

        admin_note = ""
        if st and en:
            # и начало, и конец → считаем минутажи и записываем итог
            mins = minutes_between(st, en, target, msk)
            sheet.update_cell(row, col, fmt_final(mins))
            admin_note = f"⏱ {st}–{en} → {mins//60:02d}:{mins%60:02d}"
        elif st:
            # только старт — записываем HH:MM как есть
            sheet.update_cell(row, col, st)
            admin_note = f"старт = {st}"
        elif en:
            # только конец — берём текущий старт из ячейки, считаем итог
            if not TIME_RE.match(current):
                raise RuntimeError("Нельзя поставить конец — в таблице нет старта HH:MM")
            mins = minutes_between(current, en, target, msk)
            sheet.update_cell(row, col, fmt_final(mins))
            admin_note = f"{current}–{en} → {mins//60:02d}:{mins%60:02d}"
        else:
            raise RuntimeError("Не указаны ни начало, ни конец")

        # Подсветка ручной правки
        try:
            mark_manual_red(a1)
        except Exception:
            pass

        # Уведомление админу о ручной правке
        try:
            notify_admin(
                f"🛠 Ручная правка: {fio} изменил {person} на {target.isoformat()} "
                f"(было: «{current or 'пусто'}», стало: {admin_note})."
            )
        except Exception:
            pass

        # Уведомление в тред бригады
        try:
            send_message_to_thread(
                get_thread_for(person),
                f"🛠 Ручная правка: {fio} изменил отметку на {target.isoformat()} → {admin_note}"
            )
        except Exception:
            pass

        request.session["adj_flash"] = "✅ Изменения применены"
        return RedirectResponse(url="/adjust", status_code=302)

    except Exception as e:
        request.session["adj_flash"] = f"❌ {e}"
        return RedirectResponse(url="/adjust", status_code=302)

@app.post("/adjust_status", response_class=HTMLResponse)
async def adjust_status(
    request: Request,
    person: str = Form(...),              # чья строка
    date_main: str = Form(...),           # дата больничного/уехал
    status: str = Form(...),              # "sick" | "left"
    return_date: str = Form(default=""),  # опциональная дата возвращения
    next_departure: str = Form(default=""),
):
    """
    Ручная правка статуса "больничный" / "уехал" (с возможностью проставить даты возвращения/отъезда).
    """
    fio = request.session.get("fio")
    if not fio:
        return RedirectResponse(url="/", status_code=302)

    msk = get_msk()
    try:
        day = datetime.date.fromisoformat(date_main)
        row = find_row_by_fio(person)
        col = find_col_by_date(day)
        a1  = rowcol_to_a1(row, col)
        current = (sheet.cell(row, col).value or "").strip()

        main_note = ""
        if status == "sick":
            # Логика больничного: минимум 6 часов
            if TIME_RE.match(current):
                mins_now = minutes_between(current, None, day, msk)
                final_mins = max(6*60, mins_now)
                sheet.update_cell(row, col, fmt_final(final_mins))
                main_note = f"болезнь после старта: {current} → {final_mins//60:02d}:{final_mins%60:02d} (мин. 6ч)"
            else:
                sheet.update_cell(row, col, fmt_final(6*60))
                main_note = "болезнь без старта: 06:00"
        elif status == "left":
            # Логика "уехал" через ручную правку: +4 часа
            if TIME_RE.match(current):
                mins_now = minutes_between(current, None, day, msk)
                final_mins = mins_now + 4*60
                sheet.update_cell(row, col, fmt_final(final_mins))

                main_note = f"уехал после старта: {current} → {final_mins//60:02d}:{final_mins%60:02d} (+4ч)"
            else:
                sheet.update_cell(row, col, fmt_final(4*60))
                main_note = "уехал без старта: 04:00"
        else:
            raise RuntimeError("Неизвестный статус")

        # Подсветка ручной правки
        try:
            mark_manual_red(a1)
        except Exception:
            pass

        # Краткое уведомление в тред
        try:
            if status == "sick":
                send_message_to_thread(get_thread_for(person), f"💊 {person}: больничный ({day.isoformat()})")
            else:
                send_message_to_thread(get_thread_for(person), f"🚗 {person}: уехал ({day.isoformat()})")
        except Exception:
            pass

        extra_notes = []
        # Дополнительная дата возвращения
        if return_date:
            rd = datetime.date.fromisoformat(return_date)
            rrow = find_row_by_fio(person)
            rcol = find_col_by_date(rd)
            ra1  = rowcol_to_a1(rrow, rcol)
            sheet.update_cell(rrow, rcol, "")
            try:
                mark_manual_red(ra1)
            except Exception:
                pass
            extra_notes.append(f"вернётся: {rd.isoformat()}")
            try:
                send_message_to_thread(get_thread_for(person), f"📅 {person} вернётся: {rd.isoformat()}")
            except Exception:
                pass

        # Дополнительная дата следующего отъезда
        if next_departure:
            nd = datetime.date.fromisoformat(next_departure)
            nrow = find_row_by_fio(person)
            ncol = find_col_by_date(nd)
            na1  = rowcol_to_a1(nrow, ncol)
            sheet.update_cell(nrow, ncol, "")
            try:
                mark_manual_red(na1)
            except Exception:
                pass
            extra_notes.append(f"след. отъезд: {nd.isoformat()}")
            try:
                send_message_to_thread(get_thread_for(person), f"📅 {person} следующий отъезд: {nd.isoformat()}")
            except Exception:
                pass

        # Подробное уведомление админу
        note = f"🛠 Ручная правка статуса: {fio} изменил {person} на {day.isoformat()} → {main_note}"
        if extra_notes:
            note += " | " + "; ".join(extra_notes)
        try:
            notify_admin(note)
        except Exception:
            pass

        request.session["adj_flash"] = "✅ Изменения применены"
        return RedirectResponse(url="/adjust", status_code=302)

    except Exception as e:
        request.session["adj_flash"] = f"❌ {e}"
        return RedirectResponse(url="/adjust", status_code=302)


# ============================================================
#  /not_return — "НЕ ПРИЕДУ", ИДЕМПОТЕНТНОСТЬ
# ============================================================

from fastapi.responses import JSONResponse

@app.post("/not_return")
async def not_return(request: Request):
    """
    AJAX-эндпоинт для кнопки "Не приеду".
    Делает отправку сообщения в тред только один раз в день на данного сотрудника (идемпотентность).
    """
    fio = request.session.get("fio")
    if not fio:
        return JSONResponse({"ok": False, "error": "fio is not set in session"}, status_code=400)

    # Идемпотентность: если уже отправляли сегодня для этого FIO — не шлём повторно
    sent_date = request.session.get("not_return_sent_date")
    sent_fio  = request.session.get("not_return_sent_fio")
    today = today_local_iso()

    if sent_date == today and sent_fio == fio:
        # уже отправлено сегодня — просто скажем фронту "пропущено"
        return JSONResponse({"ok": True, "skipped": True})

    # если ещё не отправляли — шлём в тему
    thread_id = EMPLOYEE_THREADS.get(fio)
    if not thread_id:
        return JSONResponse({"ok": False, "error": "thread not found for FIO"}, status_code=400)

    caption = f"⚠️ {fio}: не приеду"
    try:
        send_message_to_thread(thread_id, caption)  # функция отправки текста в тред
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"telegram send error: {e}"}, status_code=502)

    # помечаем в сессии, что сегодня уже отправили
    request.session["not_return_sent_date"] = today
    request.session["not_return_sent_fio"]  = fio

    return JSONResponse({"ok": True})


# ============================================================
#  ОБЩИЕ ХЕЛПЕРЫ ДЛЯ РАБОТЫ СО ВРЕМЕНЕМ И ФОРМАТОМ ТАБЕЛЯ
# ============================================================

def read_cell_today(fio: str, d: datetime.date) -> tuple[int, int, str]:
    """
    Удобно: прочитать (row, col, val) для конкретного ФИО и даты.
    """
    row = find_row_by_fio(fio)
    col = find_col_by_date(d)
    val = (sheet.cell(row, col).value or "").strip()
    return row, col, val

def is_time_hhmm(s: str) -> bool:
    return bool(re.fullmatch(r"\d{1,2}:\d{2}", s))

def is_final_number(s: str) -> bool:
    return bool(re.fullmatch(r"\d{1,2}", s))

def compute_rounded_hours(start_hhmm: str, end_local: datetime.datetime) -> int:
    """
    Альтернативный способ посчитать часы (через datetime).
    Сейчас используется реже, но оставлен как вспомогательный.
    """
    sh, sm = map(int, start_hhmm.split(":"))
    start_dt = datetime.datetime.combine(end_local.date(), datetime.time(sh, sm))
    end_dt = end_local.replace(tzinfo=None)
    if end_dt < start_dt:
        end_dt += datetime.timedelta(days=1)
    delta = end_dt - start_dt
    hrs = delta.seconds // 3600
    mins = (delta.seconds % 3600) // 60
    return hrs + (1 if mins > 20 else 0)

# Регулярное выражение для времени HH:MM
TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")

def parse_hhmm_to_dt(hhmm: str, day: datetime.date, tz) -> datetime.datetime:
    """
    Преобразование строки HH:MM в datetime с указанной датой и часовым поясом.
    """
    h, m = map(int, hhmm.split(":"))
    return datetime.datetime.combine(day, datetime.time(h, m))


# Новый формат только для ИТОГА (не для старта!):
# В табеле итог хранится как "H:HH:MM", чтобы отличать от простого HH:MM старта.
FINAL_RE = re.compile(r"^H:(\d{1,2}):([0-5]\d)$")

def minutes_between(start_hhmm: str, end_hhmm_or_now: Optional[str], day: datetime.date, msk) -> int:
    """
    Разница в минутах между start и end (без округления).
    Если end не задан, берём текущее время, но с фиксированной датой 'day'.
    Полночь учитывается (если end < start -> +день).
    """
    start_dt = parse_hhmm_to_dt(start_hhmm, day, msk)
    if end_hhmm_or_now:
        end_dt = parse_hhmm_to_dt(end_hhmm_or_now, day, msk)
    else:
        now_msk = datetime.datetime.now(msk)
        end_dt = datetime.datetime.combine(day, now_msk.time())  # важная правка: дата фиксирована
    if end_dt < start_dt:
        end_dt += datetime.timedelta(days=1)
    return int((end_dt - start_dt).total_seconds() // 60)

def fmt_final(total_minutes: int) -> str:
    """
    Форматирует итог в формате H:HH:MM (например, H:08:30),
    чтобы отличать от старта HH:MM.
    """
    h, m = divmod(max(0, int(total_minutes)), 60)
    return f"H:{h:02d}:{m:02d}"


def compute_rounded_hours_between(start_hhmm: str, end_hhmm_or_now: Optional[str], day: datetime.date, msk) -> int:
    """
    Ещё один вариант расчёта часов между start и end с округлением по минутам.
    """
    start_dt = parse_hhmm_to_dt(start_hhmm, day, msk)
    if end_hhmm_or_now:
        end_dt = parse_hhmm_to_dt(end_hhmm_or_now, day, msk)
    else:
        end_dt = datetime.datetime.now(msk).replace(tzinfo=None)
    if end_dt < start_dt:
        end_dt += datetime.timedelta(days=1)
    delta = end_dt - start_dt
    hrs = delta.seconds // 3600
    mins = (delta.seconds % 3600) // 60
    return hrs + (1 if mins > 20 else 0)

def get_thread_for(person: str) -> int:
    """
    Удобная обёртка: получить thread_id для конкретного ФИО или кинуть RuntimeError.
    Используется при уведомлениях.
    """
    tid = EMPLOYEE_THREADS.get(person)
    if not tid:
        raise RuntimeError(f"Не найден thread для «{person}»")
    return tid


# ============================================================
#  ОНЛАЙН-КАРТА: API ДЛЯ ФРОНТА (online.js)
# ============================================================

# === Online map endpoints ===
@app.get("/api/online/employees")
def api_online_employees():
    """
    Возвращает список сотрудников и их последних координат/состояния для онлайн-карты.
    Формат задаётся функцией get_last_points() из tracking_sqlite.py.
    """
    try:
        return get_last_points()
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/api/online/track")
def api_online_track(employee_id: str, date: str):
    """
    Возвращает геотрек конкретного сотрудника за выбранную дату.
    Используется фронтом online.js для прорисовки полилинии на карте.
    """
    try:
        return get_track(employee_id, date)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/online", response_class=HTMLResponse)
def online_page(request: Request):
    """
    Страница онлайн-карты (для руководителя).
    В шаблоне подключается Leaflet + online.js.
    """
    return templates.TemplateResponse("online.html", {"request": request})

from fastapi import Request
from fastapi.responses import HTMLResponse

@app.get("/manager", response_class=HTMLResponse)
def manager_page(request: Request):
    """
    Страница табеля для руководителя (/manager).
    Здесь рендерится таблица с часами по сотрудникам.
    """
    return templates.TemplateResponse("manager.html", {"request": request})


# ============================================================
#  /manager/org и /api/org/* — АДМИНКА ОРГСТРУКТУРЫ
# ============================================================

# === Орг-настройки: JSON поверх emp_map ===

@app.get("/manager/org", response_class=HTMLResponse)
def manager_org_page(request: Request):
    """
    Страница управления оргструктурой (/manager/org):
    - список сотрудников
    - привязка к thread_id, бригадам
    - редактирование через JS (manager_org.js).
    """
    return templates.TemplateResponse("manager_org.html", {"request": request})

# --- Треды ---
@app.get("/api/org/threads")
def api_org_threads():
    """
    Возвращает текущую «рабочую» карту ФИО → thread_id.
    Это объединение дефолтов из emp_map.py и переопределений из org.json.
    """
    return EMPLOYEE_THREADS

@app.post("/api/org/threads")
async def api_org_threads_set(payload: dict):
    """
    Установка/обновление thread_id для конкретного ФИО.
    Сохраняется в org.json через org_store.set_thread().
    """
    try:
        fio = (payload.get("fio") or "").strip()
        thread_id = int(payload.get("thread_id"))
        set_thread(fio, thread_id)          # пишем в org.json
        reload_org_in_memory()  # немедленно обновляем рантайм
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

@app.delete("/api/org/threads/{fio}")
def api_org_threads_del(fio: str):
    """
    Удаление привязки ФИО к треду.
    """
    try:
        ok = delete_thread(fio)
        if ok:
            reload_org_in_memory()  # синхронизируем память
        return {"ok": ok}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

# --- Бригады (текущая модель: fio -> brigade_name) ---
@app.get("/api/org/brigades")
def api_org_brigades():
    """
    Возвращает карту ФИО → название бригады.
    """
    return BRIGADES

@app.post("/api/org/brigades")
async def api_org_brigades_set(payload: dict):
    """
    Установка/обновление бригады для сотрудника.
    """
    try:
        fio  = (payload.get("fio") or "").strip()
        name = (payload.get("name") or "").strip()   # пустая строка = удалить назначение
        set_brigade(fio, name)
        reload_org_in_memory()      
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

@app.delete("/api/org/brigades/{fio}")
def api_org_brigades_del(fio: str):
    """
    Удаление бригады у конкретного ФИО.
    """
    try:
        ok = delete_brigade_mapping(fio)
        if ok:
            reload_org_in_memory()
        return {"ok": ok}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

# --- Групповой чат ---
@app.get("/api/org/group_chat_id")
def api_org_group_get():
    """
    Возвращает текущий group_chat_id (ID основного чата, где живут треды).
    """
    return {"group_chat_id": GROUP_CHAT_ID}

@app.post("/api/org/group_chat_id")
async def api_org_group_set(payload: dict):
    """
    Установка нового group_chat_id (ID группового чата для тредов).
    Обновляет и org.json, и глобальные переменные.
    """
    try:
        chat_id = int(payload.get("group_chat_id"))
        set_group_chat_id(chat_id)         # пишем в org.json
        # актуализируем оба идентификатора
        globals()["GROUP_CHAT_ID"] = chat_id
        globals()["GROUP_ID"] = int(os.getenv("GROUP_CHAT_ID", str(chat_id)))
        reload_org_in_memory()
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


# ============================================================
#  Геотрекинг WebApp: /api/geo/ping и /api/geo/watch_ack
# ============================================================

from fastapi import Body

@app.post("/api/geo/ping")
async def api_geo_ping(
    request: Request,
    lat: float = Form(...),
    lon: float = Form(...),
    acc: float = Form(0.0),
    ts: str | None = Form(default=None),  # unix сек, можно не передавать
):
    """
    Приём фоновых геопингов от WebApp (geo_watch.js).
    - берём fio из сессии
    - сохраняем точку в локальной БД tracking_sqlite.live_tracking.db.
    """
    fio = request.session.get("fio")
    if not fio:
        return JSONResponse({"ok": False, "error": "no session"}, status_code=401)
    try:
        # ставим серверное время, если ts не пришёл
        ts_i = int(ts) if ts else int(datetime.datetime.now(get_msk()).timestamp())
        insert_point(fio, ts_i, float(lat), float(lon), float(acc or 0.0), source="webapp")
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


# === NEW: org employees CRUD (fio <-> tg_user_id) ===
from org_store import employees_list, upsert_employee, delete_employee_by_uid, delete_employee_by_fio, as_ids_map

@app.get("/api/org/employees")
def api_org_employees():
    """
    Возвращает список сотрудников (ФИО + tg_user_id) для админки.
    """
    return employees_list()

@app.post("/api/org/employees")
async def api_org_employees_upsert(payload: dict):
    """
    Добавление / обновление сотрудника (ФИО + tg_user_id) в org.json.
    """
    try:
        fio = (payload.get("fio") or "").strip()
        uid = int(payload.get("tg_user_id"))
        upsert_employee(fio, uid)
        reload_org_in_memory()
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

@app.delete("/api/org/employees/{uid}")
def api_org_employees_del(uid: int):
    """
    Удаление сотрудника по tg_user_id.
    """
    try:
        ok = delete_employee_by_uid(int(uid))
        if ok:
            reload_org_in_memory()
        return {"ok": ok}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)

# === NEW: auth by telegram user_id from WebApp ===
from fastapi import Request

@app.post("/api/auth/tg_login2")
async def api_auth_tg_login2(request: Request, payload: dict):
    """
    Авторизация напрямую по Telegram user_id:
    - приходит user_id (из WebApp)
    - проверяем, есть ли он в org.json
    - если есть — создаём сессию (uid + fio).
    """
    try:
        uid = int(payload.get("user_id") or 0)
        fio = USER_ID_TO_FIO.get(uid)
        if not fio:
            return {"ok": False, "error": "unknown user_id"}  # нет в org.json → нет сессии
        request.session["uid"] = uid         # ← ОБЯЗАТЕЛЬНО
        request.session["fio"] = fio         # нормализуем fio по карте
        return {"ok": True, "fio": fio}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


#Дополнительные функции отключаем смену пользователя

from fastapi.responses import RedirectResponse

@app.get("/register")
def register_get(request: Request):
    """
    Временный заглушечный эндпоинт /register.
    Сейчас просто редиректит на "/?auth=required".
    """
    return RedirectResponse(url="/?auth=required", status_code=302)

@app.post("/register")
def register_post(request: Request, fio: str = Form(...)):
    """
    Пост-заглушка для /register.
    """
    return RedirectResponse(url="/?auth=required", status_code=302)


# ============================================================
#  Telegram webhook: /tg/webhook/{token}
# ============================================================

from fastapi import Request, Response
import os, json

BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()

@app.post("/tg/webhook/{token}")
async def tg_webhook(token: str, request: Request):
    """
    Вебхук для Telegram:
    - Принимает JSON-апдейт от Telegram
    - Проверяет, что token в URL совпадает с реальным BOT_TOKEN
    - Передаёт апдейт внутрь python-telegram-bot (v20+ или v13) или в кастомный handler.
    """
    # принимаем только если токен в URL совпадает с реальным
    if not BOT_TOKEN or token != BOT_TOKEN:
        return Response(status_code=404)

    # читаем JSON-апдейт от Telegram
    try:
        data = await request.json()
    except Exception:
        return Response(status_code=400)

    # === Вариант 1: python-telegram-bot v20+ (Application)
    # ожидается, что в bot_webapp.py есть переменная "application"
    try:
        from bot_webapp import application
        from telegram import Update
        update = Update.de_json(data, application.bot)
        await application.update_queue.put(update)
        return Response(status_code=200)
    except Exception:
        pass

    # === Вариант 2: python-telegram-bot v13 (Updater/Dispatcher)
    # ожидается, что в bot_webapp.py есть переменная "updater"
    try:
        from bot_webapp import updater
        from telegram import Update
        update = Update.de_json(data, updater.bot)
        import anyio
        # v13 — синхронный dispatcher; пускаем в пул, чтобы не блокировать FastAPI
        await anyio.to_thread.run_sync(updater.dispatcher.process_update, update)
        return Response(status_code=200)
    except Exception:
        pass

    # === Вариант 3: кастомный обработчик (если есть)
    # если ты в bot_webapp.py сделал свою функцию, например handle_webhook_update(data)
    try:
        from bot_webapp import handle_webhook_update
        # поддержим как sync, так и async реализацию
        result = handle_webhook_update(data)
        if hasattr(result, "__await__"):
            await result
        return Response(status_code=200)
    except Exception:
        pass

    # по умолчанию отвечаем 200, чтобы Telegram не засыпал ретраями
    return Response(status_code=200)


# --- Запуск/остановка PTB Application при старте/остановке FastAPI ---
@app.on_event("startup")
async def _ptb_startup():
    """
    При старте FastAPI пробуем запустить PTB Application (v20+), если он есть.
    Это нужно, если бот и веб-сервер живут в одном процессе.
    """
    try:
        from bot_webapp import application  # PTB v20+
        # Инициализируем и запускаем обработчики
        await application.initialize()
        await application.start()
    except Exception:
        # Если application нет (например, PTB v13 + updater) — тихо пропускаем
        pass

@app.on_event("shutdown")
async def _ptb_shutdown():
    """
    При остановке FastAPI аккуратно останавливаем PTB Application (если он есть).
    """
    try:
        from bot_webapp import application  # PTB v20+
        await application.stop()
    except Exception:
        pass


# ============================================================
#  ROOT HANDLER "/" — ВХОД В WEBAPP
# ============================================================

# ===== ROOT HANDLER (обязателен) =====
from fastapi.responses import RedirectResponse, JSONResponse
import os, hmac, hashlib

def _sign_secret():
    """
    Секрет для проверки подписи uid:
    - берём SIGN_SECRET или BOT_TOKEN из окружения.
    Используется как ключ в HMAC(uid, secret).
    """
    # принимаем подписи от бота: HMAC(uid, key = BOT_TOKEN)
    return os.environ.get("SIGN_SECRET") or os.environ.get("BOT_TOKEN") or ""

@app.api_route("/", methods=["GET","HEAD"])
def root(request: Request, uid: int | None = None, sig: str | None = None):
    """
    Корневой обработчик:
    1) Если пришли с tg-ссылкой (?uid=...&sig=...):
       - проверяем подпись HMAC(uid, secret)
       - если всё корректно и uid есть в org.json → создаём сессию и перекидываем на /check
       - иначе → /denied
    2) Если просто зашли по / — проверяем сессию require_auth() и ведём на /check.
    """
    # Если пришли с tg-ссылкой — проверяем подпись и заводим сессию
    if uid and sig:
        secret = (_sign_secret() or "").encode()
        expected = hmac.new(secret, str(uid).encode(), hashlib.sha256).hexdigest() if secret else ""
        if expected and expected == sig:
            ids_map_now = as_ids_map()
            fio = ids_map_now.get(uid)
            if fio:
                request.session["uid"] = uid
                request.session["fio"] = fio
                return RedirectResponse(url="/check", status_code=302)
        # подпись не сошлась или uid неизвестен — запрещаем доступ
        return RedirectResponse(url="/denied", status_code=302)

    # Обычный вход по сессии
    guard = require_auth(request)
    if guard:
        return guard
    return RedirectResponse(url="/check", status_code=302)

# ===== DEBUG ONLY: /_diag_sign=====
@app.get("/_diag_sign")
def _diag_sign(uid: int, sig: str, request: Request):
    """
    Отладочный эндпоинт для диагностики подписи:
    - показывает, какой expected HMAC ожидается
    - совпадает ли он с фактическим sig
    - есть ли uid в as_ids_map()
    """
    secret = (_sign_secret() or "").encode()
    expected = hmac.new(secret, str(uid).encode(), hashlib.sha256).hexdigest() if secret else ""
    ids_map_now = as_ids_map()
    return JSONResponse({
        "env_has_bot_token": bool(os.environ.get("BOT_TOKEN")),
        "secret_len": len(secret),
        "expected": expected,
        "got": sig,
        "equals": (expected == sig),
        "in_ids_map": (uid in ids_map_now),
        "fio": ids_map_now.get(uid)
    })


# --- GET/HEAD для /check (страница формы) ---
@app.api_route("/check", methods=["GET","HEAD"])
def check_page(request: Request):
    """
    Страница формы отметки смены (check.html).
    Саму обработку post-запроса делает /check (POST) выше.
    """
    guard = require_auth(request)
    if guard:
        return guard
    # В шаблон прокинем FIO из сессии
    return templates.TemplateResponse(
        "check.html",
        {"request": request, "fio": request.session.get("fio", "")}
    )

# --- /api/geo/watch_ack — фронт сообщает, что перестал смотреть geo_watch ---
@app.post("/api/geo/watch_ack")
def api_geo_watch_ack(request: Request):
    """
    Эндпоинт, который вызывает фронт, когда пользователь отключил геотрекинг.
    Снимает флаг geo_watch_enable в сессии.
    """
    request.session["geo_watch_enable"] = False
    return {"ok": True}
