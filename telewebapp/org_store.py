# ================================================================
# org_store.py
#
# Назначение файла:
# ------------------
# Хранение И УПРАВЛЕНИЕ оргструктурой в файле org.json:
#
#   • topics    — карта "ФИО → thread_id" (ID темы в групповом чате Telegram)
#   • brigades  — карта "ФИО → название бригады"
#   • group_chat_id — ID основного группового чата, где живут эти темы
#   • employees — список сотрудников (ФИО + tg_user_id)
#
# Важно:
#  - emp_map.py задаёт ДЕФОЛТЫ (что было "из коробки").
#  - org_store.py поверх этого читает org.json и даёт "боевую" конфигурацию.
#  - org.json можно менять через веб-интерфейс (/manager_org, /api/org/*).
#
# То есть:
#   emp_map.py   — статичные данные по умолчанию
#   org.json     — живой конфиг, меняемый руководителем
#   org_store.py — прослойка между приложением и org.json, даёт удобные API
# ================================================================

import os
import json
import threading
from typing import Dict, Any, Optional, List

# Имя файла org.json:
# -------------------
# Можно переопределить через переменную окружения ORG_JSON,
# по умолчанию используется "org.json" в текущей директории.
ORG_JSON = os.getenv("ORG_JSON", "org.json")

# Мьютекс для потокобезопасной работы с файлом (резерв).
# В текущей версии не используется явно, но оставлен для
# возможного параллельного доступа из разных потоков.
_lock = threading.Lock()


# ---------------------------------------------------------------
# ВНУТРЕННИЕ СЕРВИСНЫЕ ФУНКЦИИ: _ensure_file, _read, _write
# ---------------------------------------------------------------

def _ensure_file():
    """
    Гарантирует существование org.json.

    Если файл отсутствует:
      - создаём новый org.json со следующей структурой:

        {
          "topics": {},
          "brigades": {},
          "group_chat_id": null,
          "employees": []
        }

    Это позволяет остальному коду НЕ проверять каждый раз,
    существует ли файл — он всегда есть.
    """
    if not os.path.exists(ORG_JSON):
        with open(ORG_JSON, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "topics": {},       # "ФИО" → thread_id
                    "brigades": {},     # "ФИО" → "A"/"B"/"C"/...
                    "group_chat_id": None,
                    "employees": []     # [{"fio": "...", "tg_user_id": 123}, ...]
                },
                f,
                ensure_ascii=False,
                indent=2
            )


def _read() -> Dict[str, Any]:
    """
    Читает и возвращает содержимое org.json в виде словаря Python.

    Порядок:
      - если файла нет → создаём его (_ensure_file)
      - открываем org.json и делаем json.load(...)
      - возвращаем словарь.
    """
    _ensure_file()
    with open(ORG_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


def _write(doc: Dict[str, Any]):
    """
    Перезаписывает org.json целиком.

    doc — словарь с ключами:
      "topics", "brigades", "group_chat_id", "employees"

    Файл пишется с отступами, в UTF-8, без экранирования русских букв.
    """
    with open(ORG_JSON, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------
# threads_map — получить карту "ФИО → thread_id"
# ---------------------------------------------------------------

def threads_map(default: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    """
    Возвращает карту "ФИО → thread_id" (topics) из org.json.

    Если передан default:
      - объединяет default и topics из org.json:
        1) берётся копия default
        2) поверх неё накладываются значения из org.json
           (т.е. org.json переопределяет дефолты)

    Это используется так:
      EMPLOYEE_THREADS = threads_map(default=EMPLOYEE_THREADS_ИЗ_emp_map)

    То есть:
      - emp_map.py задаёт стартовые значения (которые были изначально)
      - в org.json можно менять конкретные thread_id, не переписывая весь список.
    """
    data = _read()
    # берём "topics" и нормализуем: ключи → строки, значения → int
    topics = {
        str(k): int(v)
        for k, v in (data.get("topics") or {}).items()
        if k
    }

    if default:
        merged = dict(default)  # копия исходного словаря
        merged.update(topics)   # поверх — переопределения из org.json
        return merged
    return topics


# ---------------------------------------------------------------
# brigades_map — получить карту "ФИО → бригада"
# ---------------------------------------------------------------

def brigades_map(default: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Возвращает карту "ФИО → название бригады" из org.json.

    Логика аналогична threads_map():
      - берём "brigades" из org.json
      - если передан default — объединяем
        (org.json переопределяет дефолты).

    Пример:
      BRIGADES = brigades_map(default=BRIGADES_ИЗ_emp_map)
    """
    data = _read()
    br = {
        str(k): str(v)
        for k, v in (data.get("brigades") or {}).items()
        if k
    }

    if default:
        merged = dict(default)
        merged.update(br)
        return merged
    return br


# ---------------------------------------------------------------
# group_chat_id — ID основного чата Telegram
# ---------------------------------------------------------------

def get_group_chat_id(default: Optional[int] = None) -> Optional[int]:
    """
    Возвращает group_chat_id из org.json.

    Если в org.json:
      - group_chat_id отсутствует или равен null → вернём default
      - group_chat_id есть → пробуем привести к int и вернуть.

    Используется в app.py:
      - чтобы знать, в какой чат отправлять сообщения/фото (sendPhoto/sendMessage).
    """
    data = _read()
    val = data.get("group_chat_id")
    try:
        return int(val) if val is not None else default
    except Exception:
        # если привести к int не получилось — возвращаем default
        return default


def set_group_chat_id(chat_id: int):
    """
    Устанавливает новый group_chat_id в org.json.

    Используется, когда через веб-интерфейс администратор
    меняет основной групповой чат, в котором живут темы сотрудников.
    """
    data = _read()
    data["group_chat_id"] = int(chat_id)
    _write(data)


# ---------------------------------------------------------------
# Работа с "topics" (ФИО → thread_id)
# ---------------------------------------------------------------

def set_thread(fio: str, thread_id: int):
    """
    Задать/обновить thread_id для конкретного сотрудника (ФИО).

    fio       — строка ФИО, ключ (обязательно)
    thread_id — ID темы в супергруппе (int)

    Если fio пустой → ValueError.
    После обновления запись сохраняется в org.json.
    """
    fio = (fio or "").strip()
    if not fio:
        raise ValueError("fio обязателен")

    data = _read()
    tp = data.setdefault("topics", {})  # если "topics" нет — создаём пустой dict
    tp[fio] = int(thread_id)
    _write(data)


def delete_thread(fio: str) -> bool:
    """
    Удаляет привязку ФИО → thread_id из org.json.

    Возвращает:
      True  — если запись была и успешно удалена
      False — если записи не было (ничего не делали)
    """
    fio = (fio or "").strip()
    data = _read()
    tp = data.setdefault("topics", {})

    if fio in tp:
        tp.pop(fio, None)
        _write(data)
        return True
    return False


# ---------------------------------------------------------------
# Работа с "brigades" (ФИО → бригада)
# ---------------------------------------------------------------

def set_brigade(fio: str, name: str):
    """
    Задать/обновить бригаду для сотрудника.

    fio  — ФИО сотрудника
    name — название бригады (строка). Если пустая строка → удаляем бригаду.

    Примеры:
      set_brigade("Иванов Иван", "A")   → назначить бригаду A
      set_brigade("Иванов Иван", "")    → убрать из любой бригады
    """
    fio = (fio or "").strip()
    data = _read()
    br = data.setdefault("brigades", {})

    if (name or "").strip():
        # Непустое название — записываем/обновляем
        br[fio] = (name or "").strip()
    else:
        # Пустое название → удаляем бригаду у сотрудника
        br.pop(fio, None)

    _write(data)


def delete_brigade_mapping(fio: str) -> bool:
    """
    Удаляет информацию о бригаде для указанного ФИО.

    Возвращает:
      True  — если запись была и удалена
      False — если ничего не было.
    """
    fio = (fio or "").strip()
    data = _read()
    br = data.setdefault("brigades", {})

    if fio in br:
        br.pop(fio, None)
        _write(data)
        return True
    return False


# ---------------------------------------------------------------
# employees — список сотрудников (ФИО + tg_user_id)
# ---------------------------------------------------------------

def employees_list() -> List[Dict[str, Any]]:
    """
    Возвращает НОРМАЛИЗОВАННЫЙ список сотрудников из org.json.

    Структура в файле:
      "employees": [
        {"fio": "Иванов Иван Иванович", "tg_user_id": 123456789},
        ...
      ]

    Что делает функция:
      1. Читает массив "employees" (если нет — берёт []).
      2. Нормализует данные:
         - fio → str.strip()
         - tg_user_id → int
         - убирает пустые fio и невалидные/дублирующиеся uid
      3. Если после нормализации список изменился (очистились мусорные записи),
         перезаписывает "employees" обратно в org.json.

    Возвращает:
      Список словарей вида {"fio": str, "tg_user_id": int}.
    """
    data = _read()
    arr = data.get("employees") or []
    norm: List[Dict[str, Any]] = []
    seen = set()   # сюда складываем uid, чтобы не было дублей

    for it in arr:
        try:
            fio = str(it.get("fio") or "").strip()
            uid = int(it.get("tg_user_id"))
            # Пропускаем пустые ФИО и uid=0, а также дубли по uid
            if fio and uid and uid not in seen:
                norm.append({"fio": fio, "tg_user_id": uid})
                seen.add(uid)
        except Exception:
            # Любые ошибки парсинга конкретной записи — игнорируем её
            pass

    # Если нормализованный список отличается от исходного — сохраняем исправленную версию.
    if arr != norm:
        data["employees"] = norm
        _write(data)

    return norm


def as_ids_map() -> Dict[int, str]:
    """
    Возвращает карту "tg_user_id → ФИО", построенную по employees_list().

    Пример:
      employees_list() → [{"fio": "Иванов", "tg_user_id": 111}, {"fio": "Петров", "tg_user_id": 222}]
      as_ids_map()     → {111: "Иванов", 222: "Петров"}

    Используется:
      - в app.py для авторизации по Telegram user_id:
          USER_ID_TO_FIO = as_ids_map()
      - current_user(), /api/auth/tg_login2 и т.п.
    """
    mp: Dict[int, str] = {}
    for it in employees_list():
        mp[int(it["tg_user_id"])] = str(it["fio"])
    return mp


def upsert_employee(fio: str, tg_user_id: int):
    """
    Добавить или обновить сотрудника в списке employees.

    Поведение:
      - Если сотрудник с таким tg_user_id уже есть → заменяем его ФИО.
      - Если нет → добавляем новую запись.

    fio        — строка, ФИО сотрудника
    tg_user_id — Telegram user_id (целое число > 0)

    Используется в:
      - /api/org/employees (админка оргструктуры)
    """
    fio = (fio or "").strip()
    uid = int(tg_user_id)

    if not fio or uid <= 0:
        raise ValueError("fio и tg_user_id обязательны")

    data = _read()
    # Берём уже нормализованный список (заодно чистим мусор)
    arr = employees_list()

    # Удаляем возможную старую запись с таким же uid
    arr = [it for it in arr if int(it["tg_user_id"]) != uid]

    # Добавляем (fio, uid) в конец списка
    arr.append({"fio": fio, "tg_user_id": uid})

    data["employees"] = arr
    _write(data)


def delete_employee_by_uid(uid: int) -> bool:
    """
    Удалить сотрудника по tg_user_id.

    Возвращает:
      True  — если запись была и удалена
      False — если сотрудника с таким uid не было.

    Используется при работе админки, когда удаляют сотрудника.
    """
    uid = int(uid)
    data = _read()
    arr = employees_list()  # нормализованный список
    new_arr = [it for it in arr if int(it["tg_user_id"]) != uid]

    ok = (len(new_arr) != len(arr))
    if ok:
        data["employees"] = new_arr
        _write(data)
    return ok


def delete_employee_by_fio(fio: str) -> bool:
    """
    Удалить сотрудника по ФИО.

    Возвращает:
      True  — если запись была и удалена
      False — если не найдено ни одного сотрудника с таким ФИО.

    Удобно, если нужно чистить по имени, а не по tg_user_id.
    """
    fio = (fio or "").strip()
    data = _read()
    arr = employees_list()
    new_arr = [it for it in arr if str(it["fio"]) != fio]

    ok = (len(new_arr) != len(arr))
    if ok:
        data["employees"] = new_arr
        _write(data)
    return ok
