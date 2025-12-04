
# --- ADD to your existing org_store.py (or replace) ---
import os, json, threading
from typing import Dict, Any, Optional, List

ORG_JSON = os.getenv("ORG_JSON", "org.json")
_lock = threading.Lock()

def _ensure_file():
    if not os.path.exists(ORG_JSON):
        with open(ORG_JSON, "w", encoding="utf-8") as f:
            json.dump({"topics": {}, "brigades": {}, "group_chat_id": None, "employees": []}, f, ensure_ascii=False, indent=2)

def _read() -> Dict[str, Any]:
    _ensure_file()
    with open(ORG_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def _write(doc: Dict[str, Any]):
    with open(ORG_JSON, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=2)

def threads_map(default: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    data = _read()
    topics = {str(k): int(v) for k, v in (data.get("topics") or {}).items() if k}
    if default:
        merged = dict(default); merged.update(topics); return merged
    return topics

def brigades_map(default: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    data = _read()
    br = {str(k): str(v) for k, v in (data.get("brigades") or {}).items() if k}
    if default:
        merged = dict(default); merged.update(br); return merged
    return br

def get_group_chat_id(default: Optional[int] = None) -> Optional[int]:
    data = _read()
    val = data.get("group_chat_id")
    try:
        return int(val) if val is not None else default
    except Exception:
        return default

def set_group_chat_id(chat_id: int):
    data = _read()
    data["group_chat_id"] = int(chat_id)
    _write(data)

def set_thread(fio: str, thread_id: int):
    fio = (fio or "").strip()
    if not fio: raise ValueError("fio обязателен")
    data = _read()
    tp = data.setdefault("topics", {})
    tp[fio] = int(thread_id)
    _write(data)

def delete_thread(fio: str) -> bool:
    fio = (fio or "").strip()
    data = _read()
    tp = data.setdefault("topics", {})
    if fio in tp:
        tp.pop(fio, None); _write(data); return True
    return False

def set_brigade(fio: str, name: str):
    fio = (fio or "").strip()
    data = _read()
    br = data.setdefault("brigades", {})
    if (name or "").strip():
        br[fio] = (name or "").strip()
    else:
        br.pop(fio, None)
    _write(data)

def delete_brigade_mapping(fio: str) -> bool:
    fio = (fio or "").strip()
    data = _read()
    br = data.setdefault("brigades", {})
    if fio in br:
        br.pop(fio, None); _write(data); return True
    return False

# employees
def employees_list() -> List[Dict[str, Any]]:
    data = _read()
    arr = data.get("employees") or []
    norm = []
    seen = set()
    for it in arr:
        try:
            fio = str(it.get("fio") or "").strip()
            uid = int(it.get("tg_user_id"))
            if fio and uid and uid not in seen:
                norm.append({"fio": fio, "tg_user_id": uid})
                seen.add(uid)
        except Exception:
            pass
    if arr != norm:
        data["employees"] = norm; _write(data)
    return norm

def as_ids_map() -> Dict[int, str]:
    mp: Dict[int, str] = {}
    for it in employees_list():
        mp[int(it["tg_user_id"])] = str(it["fio"])
    return mp

def upsert_employee(fio: str, tg_user_id: int):
    fio = (fio or "").strip()
    uid = int(tg_user_id)
    if not fio or uid <= 0: raise ValueError("fio и tg_user_id обязательны")
    data = _read()
    arr = employees_list()
    arr = [it for it in arr if int(it["tg_user_id"]) != uid]
    arr.append({"fio": fio, "tg_user_id": uid})
    data["employees"] = arr
    _write(data)

def delete_employee_by_uid(uid: int) -> bool:
    uid = int(uid)
    data = _read()
    arr = employees_list()
    new_arr = [it for it in arr if int(it["tg_user_id"]) != uid]
    ok = (len(new_arr) != len(arr))
    if ok:
        data["employees"] = new_arr; _write(data)
    return ok

def delete_employee_by_fio(fio: str) -> bool:
    fio = (fio or "").strip()
    data = _read()
    arr = employees_list()
    new_arr = [it for it in arr if str(it["fio"]) != fio]
    ok = (len(new_arr) != len(arr))
    if ok:
        data["employees"] = new_arr; _write(data)
    return ok
