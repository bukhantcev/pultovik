import asyncio
import os
import sqlite3
from typing import List
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from dotenv import load_dotenv
from datetime import datetime, date, UTC
import tempfile
from pathlib import Path
import pandas as pd

# ====== Config ======
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# ====== UI ======
MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="Спектакли"), KeyboardButton(text="Сотрудники")]],
    resize_keyboard=True,
)

# ====== SQLite DB ======
DB_PATH = os.getenv("BOT_DB", "bot.db")

class DB:
    def count_assigned_for_month(self, employee_id: int, year: int, month: int) -> int:
        prefix = f"{year:04d}-{month:02d}-"
        with self._conn() as con:
            row = con.execute(
                """
                SELECT COUNT(*)
                FROM events ev
                JOIN employees e ON e.display = ev.employee
                WHERE e.id=? AND ev.date LIKE ?
                """,
                (employee_id, prefix + '%'),
            ).fetchone()
            return int(row[0] if row and row[0] is not None else 0)
    def __init__(self, path: str):
        self.path = path
        self._ensure()

    def _conn(self):
        return sqlite3.connect(self.path)

    def _ensure(self):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS spectacles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT UNIQUE NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_name TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    tg_id TEXT,
                    display TEXT UNIQUE NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS spectacle_employees (
                    spectacle_id INTEGER NOT NULL,
                    employee_id INTEGER NOT NULL,
                    UNIQUE(spectacle_id, employee_id),
                    FOREIGN KEY(spectacle_id) REFERENCES spectacles(id) ON DELETE CASCADE,
                    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
                )
            """)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS employee_busy (
                    employee_id INTEGER NOT NULL,
                    date_str TEXT NOT NULL,
                    UNIQUE(employee_id, date_str),
                    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
                )
                """
            )
            # monthly busy window, logs, and submissions
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS busy_window (
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    opened_at TEXT,
                    broadcast_sent INTEGER DEFAULT 0,
                    PRIMARY KEY(year, month)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS busy_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    employee_id INTEGER NOT NULL,
                    action TEXT NOT NULL, -- add/remove/clear
                    payload TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS user_submissions (
                    employee_id INTEGER NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    submitted_at TEXT NOT NULL,
                    UNIQUE(employee_id, year, month),
                    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    type TEXT,
                    title TEXT,
                    time TEXT,
                    location TEXT,
                    city TEXT,
                    employee TEXT,
                    info TEXT
                )
                """
            )
            # migration from legacy employees(name)
            # try to detect old schema and migrate rows
            try:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='employees'")
                # check columns
                cols = [r[1] for r in cur.execute("PRAGMA table_info(employees)").fetchall()]
                if 'display' not in cols:
                    # recreate table if old schema exists (fallback migration)
                    cur.execute("ALTER TABLE employees RENAME TO employees_old")
                    cur.execute("""
                        CREATE TABLE employees (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            last_name TEXT NOT NULL,
                            first_name TEXT NOT NULL,
                            tg_id TEXT,
                            display TEXT UNIQUE NOT NULL
                        )
                    """)
                    for (old_name,) in cur.execute("SELECT name FROM employees_old").fetchall():
                        ln = old_name.strip()
                        fn = ''
                        disp = (ln + ((' ' + fn) if fn else '')).strip()
                        cur.execute("INSERT OR IGNORE INTO employees(last_name, first_name, tg_id, display) VALUES(?,?,?,?)", (ln, fn, None, disp))
                    cur.execute("DROP TABLE employees_old")
            except Exception:
                pass
            con.commit()
    def ensure_window(self, year: int, month: int):
        with self._conn() as con:
            con.execute(
                "INSERT OR IGNORE INTO busy_window(year, month, opened_at) VALUES(?,?,?)",
                (year, month, datetime.now(UTC).isoformat()),
            )
            con.commit()

    def get_window(self, year: int, month: int):
        with self._conn() as con:
            return con.execute(
                "SELECT year, month, opened_at, broadcast_sent FROM busy_window WHERE year=? AND month=?",
                (year, month),
            ).fetchone()

    def mark_broadcast_sent(self, year: int, month: int):
        with self._conn() as con:
            con.execute(
                "UPDATE busy_window SET broadcast_sent=1 WHERE year=? AND month=?",
                (year, month),
            )
            con.commit()

    def list_employees_with_tg(self) -> list[tuple[int, str, int]]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT id, display, tg_id FROM employees WHERE tg_id IS NOT NULL AND LENGTH(tg_id)>0"
            ).fetchall()
            # return (id, display, tg_id as int)
            res = []
            for i, d, tg in rows:
                try:
                    res.append((i, d, int(tg)))
                except Exception:
                    continue
            return res

    def log_busy(self, employee_id: int, action: str, payload: str):
        with self._conn() as con:
            con.execute(
                "INSERT INTO busy_log(employee_id, action, payload, ts) VALUES(?,?,?,?)",
                (employee_id, action, payload, datetime.now(UTC).isoformat()),
            )
            con.commit()

    def set_submitted(self, employee_id: int, year: int, month: int):
        with self._conn() as con:
            con.execute(
                "INSERT OR IGNORE INTO user_submissions(employee_id, year, month, submitted_at) VALUES(?,?,?,?)",
                (employee_id, year, month, datetime.now(UTC).isoformat()),
            )
            con.commit()

    def unset_submitted(self, employee_id: int, year: int, month: int):
        with self._conn() as con:
            con.execute(
                "DELETE FROM user_submissions WHERE employee_id=? AND year=? AND month=?",
                (employee_id, year, month),
            )
            con.commit()

    def has_submitted(self, employee_id: int, year: int, month: int) -> bool:
        with self._conn() as con:
            row = con.execute(
                "SELECT 1 FROM user_submissions WHERE employee_id=? AND year=? AND month=?",
                (employee_id, year, month),
            ).fetchone()
            return bool(row)

    def list_all_employees(self) -> list[tuple[int, str]]:
        with self._conn() as con:
            rows = con.execute("SELECT id, display FROM employees ORDER BY last_name, first_name").fetchall()
            return [(r[0], r[1]) for r in rows]

    # Spectacles
    def list_spectacles(self) -> List[str]:
        with self._conn() as con:
            rows = con.execute("SELECT title FROM spectacles ORDER BY title").fetchall()
            return [r[0] for r in rows]

    def upsert_spectacle(self, title: str):
        with self._conn() as con:
            con.execute("INSERT OR IGNORE INTO spectacles(title) VALUES(?)", (title,))
            con.commit()

    def get_spectacle_id(self, title: str):
        with self._conn() as con:
            row = con.execute("SELECT id FROM spectacles WHERE title=?", (title,)).fetchone()
            return row[0] if row else None

    # Employees
    def list_employees(self) -> List[str]:
        with self._conn() as con:
            rows = con.execute("SELECT display FROM employees ORDER BY last_name, first_name").fetchall()
            return [r[0] for r in rows]

    def upsert_employee(self, last_name: str, first_name: str, tg_id: str | None = None):
        last_name = (last_name or '').strip()
        first_name = (first_name or '').strip()
        if not last_name or not first_name:
            raise ValueError('Фамилия и Имя обязательны')
        display = f"{last_name} {first_name}".strip()
        with self._conn() as con:
            con.execute(
                "INSERT OR IGNORE INTO employees(last_name, first_name, tg_id, display) VALUES(?,?,?,?)",
                (last_name, first_name, tg_id, display),
            )
            # update tg_id if provided later
            if tg_id:
                con.execute("UPDATE employees SET tg_id=? WHERE display=?", (tg_id, display))
            con.commit()

    def get_employee_id(self, display: str):
        with self._conn() as con:
            row = con.execute("SELECT id FROM employees WHERE display=?", (display,)).fetchone()
            return row[0] if row else None

    # Relations
    def set_spectacle_employees(self, title: str, employee_names: List[str]):
        self.upsert_spectacle(title)
        with self._conn() as con:
            sid = con.execute("SELECT id FROM spectacles WHERE title=?", (title,)).fetchone()[0]
            # clear old links
            con.execute("DELETE FROM spectacle_employees WHERE spectacle_id=?", (sid,))
            # link only existing employees (skip unknown names)
            for nm in employee_names:
                row = con.execute("SELECT id FROM employees WHERE display=?", (nm,)).fetchone()
                if row:
                    con.execute(
                        "INSERT OR IGNORE INTO spectacle_employees(spectacle_id, employee_id) VALUES(?,?)",
                        (sid, row[0]),
                    )
            con.commit()

    def get_spectacle_employees(self, title: str) -> List[str]:
        with self._conn() as con:
            row = con.execute("SELECT id FROM spectacles WHERE title=?", (title,)).fetchone()
            if not row:
                return []
            sid = row[0]
            rows = con.execute(
                """
                SELECT e.display FROM spectacle_employees se
                JOIN employees e ON e.id = se.employee_id
                WHERE se.spectacle_id=?
                ORDER BY e.last_name, e.first_name
                """,
                (sid,),
            ).fetchall()
            return [r[0] for r in rows]

    def list_employees_full(self) -> list[tuple[int, str]]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT id, display FROM employees ORDER BY last_name, first_name"
            ).fetchall()
            return [(r[0], r[1]) for r in rows]

    def get_spectacle_employee_ids(self, title: str) -> set[int]:
        with self._conn() as con:
            row = con.execute("SELECT id FROM spectacles WHERE title=?", (title,)).fetchone()
            if not row:
                return set()
            sid = row[0]
            rows = con.execute(
                "SELECT employee_id FROM spectacle_employees WHERE spectacle_id=?",
                (sid,),
            ).fetchall()
            return {r[0] for r in rows}

    def toggle_spectacle_employee(self, spectacle_id: int, employee_id: int) -> None:
        with self._conn() as con:
            exists = con.execute(
                "SELECT 1 FROM spectacle_employees WHERE spectacle_id=? AND employee_id=?",
                (spectacle_id, employee_id),
            ).fetchone()
            if exists:
                con.execute(
                    "DELETE FROM spectacle_employees WHERE spectacle_id=? AND employee_id=?",
                    (spectacle_id, employee_id),
                )
            else:
                con.execute(
                    "INSERT OR IGNORE INTO spectacle_employees(spectacle_id, employee_id) VALUES(?,?)",
                    (spectacle_id, employee_id),
                )
            con.commit()

    def delete_employee(self, employee_id: int) -> None:
        with self._conn() as con:
            con.execute("DELETE FROM employees WHERE id=?", (employee_id,))
            con.commit()

    def set_employee_tg_by_id(self, employee_id: int, tg_id: str | None) -> None:
        with self._conn() as con:
            con.execute("UPDATE employees SET tg_id=? WHERE id=?", (tg_id, employee_id))
            con.commit()

    def get_employee_by_tg(self, tg_id: int | str):
        with self._conn() as con:
            row = con.execute("SELECT id, display FROM employees WHERE tg_id=?", (str(tg_id),)).fetchone()
            return row  # (id, display) or None

    def add_busy_dates(self, employee_id: int, dates: list[str]) -> list[str]:
        added = []
        with self._conn() as con:
            for ds in dates:
                cur = con.execute(
                    "INSERT OR IGNORE INTO employee_busy(employee_id, date_str) VALUES(?, ?)",
                    (employee_id, ds),
                )
                if cur.rowcount:
                    added.append(ds)
            con.commit()
        return added

    def list_busy_dates(self, employee_id: int) -> list[str]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT date_str FROM employee_busy WHERE employee_id=? ORDER BY date_str",
                (employee_id,),
            ).fetchall()
            return [r[0] for r in rows]

    def remove_busy_dates(self, employee_id: int, dates: list[str]) -> list[str]:
        removed = []
        with self._conn() as con:
            for ds in dates:
                cur = con.execute(
                    "DELETE FROM employee_busy WHERE employee_id=? AND date_str=?",
                    (employee_id, ds),
                )
                if cur.rowcount:
                    removed.append(ds)
            con.commit()
        return removed

    def clear_busy_dates(self, employee_id: int) -> None:
        with self._conn() as con:
            con.execute("DELETE FROM employee_busy WHERE employee_id=?", (employee_id,))
            con.commit()

    def count_busy_for_month(self, employee_id: int, year: int, month: int) -> int:
        prefix = f"{year:04d}-{month:02d}-"
        with self._conn() as con:
            row = con.execute(
                "SELECT COUNT(*) FROM employee_busy WHERE employee_id=? AND date_str LIKE ?",
                (employee_id, prefix + '%'),
            ).fetchone()
            return int(row[0] if row and row[0] is not None else 0)

    def delete_events_for_month(self, year: int, month: int):
        prefix = f"{year:04d}-{month:02d}-"
        with self._conn() as con:
            con.execute("DELETE FROM events WHERE date LIKE ?", (prefix + '%',))
            con.commit()

    def insert_events(self, rows: list[dict]):
        if not rows:
            return
        with self._conn() as con:
            con.executemany(
                """
                INSERT INTO events(date, type, title, time, location, city, employee, info)
                VALUES(:date, :type, :title, :time, :location, :city, :employee, :info)
                """,
                rows,
            )
            con.commit()

DBI = DB(DB_PATH)

# ====== FSM ======
class AddSpectacle(StatesGroup):
    waiting_for_name = State()
    waiting_for_employees = State()

class AddEmployee(StatesGroup):
    waiting_for_last_name = State()
    waiting_for_first_name = State()
    waiting_for_tg_id = State()

# FSM for editing employee TG ID
class EditEmployeeTg(StatesGroup):
    waiting_for_tg = State()

import calendar

# ====== UI helpers ======

def get_employees_kb(selected: list[str] | None = None):
    if selected is None:
        selected = []
    btns = []
    for emp in DBI.list_employees():
        mark = "✅ " if emp in selected else ""
        btns.append([KeyboardButton(text=f"{mark}{emp}")])
    btns.append([KeyboardButton(text="✅ Готово")])
    return ReplyKeyboardMarkup(keyboard=btns, resize_keyboard=True)

def get_spectacles_inline_kb():
    builder = InlineKeyboardBuilder()
    for name in DBI.list_spectacles():
        builder.button(text=name, callback_data=f"title:{name}")
    builder.button(text="➕ Добавить", callback_data="add_spectacle")
    builder.adjust(1)
    return builder.as_markup()

RU_MONTHS = [
    "Январь","Февраль","Март","Апрель","Май","Июнь",
    "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь",
]

# Genitive case month names for Russian dates
RU_MONTHS_GEN = [
    "января","февраля","марта","апреля","мая","июня",
    "июля","августа","сентября","октября","ноября","декабря",
]

def human_ru_date(date_str: str) -> str:
    """Convert 'YYYY-MM-DD' to 'D <месяца> YYYY' in Russian (e.g., '1 сентября 2025')."""
    try:
        y_s, m_s, d_s = date_str.split("-")
        y = int(y_s); m = int(m_s); d = int(d_s)
        if 1 <= m <= 12:
            return f"{d} {RU_MONTHS_GEN[m-1]} {y}"
        return date_str
    except Exception:
        return date_str

# ====== Auto-assignment helpers ======
TYPE_ORDER = {"монтаж": 0, "репетиция": 1, "репетиции": 1, "спектакль": 2}

def _normalize_type(tp: str | None) -> str:
    if not tp:
        return "спектакль"
    s = str(tp).strip().lower()
    if s.startswith("монтаж"):
        return "монтаж"
    if s.startswith("репет"):
        return "репетиция"
    if s.startswith("спект"):
        return "спектакль"
    return s

# ... keep all helpers like _get_qualified_employee_ids, _date_busy_map_for_employees, _already_assigned_dates_map,
# _pick_employee_for_block, _update_event_employee_by_ids, and auto_assign_events_for_month exactly as in my previous response.

# --- Auto-assignment helpers ---
def _get_qualified_employee_ids(title: str) -> set[int]:
    # Returns set of employee IDs qualified for the given spectacle title
    return DBI.get_spectacle_employee_ids(title)

def _date_busy_map_for_employees(employee_ids: set[int]) -> dict[int, set[str]]:
    # Returns {eid: set of busy dates (YYYY-MM-DD)}
    busy_map = {}
    for eid in employee_ids:
        busy_map[eid] = set(DBI.list_busy_dates(eid))
    return busy_map

def _already_assigned_dates_map(events: list[dict]) -> dict[str, set[int]]:
    # Returns {date: set of employee_ids already assigned}
    date_map = {}
    for ev in events:
        d = ev.get('date')
        emp = ev.get('employee')
        if not d or not emp:
            continue
        # Employee may be stored as display name, but try to get ID
        eid = DBI.get_employee_id(emp)
        if eid is not None:
            date_map.setdefault(d, set()).add(eid)
    return date_map

def _pick_employee_for_block(
    block: list[dict],
    qualified_ids: set[int],
    busy_map: dict[int, set[str]],
    assigned_dates: dict[str, set[int]],
    prefer_not: int | None = None,
) -> int | None:
    """Выбор сотрудника для блока по правилам:
    - Только из тех, кто привязан к спектаклю (qualified_ids)
    - Не занят на эту дату (busy_map)
    - Не назначен уже на эту дату (assigned_dates)
    - Баланс по минимальному числу назначений в этом месяце
    - В Москве при подряд идущих одинаковых названиях стараемся не ставить того же (prefer_not)
    """
    if not block or not qualified_ids:
        return None

    date = block[0].get("date")
    if not date:
        return None

    # Фильтр по доступности и отсутствию даблбука
    candidates: list[int] = []
    for eid in qualified_ids:
        if date in busy_map.get(eid, set()):
            continue
        if eid in assigned_dates.get(date, set()):
            continue
        candidates.append(eid)
    if not candidates:
        return None

    # Баланс: минимальное число назначений в этом месяце
    try:
        y = int(date[0:4]); m = int(date[5:7])
    except Exception:
        y = 1970; m = 1
    scored = [(DBI.count_assigned_for_month(eid, y, m), eid) for eid in candidates]
    scored.sort()  # по количеству, затем по id
    ordered = [eid for _, eid in scored]

    # Избегаем prefer_not если есть альтернатива
    if prefer_not is not None and len(ordered) > 1 and ordered[0] == prefer_not:
        return ordered[1]

    return ordered[0]

def _update_event_employee_by_ids(event_ids: list[int], employee_id: int):
    # Updates events (by rowid) to set employee by display name
    with DBI._conn() as con:
        row = con.execute("SELECT display FROM employees WHERE id=?", (employee_id,)).fetchone()
        if not row:
            return
        display = row[0]
        for eid in event_ids:
            con.execute("UPDATE events SET employee=? WHERE id=?", (display, eid))
        con.commit()

def auto_assign_events_for_month(year: int | None = None, month: int | None = None) -> int:
    # If year/month not given, process all events
    with DBI._conn() as con:
        if year and month:
            prefix = f"{year:04d}-{month:02d}-"
            rows = con.execute("SELECT id, date, type, title, city, employee FROM events WHERE date LIKE ?", (prefix + "%",)).fetchall()
        else:
            rows = con.execute("SELECT id, date, type, title, city, employee FROM events").fetchall()
        # Build blocks: (date, type, title, city) -> [event dicts]
        blocks = {}
        for r in rows:
            eid, d, t, title, city, emp = r
            k = (d, _normalize_type(t), title, (city or '').strip())
            blocks.setdefault(k, []).append({'id': eid, 'date': d, 'type': t, 'title': title, 'city': city, 'employee': emp})
        # For each block, if employee is empty, assign
        updated = 0
        # Prepare busy maps
        # Get all unique employees relevant for any block
        all_titles = set(title for (_, _, title, _) in blocks.keys() if title)
        all_emp_ids = set()
        for title in all_titles:
            all_emp_ids.update(_get_qualified_employee_ids(title))
        busy_map = _date_busy_map_for_employees(all_emp_ids)
        # Prepare already assigned map
        assigned_dates = _already_assigned_dates_map([ev for block in blocks.values() for ev in block])
        # Dict to remember last assigned employee per Moscow title
        last_moscow_assignee: dict[str, int] = {}
        # Sort blocks by (date, title, type order)
        def _block_sort_key(k):
            d, tp, title, city = k
            tord = TYPE_ORDER.get(_normalize_type(tp), 99)
            return (d, title or '', tord)
        for k in sorted(blocks, key=_block_sort_key):
            block = blocks[k]
            # If all already assigned, skip
            if all(ev.get('employee') for ev in block):
                continue
            date, tp, title, city = k
            qualified = _get_qualified_employee_ids(title)
            prefer_not = None
            if (city or '').strip().lower() == 'москва':
                # avoid repeating the same employee on consecutive same-title days in Moscow
                prefer_not = last_moscow_assignee.get(title)
            eid = _pick_employee_for_block(block, qualified, busy_map, assigned_dates, prefer_not=prefer_not)
            if eid is not None:
                ids_to_update = [ev['id'] for ev in block if not ev.get('employee')]
                _update_event_employee_by_ids(ids_to_update, eid)
                updated += len(ids_to_update)
                # Update assigned_dates and busy_map
                assigned_dates.setdefault(date, set()).add(eid)
                busy_map.setdefault(eid, set()).add(date)
                if (city or '').strip().lower() == 'москва':
                    last_moscow_assignee[title] = eid
        return updated
async def handle_auto_assign(message: Message):
    if not _is_admin(message.from_user.id):
        await message.answer("Только для админа")
        return
    import re
    m = re.search(r"(?i)^автоназначение\s*(\d{4})-(\d{2})\s*$", message.text or "")
    if m:
        y = int(m.group(1)); mo = int(m.group(2))
        updated = auto_assign_events_for_month(y, mo)
        await message.answer(f"Автоназначение за {RU_MONTHS[mo-1]} {y}: обновлено {updated}")
    else:
        updated = auto_assign_events_for_month()
        await message.answer(f"Автоназначение (все события): обновлено {updated}")
def _is_admin(user_id: int) -> bool:
    return bool(ADMIN_ID) and str(user_id) == str(ADMIN_ID)

def can_show_user_busy_buttons(user_id: int, today: date | None = None) -> bool:
    d = today or date.today()
    # обычным пользователям inline/reply-кнопки показываем до 25-го, админу — всегда
    return _is_admin(user_id) or d.day < 25

def get_user_busy_reply_kb(user_id: int) -> ReplyKeyboardMarkup:
    # Главные кнопки доступны только админу
    base_rows = []
    if _is_admin(user_id):
        base_rows = [[KeyboardButton(text="Спектакли"), KeyboardButton(text="Сотрудники")],
                     [KeyboardButton(text="Сделать график")]]

    # Если кнопки занятости скрыты для пользователя — показываем только главное меню (если оно есть)
    if not can_show_user_busy_buttons(user_id):
        # если не админ, base_rows может быть пустым, тогда вернём пустую либо базовую клавиатуру
        return ReplyKeyboardMarkup(keyboard=base_rows or [], resize_keyboard=True)

    next_m, next_y, mname = next_month_and_year()

    has_busy = False
    row = DBI.get_employee_by_tg(user_id)
    if row:
        has_busy = DBI.count_busy_for_month(row[0], next_y, next_m) > 0

    # Показываем ровно одну кнопку: либо "Подать даты...", либо "Посмотреть свои даты"
    busy_rows = [[KeyboardButton(text="Посмотреть свои даты")]] if has_busy else [[KeyboardButton(text=f"Подать даты за {mname}")]]

    return ReplyKeyboardMarkup(keyboard=base_rows + busy_rows, resize_keyboard=True)
def get_user_busy_manage_kb(show_add: bool = True, user_id: int | None = None):
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить", callback_data="busy:add")
    builder.button(text="➖ Убрать", callback_data="busy:remove")
    builder.adjust(2)
    return builder.as_markup()

def get_employees_inline_kb():
    builder = InlineKeyboardBuilder()
    for disp in DBI.list_employees():
        builder.button(text=disp, callback_data=f"emp:show:{disp}")
    builder.button(text="➕ Добавить", callback_data="emp:add")
    builder.adjust(1)
    return builder.as_markup()

def get_edit_employees_inline_kb(sid: int):
    builder = InlineKeyboardBuilder()
    # current links
    with DBI._conn() as con:
        rows = con.execute("SELECT employee_id FROM spectacle_employees WHERE spectacle_id=?", (sid,)).fetchall()
        current = {r[0] for r in rows}
    for eid, disp in DBI.list_employees_full():
        checked = "✅ " if eid in current else ""
        builder.button(text=f"{checked}{disp}", callback_data=f"edittoggle:{sid}:{eid}")
    builder.button(text="✅ Готово", callback_data=f"editdone:{sid}")
    builder.adjust(1)
    return builder.as_markup()

async def _send_next_unknown_for_assignment(message_or_cb_msg, state: FSMContext):
    data = await state.get_data()
    sids: list[int] = data.get('assign_unknown_sids', [])
    idx: int = int(data.get('assign_idx', 0))
    if not sids or idx >= len(sids):
        return False
    sid = int(sids[idx])
    with DBI._conn() as con:
        row = con.execute("SELECT title FROM spectacles WHERE id=?", (sid,)).fetchone()
    title = row[0] if row else "Спектакль"
    await message_or_cb_msg.answer(
        f"Новый спектакль из Excel: {title}\nНазначьте сотрудников:",
        reply_markup=get_edit_employees_inline_kb(sid)
    )
    return True

# ====== Handlers ======
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Выберите раздел:", reply_markup=get_user_busy_reply_kb(message.from_user.id))

# ========== BUSY FLOW ========== #
# Month utils
def next_month_and_year(today: date = None):
    if today is None:
        today = date.today()
    m = today.month + 1
    y = today.year
    if m > 12:
        m = 1
        y += 1
    mname = RU_MONTHS[m-1]
    return m, y, mname

def format_busy_dates_for_month(days: list[int], month: int, year: int) -> list[str]:
    return [f"{year:04d}-{month:02d}-{d:02d}" for d in days]


import calendar

def parse_days_for_month(text: str, month: int, year: int) -> list[int]:
    s = (text or "").replace(" ", "").strip()
    if not s:
        return []
    max_day = calendar.monthrange(year, month)[1]
    days: set[int] = set()
    for part in s.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                a_i = int(a); b_i = int(b)
            except ValueError:
                continue
            if a_i > b_i:
                a_i, b_i = b_i, a_i
            for d in range(a_i, b_i + 1):
                if 1 <= d <= max_day:
                    days.add(d)
        else:
            try:
                v = int(part)
                if 1 <= v <= max_day:
                    days.add(v)
            except ValueError:
                continue
    return sorted(days)

async def ensure_known_user_or_report_message(message: Message) -> int | None:
    row = DBI.get_employee_by_tg(message.from_user.id)
    if row:
        return row[0]
    if ADMIN_ID:
        u = message.from_user
        info = (f"Неизвестный пользователь\nID: {u.id}\nИмя: {u.first_name}\nФамилия: {u.last_name}\nUsername: @{u.username if u.username else '-'}")
        kb = InlineKeyboardBuilder()
        for eid, disp in DBI.list_employees_full():
            kb.button(text=disp, callback_data=f"maptg:{eid}:{u.id}")
        kb.adjust(1)
        try:
            await message.bot.send_message(ADMIN_ID, info, reply_markup=kb.as_markup())
        except Exception:
            pass
    await message.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
    return None

# Dummy busy flow handlers for demonstration; replace with your real implementations.
class BusyInput(StatesGroup):
    waiting_for_add_user = State()
    waiting_for_remove_user = State()

class AdminBusyInput(StatesGroup):
    waiting_for_add = State()
    waiting_for_remove = State()

# FSM for Excel upload flow
class UploadExcel(StatesGroup):
    waiting_for_month = State()
class AssignUnknown(StatesGroup):
    waiting = State()
def get_month_pick_inline(today: date | None = None, prefix: str = 'xlsmonth:'):
    d = today or date.today()
    buttons = []
    for i in range(3):
        m = d.month + i
        y = d.year
        if m > 12:
            m -= 12
            y += 1
        label = f"{RU_MONTHS[m-1]} {y}"
        cb = f"{prefix}{y:04d}-{m:02d}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=cb)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)
# ====== Handlers ======

# Excel import helpers
EXPECTED_EVENT_COLUMNS = {
    'id': 'id',
    'дата': 'date',
    'тип': 'type',
    'название': 'title',
    'время': 'time',
    'локация': 'location',
    'город': 'city',
    'сотрудник': 'employee',
    'инфо': 'info',
}

def _normalize_event_columns(df: pd.DataFrame) -> pd.DataFrame:
    # map columns by lower-case name
    mapping = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key in EXPECTED_EVENT_COLUMNS:
            mapping[col] = EXPECTED_EVENT_COLUMNS[key]
    return df.rename(columns=mapping)

def import_events_from_excel(path: Path, year: int, month: int) -> tuple[int, int, list[str]]:
    """Reads Excel and replaces events for given year-month. Returns (deleted, inserted, titles).
    Expect 'Дата' to contain day numbers (1..31). Any month/year in file is ignored.
    """
    df = pd.read_excel(path)
    df = _normalize_event_columns(df)
    if 'date' not in df.columns:
        raise ValueError("В Excel нет колонки 'Дата'")

    # keep only expected columns
    keep = ['date','type','title','time','location','city','employee','info']
    for k in keep:
        if k not in df.columns:
            df[k] = None

    # Coerce 'date' to day integers 1..max_day for selected month
    import re, calendar as _cal
    max_day = _cal.monthrange(year, month)[1]

    def _to_day(v):
        if pd.isna(v):
            return None
        # pandas Timestamp -> take day
        if hasattr(v, 'day'):
            try:
                d = int(v.day)
                return d if 1 <= d <= max_day else None
            except Exception:
                return None
        # numeric (Excel may give floats)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            try:
                d = int(v)
                return d if 1 <= d <= max_day else None
            except Exception:
                return None
        # string: extract first integer token
        s = str(v).strip()
        m = re.search(r"\d+", s)
        if m:
            try:
                d = int(m.group(0))
                return d if 1 <= d <= max_day else None
            except Exception:
                return None
        return None

    days = df['date'].map(_to_day)
    df = df.assign(_day=days)
    df = df[df['_day'].notna()]

    # Build full YYYY-MM-DD from selected month/year and day
    df['date'] = df['_day'].astype(int).map(lambda d: f"{year:04d}-{month:02d}-{d:02d}")
    df = df.drop(columns=['_day'])

    # Collect unique non-empty titles for the selected period
    titles: list[str] = []
    if 'title' in df.columns:
        for v in df['title']:
            if pd.isna(v):
                continue
            s = str(v).strip()
            if s and s not in titles:
                titles.append(s)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'date': r['date'],
            'type': None if pd.isna(r['type']) else str(r['type']),
            'title': None if pd.isna(r['title']) else str(r['title']),
            'time': None if pd.isna(r['time']) else str(r['time']),
            'location': None if pd.isna(r['location']) else str(r['location']),
            'city': None if pd.isna(r['city']) else str(r['city']),
            'employee': None if pd.isna(r['employee']) else str(r['employee']),
            'info': None if pd.isna(r['info']) else str(r['info']),
        })

    # Replace in DB for chosen month
    DBI.delete_events_for_month(year, month)
    DBI.insert_events(rows)
    return (0, len(rows), titles)


# Handler to accept Excel from admin and ask for month
async def handle_excel_upload(message: Message, state: FSMContext):
    # admin only
    if not _is_admin(message.from_user.id):
        return
    doc = message.document
    if not doc:
        return
    # save to temp file
    file = await message.bot.get_file(doc.file_id)
    suffix = Path(doc.file_name or 'upload.xlsx').suffix or '.xlsx'
    tmp = Path(tempfile.gettempdir()) / f"pultovik_upload_{message.from_user.id}{suffix}"
    await message.bot.download_file(file.file_path, destination=tmp)
    await state.update_data(upload_path=str(tmp))
    await state.set_state(UploadExcel.waiting_for_month)
    await message.answer("На какой месяц?", reply_markup=get_month_pick_inline(prefix='xlsmonth:'))
# ====== Make Schedule Handlers ======
async def handle_make_schedule(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        await message.answer("Только для админа")
        return
    await message.answer("На какой месяц сформировать график?", reply_markup=get_month_pick_inline(prefix='mkmonth:'))

async def handle_make_schedule_pick(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ''
    if not data.startswith('xlsmonth:') and not data.startswith('mkmonth:'):
        await callback.answer(); return
    # поддержим оба префикса, но на отправку файла используем mkmonth:
    if data.startswith('xlsmonth:'):
        ym = data.split(':',1)[1]
    else:
        ym = data.split(':',1)[1]
    try:
        year_s, month_s = ym.split('-',1)
        year = int(year_s); month = int(month_s)
    except Exception:
        await callback.answer("Неверный месяц", show_alert=True)
        return

    # автоназначение для выбранного месяца
    updated = auto_assign_events_for_month(year, month)

    # собрать события за месяц
    prefix = f"{year:04d}-{month:02d}-"
    with DBI._conn() as con:
        evs = con.execute(
            """
            SELECT date, COALESCE(type,''), COALESCE(title,''), COALESCE(time,''),
                   COALESCE(location,''), COALESCE(city,''), COALESCE(employee,''), COALESCE(info,'')
            FROM events WHERE date LIKE ? ORDER BY date, title, time
            """,
            (prefix+'%',)
        ).fetchall()

    # Create dataframe without internal id and format columns
    cols_internal = ["date","type","title","time","location","city","employee","info"]
    df = pd.DataFrame(evs, columns=cols_internal)

    # Human-readable Russian date, e.g., "2 сентября 2025"
    df["date"] = df["date"].map(human_ru_date)

    # Rename columns to nice Russian headers
    df = df.rename(columns={
        "date": "Дата",
        "type": "Тип",
        "title": "Название",
        "time": "Время",
        "location": "Локация",
        "city": "Город",
        "employee": "Сотрудник",
        "info": "Инфо",
    })

    # сохранить во временный XLSX
    out_path = Path(tempfile.gettempdir()) / f"График_{year}-{month:02d}.xlsx"
    try:
        df.to_excel(out_path, index=False)
    except Exception as e:
        await callback.message.answer(f"Ошибка формирования файла: {e}")
        await callback.answer(); return

    # отправить файл админу
    try:
        file = FSInputFile(str(out_path))
        await callback.message.answer_document(file, caption=f"График на {RU_MONTHS[month-1]} {year}. Обновлено назначений: {updated}")
    except Exception as e:
        await callback.message.answer(f"Не удалось отправить файл: {e}")
    await callback.answer("Готово")

# Callback handler to import after month picked
async def handle_excel_month_pick(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ''
    if not data.startswith('xlsmonth:'):
        await callback.answer()
        return
    ym = data.split(':',1)[1]
    try:
        year_s, month_s = ym.split('-',1)
        year = int(year_s); month = int(month_s)
    except Exception:
        await callback.answer("Неверный месяц", show_alert=True)
        return
    st = await state.get_data()
    p = st.get('upload_path')
    if not p:
        await callback.answer("Файл не найден. Пришлите Excel заново.", show_alert=True)
        return
    try:
        deleted, inserted, titles = import_events_from_excel(Path(p), year, month)
    except Exception as e:
        await callback.message.answer(f"Ошибка импорта: {e}")
        await state.clear()
        await callback.answer()
        return
    await state.clear()

    # Find unknown spectacles, save them, and start sequential assignment
    unknown_sids: list[int] = []
    try:
        known = set(DBI.list_spectacles())
        for t in titles:
            if t not in known:
                DBI.upsert_spectacle(t)
                sid = DBI.get_spectacle_id(t)
                if sid is not None:
                    unknown_sids.append(sid)
    except Exception:
        unknown_sids = []

    if unknown_sids:
        # Queue unknowns and start step-by-step assignment; postpone final import message
        await state.update_data(
            assign_unknown_sids=unknown_sids,
            assign_idx=0,
            import_inserted=inserted,
            import_year=year,
            import_month=month
        )
        await state.set_state(AssignUnknown.waiting)
        await _send_next_unknown_for_assignment(callback.message, state)
        await callback.answer("Готово")
        return

    # No unknowns — finish import immediately
    await callback.message.answer(f"Импорт завершён: {inserted} записей за {RU_MONTHS[month - 1]} {year}")
    await callback.answer("Готово")

def get_user_busy_inline(user_id: int):
    if not can_show_user_busy_buttons(user_id):
        return InlineKeyboardBuilder().as_markup()
    builder = InlineKeyboardBuilder()
    builder.button(text="Подать даты", callback_data="busy:submit")
    builder.button(text="Посмотреть даты", callback_data="busy:view")
    builder.adjust(1)
    return builder.as_markup()

async def busy_submit_text(message: Message, state: FSMContext):
    eid = await ensure_known_user_or_report_message(message)
    if eid is None:
        return
    _, _, mname = next_month_and_year()
    await state.set_state(BusyInput.waiting_for_add_user)
    await message.answer(f"Введите числа за {mname} через запятую или через дефис для диапазона (пример: 1,3,5-7)")

async def busy_view_text(message: Message, state: FSMContext):
    eid = await ensure_known_user_or_report_message(message)
    if eid is None:
        return
    dates = DBI.list_busy_dates(eid)
    txt = "\n".join(human_ru_date(d) for d in dates) if dates else "пусто"
    await message.answer(f"Ваши даты:\n{txt}", reply_markup=get_user_busy_manage_kb(user_id=message.from_user.id))

async def _notify_admin_busy_change(bot: Bot, employee_id: int, action: str, items: list[str], user: Message | CallbackQuery):
    if not ADMIN_ID:
        return
    with DBI._conn() as con:
        row = con.execute("SELECT display FROM employees WHERE id=?", (employee_id,)).fetchone()
        disp = row[0] if row else str(employee_id)
    who = user.from_user
    payload = ", ".join(items) if items else "—"
    text = f"[BUSY] {action} — {disp}: {payload}\nby: {who.id} @{who.username if who.username else '-'}"
    try:
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        pass

async def handle_busy_add_text(message: Message, state: FSMContext):
    eid = await ensure_known_user_or_report_message(message)
    if eid is None:
        await state.clear(); return
    month, year, _ = next_month_and_year()
    days = parse_days_for_month(message.text, month, year)
    dates = format_busy_dates_for_month(days, month, year)
    added = DBI.add_busy_dates(eid, dates)
    if added:
        DBI.set_submitted(eid, year, month)
        DBI.log_busy(eid, 'add', ','.join(added))
        await _notify_admin_busy_change(message.bot, eid, 'add', added, message)
    await message.answer(f"Добавлено: {', '.join(added) if added else 'ничего нового'}",
                         reply_markup=get_user_busy_reply_kb(message.from_user.id))
    await state.clear()

async def handle_busy_remove_text(message: Message, state: FSMContext):
    eid = await ensure_known_user_or_report_message(message)
    if eid is None:
        await state.clear(); return
    month, year, _ = next_month_and_year()
    raw = (message.text or '').strip().lower()
    if raw in {"очистить", "очистка", "clear"}:
        DBI.clear_busy_dates(eid)
        DBI.log_busy(eid, 'clear', '-')
        # Сбросить факт подачи за этот месяц
        DBI.unset_submitted(eid, year, month)
        await _notify_admin_busy_change(message.bot, eid, 'clear', [], message)
        await message.answer("Все даты удалены.", reply_markup=get_user_busy_reply_kb(message.from_user.id))
        await state.clear(); return
    days = parse_days_for_month(raw, month, year)
    dates = format_busy_dates_for_month(days, month, year)
    removed = DBI.remove_busy_dates(eid, dates)
    if removed:
        DBI.log_busy(eid, 'remove', ','.join(removed))
        await _notify_admin_busy_change(message.bot, eid, 'remove', removed, message)
    # Если за следующий месяц больше не осталось дат — снять флаг подачи
    remaining = [d for d in DBI.list_busy_dates(eid) if d.startswith(f"{year:04d}-{month:02d}-")]
    if not remaining:
        DBI.unset_submitted(eid, year, month)
    await message.answer(f"Удалено: {', '.join(removed) if removed else 'ничего не удалено'}",
                         reply_markup=get_user_busy_reply_kb(message.from_user.id))
    await state.clear()

async def admin_busy_panel(message: Message):
    if not ADMIN_ID or str(message.from_user.id) != str(ADMIN_ID):
        return
    m, y, mname = next_month_and_year()
    submitted = []
    missing = []
    for eid, disp in DBI.list_all_employees():
        if DBI.has_submitted(eid, y, m):
            submitted.append(disp)
        else:
            missing.append(disp)
    text = [f"Статус подачи за {mname}:"]
    text.append("\nПодали (" + str(len(submitted)) + "): " + (", ".join(submitted) if submitted else "—"))
    text.append("Не подали (" + str(len(missing)) + "): " + (", ".join(missing) if missing else "—"))
    await message.answer("\n".join(text))

async def admin_map_tg(callback: CallbackQuery):
    if not ADMIN_ID or str(callback.from_user.id) != str(ADMIN_ID):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        _, eid_s, tg_s = (callback.data or '').split(":", 2)
        eid = int(eid_s)
        tg_id = tg_s
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True); return
    DBI.set_employee_tg_by_id(eid, tg_id)
    await callback.answer("Сопоставлено ✅", show_alert=False)

async def busy_submit(callback: CallbackQuery, state: FSMContext):
    row = DBI.get_employee_by_tg(callback.from_user.id)
    if not row:
        if ADMIN_ID:
            u = callback.from_user
            info = (f"Неизвестный пользователь\nID: {u.id}\nИмя: {u.first_name}\nФамилия: {u.last_name}\nUsername: @{u.username if u.username else '-'}")
            kb = InlineKeyboardBuilder()
            for eid, disp in DBI.list_employees_full():
                kb.button(text=disp, callback_data=f"maptg:{eid}:{u.id}")
            kb.adjust(1)
            try:
                await callback.bot.send_message(ADMIN_ID, info, reply_markup=kb.as_markup())
            except Exception:
                pass
        await callback.message.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
        await callback.answer(); return
    _, _, mname = next_month_and_year()
    await state.set_state(BusyInput.waiting_for_add_user)
    await callback.message.answer(f"Введите числа за {mname} через запятую или через дефис для диапазона (пример: 1,3,5-7)")
    await callback.answer()

async def busy_view(callback: CallbackQuery, state: FSMContext):
    row = DBI.get_employee_by_tg(callback.from_user.id)
    if not row:
        if ADMIN_ID:
            u = callback.from_user
            info = (f"Неизвестный пользователь\nID: {u.id}\nИмя: {u.first_name}\nФамилия: {u.last_name}\nUsername: @{u.username if u.username else '-'}")
            kb = InlineKeyboardBuilder()
            for eid, disp in DBI.list_employees_full():
                kb.button(text=disp, callback_data=f"maptg:{eid}:{u.id}")
            kb.adjust(1)
            try:
                await callback.bot.send_message(ADMIN_ID, info, reply_markup=kb.as_markup())
            except Exception:
                pass
        await callback.message.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
        await callback.answer(); return
    eid = row[0]
    dates = DBI.list_busy_dates(eid)
    txt = "\n".join(human_ru_date(d) for d in dates) if dates else "пусто"
    await callback.message.answer(f"Ваши даты:\n{txt}", reply_markup=get_user_busy_manage_kb(user_id=callback.from_user.id))
    await callback.answer()

async def busy_add(callback: CallbackQuery, state: FSMContext):
    row = DBI.get_employee_by_tg(callback.from_user.id)
    if not row:
        await callback.message.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
        await callback.answer(); return
    _, _, mname = next_month_and_year()
    await state.set_state(BusyInput.waiting_for_add_user)
    await callback.message.answer(f"Введите числа за {mname} (пример: 2,4,10-12)")
    await callback.answer()

async def busy_remove(callback: CallbackQuery, state: FSMContext):
    row = DBI.get_employee_by_tg(callback.from_user.id)
    if not row:
        await callback.message.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
        await callback.answer(); return
    await state.set_state(BusyInput.waiting_for_remove_user)
    await callback.message.answer("Введите число для удаления или напишите 'очистить' чтобы удалить все даты")
    await callback.answer()

async def handle_spectacles(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        await message.answer("Только для админа")
        return
    txt = "Выберите спектакль или добавьте новый:" if DBI.list_spectacles() else "Список пуст. Нажмите \"➕ Добавить\"."
    await message.answer(txt, reply_markup=get_spectacles_inline_kb())

async def handle_workers(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        await message.answer("Только для админа")
        return
    txt = "Выберите сотрудника или добавьте нового:" if DBI.list_employees() else "Список пуст. Нажмите \"➕ Добавить\"."
    await message.answer(txt, reply_markup=get_employees_inline_kb())

async def spectacles_menu_router(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    if data == "add_spectacle":
        await state.set_state(AddSpectacle.waiting_for_name)
        await callback.message.answer("Напиши название спектакля", reply_markup=ReplyKeyboardRemove())
        await callback.answer()
    elif data.startswith("title:"):
        title = data.split(":", 1)[1]
        emps = DBI.get_spectacle_employees(title)
        sid = DBI.get_spectacle_id(title)
        kb = InlineKeyboardBuilder()
        if sid is not None:
            kb.button(text="✏️ Изменить", callback_data=f"editstart:{sid}")
            kb.adjust(1)
            await callback.message.answer(
                f"Спектакль: {title}\nСотрудники: {', '.join(emps) if emps else 'нет'}",
                reply_markup=kb.as_markup(),
            )
        else:
            await callback.message.answer(
                f"Спектакль: {title}\nСотрудники: {', '.join(emps) if emps else 'нет'}"
            )
        await callback.answer()
        return
    else:
        await callback.answer("Пожалуйста, выберите спектакль из списка или '➕ Добавить'.", show_alert=True)
        return

async def employees_menu_router(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    if data == "emp:add":
        await state.set_state(AddEmployee.waiting_for_last_name)
        await callback.message.answer("Напиши фамилию сотрудника", reply_markup=ReplyKeyboardRemove())
        await callback.answer()
        return
    if data.startswith("emp:show:"):
        disp = data.split(":", 2)[2]
        with DBI._conn() as con:
            row = con.execute("SELECT id, last_name, first_name, tg_id FROM employees WHERE display=?", (disp,)).fetchone()
        if row:
            eid, ln, fn, tg = row
            tg_text = tg if tg else "—"
            kb = InlineKeyboardBuilder()
            kb.button(text="✏️ Изменить TG ID", callback_data=f"emp:tg:start:{eid}")
            kb.button(text="🗑 Удалить", callback_data=f"emp:del:ask:{eid}")
            kb.button(text="📅 Показать даты", callback_data=f"emp:busy:view:{eid}")
            kb.adjust(1)
            await callback.message.answer(
                f"Сотрудник:\nФамилия: {ln}\nИмя: {fn}\nTelegram ID: {tg_text}",
                reply_markup=kb.as_markup(),
            )
        else:
            await callback.message.answer("Не найден сотрудник")
        await callback.answer()
        return

# === Employee Delete and Edit TG ID handlers ===
async def emp_del_ask(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    # emp:del:ask:<eid>
    try:
        eid = int(data.split(":", 3)[3])
    except Exception:
        await callback.answer("Ошибка", show_alert=True)
        return
    kb = InlineKeyboardBuilder()
    kb.button(text="Да, удалить", callback_data=f"emp:del:yes:{eid}")
    kb.button(text="Отмена", callback_data="emp:del:no")
    kb.adjust(1)
    await callback.message.answer("Точно удалить сотрудника?", reply_markup=kb.as_markup())
    await callback.answer()

async def emp_del_yes(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    # emp:del:yes:<eid>
    try:
        eid = int(data.split(":", 3)[3])
    except Exception:
        await callback.answer("Ошибка", show_alert=True)
        return
    DBI.delete_employee(eid)
    await callback.message.answer("Сотрудник удалён ✅")
    await callback.answer()

async def emp_del_no(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    await callback.answer("Отменено")

async def emp_tg_start(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    # emp:tg:start:<eid>
    try:
        eid = int(data.split(":", 3)[3])
    except Exception:
        await callback.answer("Ошибка", show_alert=True)
        return
    await state.update_data(edit_emp_id=eid)
    await state.set_state(EditEmployeeTg.waiting_for_tg)
    await callback.message.answer("Пришли новый Telegram ID (или напиши 'Пропустить' чтобы отменить, или 'Очистить' чтобы удалить ID)")
    await callback.answer()

async def emp_tg_set_value(message: Message, state: FSMContext):
    data = await state.get_data()
    eid = data.get('edit_emp_id')
    if eid is None:
        await message.answer("Нет выбранного сотрудника. Откройте карточку сотрудника ещё раз.")
        await state.clear()
        return
    raw = (message.text or '').strip()
    if raw.lower() == 'пропустить':
        await message.answer("Изменение отменено.", reply_markup=get_user_busy_reply_kb(message.from_user.id))
        await state.clear()
        return
    if raw.lower() in {'очистить', 'удалить', '-'}:
        DBI.set_employee_tg_by_id(eid, None)
        await message.answer("Telegram ID очищен ✅", reply_markup=get_user_busy_reply_kb(message.from_user.id))
        await state.clear()
        return
    # save new TG id
    DBI.set_employee_tg_by_id(eid, raw)
    await message.answer("Telegram ID обновлён ✅", reply_markup=get_user_busy_reply_kb(message.from_user.id))
    await state.clear()

async def emp_busy_view(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    try:
        eid = int(data.split(":", 3)[3])
    except Exception:
        await callback.answer("Ошибка", show_alert=True)
        return
    dates = DBI.list_busy_dates(eid)
    txt = "\n".join(human_ru_date(d) for d in dates) if dates else "пусто"
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить", callback_data=f"empbusy:add:{eid}")
    kb.button(text="➖ Убрать", callback_data=f"empbusy:remove:{eid}")
    kb.adjust(2)
    await callback.message.answer(f"Даты сотрудника: {txt}", reply_markup=kb.as_markup())
    await callback.answer()

async def emp_busy_add_start(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    eid = int(callback.data.split(":", 2)[2])
    await state.update_data(admin_target_eid=eid)
    _, _, mname = next_month_and_year()
    await state.set_state(AdminBusyInput.waiting_for_add)
    await callback.message.answer(f"Введите числа за {mname} (пример: 2,4,10-12)")
    await callback.answer()

async def emp_busy_remove_start(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    eid = int(callback.data.split(":", 2)[2])
    await state.update_data(admin_target_eid=eid)
    await state.set_state(AdminBusyInput.waiting_for_remove)
    await callback.message.answer("Введите число/диапазон для удаления или 'очистить' чтобы удалить все даты")
    await callback.answer()

async def edit_employees_start(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    # editstart:<sid>
    try:
        sid = int(data.split(":", 1)[1])
    except Exception:
        await callback.answer("Ошибка формата", show_alert=True)
        return
    await callback.message.answer("Изменение списка сотрудников:", reply_markup=get_edit_employees_inline_kb(sid))
    await callback.answer()

async def edit_employees_toggle(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    # edittoggle:<sid>:<eid>
    try:
        _, sid_s, eid_s = data.split(":", 2)
        sid = int(sid_s); eid = int(eid_s)
    except Exception:
        await callback.answer("Ошибка формата", show_alert=True)
        return
    DBI.toggle_spectacle_employee(sid, eid)
    # re-render keyboard with updated checkmarks
    await callback.message.edit_reply_markup(reply_markup=get_edit_employees_inline_kb(sid))
    await callback.answer()

async def edit_employees_done(callback: CallbackQuery, state: FSMContext):
    if not _is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = callback.data or ""
    # editdone:<sid>
    try:
        sid = int(data.split(":", 1)[1])
    except Exception:
        await callback.answer("Готово")
        return
    with DBI._conn() as con:
        row = con.execute("SELECT title FROM spectacles WHERE id=?", (sid,)).fetchone()
        title = row[0] if row else "Спектакль"
        rows = con.execute(
            """
            SELECT e.display FROM spectacle_employees se
            JOIN employees e ON e.id = se.employee_id
            WHERE se.spectacle_id=?
            ORDER BY e.last_name, e.first_name
            """,
            (sid,),
        ).fetchall()
        final_list = ", ".join(r[0] for r in rows) if rows else "нет"
    await callback.message.answer(f"Сохранено. {title}: {final_list}")

    # If in sequential assignment flow, move to next unknown
    st = await state.get_data()
    sids = st.get('assign_unknown_sids')
    if sids is not None:
        try:
            idx = int(st.get('assign_idx', 0)) + 1
        except Exception:
            idx = 1
        if idx < len(sids):
            await state.update_data(assign_idx=idx)
            await _send_next_unknown_for_assignment(callback.message, state)
            await callback.answer()
            return
        else:
            # Finished all unknowns — send final import summary and clear state
            inserted = st.get('import_inserted', 0)
            year = st.get('import_year')
            month = st.get('import_month')
            await state.clear()
            try:
                await callback.message.answer(f"Импорт завершён: {inserted} записей за {RU_MONTHS[int(month)-1]} {int(year)}")
            except Exception:
                await callback.message.answer("Импорт завершён")
            await callback.answer()
            return

    await callback.answer()

async def add_spectacle_name(message: Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await message.answer("Название не может быть пустым. Введите ещё раз.")
        return
    DBI.upsert_spectacle(name)
    await state.update_data(name=name, employees=[])
    await state.set_state(AddSpectacle.waiting_for_employees)
    await message.answer(
        "Выберите сотрудников для спектакля (можно несколько):",
        reply_markup=get_employees_kb()
    )

async def add_spectacle_employees(message: Message, state: FSMContext):
    data = await state.get_data()
    selected = data.get("employees", [])
    txt = message.text.strip()
    if not DBI.list_employees():
        await message.answer("Сначала добавьте сотрудников в разделе ‘Сотрудники’.", reply_markup=get_user_busy_reply_kb(message.from_user.id))
        await state.clear()
        return
    if txt == "✅ Готово":
        name = data.get("name")
        DBI.set_spectacle_employees(name, selected)
        await state.clear()
        await message.answer(f"Сохранено!\nСпектакль «{name}» добавлен.", reply_markup=get_user_busy_reply_kb(message.from_user.id))
        return
    # Remove checkmark if present
    emp = txt.replace("✅ ", "")
    if emp not in DBI.list_employees():
        await message.answer("Выберите сотрудника кнопкой или нажмите '✅ Готово'.", reply_markup=get_employees_kb(selected))
        return
    if emp in selected:
        selected.remove(emp)
    else:
        selected.append(emp)
    await state.update_data(employees=selected)
    await message.answer(
        f"Сотрудники: {', '.join(selected) if selected else 'нет'}",
        reply_markup=get_employees_kb(selected)
    )

async def add_employee_last_name(message: Message, state: FSMContext):
    ln = (message.text or '').strip()
    if not ln:
        await message.answer("Фамилия обязательна. Введите фамилию ещё раз.")
        return
    await state.update_data(last_name=ln)
    await state.set_state(AddEmployee.waiting_for_first_name)
    await message.answer("Теперь напиши имя сотрудника")

async def add_employee_first_name(message: Message, state: FSMContext):
    fn = (message.text or '').strip()
    if not fn:
        await message.answer("Имя обязательное. Введите имя ещё раз.")
        return
    await state.update_data(first_name=fn)
    await state.set_state(AddEmployee.waiting_for_tg_id)
    await message.answer("Отправь Telegram ID (или напиши \"Пропустить\")")

async def add_employee_tg(message: Message, state: FSMContext):
    data = await state.get_data()
    ln = data.get('last_name')
    fn = data.get('first_name')
    tg_raw = (message.text or '').strip()
    tg_id = None if tg_raw.lower() == 'пропустить' else tg_raw
    # save
    DBI.upsert_employee(ln, fn, tg_id)
    await state.clear()
    await message.answer(f"Сотрудник сохранён: {ln} {fn}", reply_markup=get_user_busy_reply_kb(message.from_user.id))
async def admin_handle_busy_add_text(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        await message.answer("Только для админа")
        await state.clear(); return
    data = await state.get_data()
    eid = data.get('admin_target_eid')
    if not eid:
        await message.answer("Нет выбранного сотрудника.")
        await state.clear(); return
    month, year, _ = next_month_and_year()
    days = parse_days_for_month(message.text, month, year)
    dates = format_busy_dates_for_month(days, month, year)
    added = DBI.add_busy_dates(eid, dates)
    if added:
        DBI.set_submitted(eid, year, month)
        DBI.log_busy(eid, 'add', ','.join(added))
        await _notify_admin_busy_change(message.bot, eid, 'add', added, message)
    await message.answer(f"Добавлено: {', '.join(added) if added else 'ничего нового'}")
    await state.clear()


async def admin_handle_busy_remove_text(message: Message, state: FSMContext):
    if not _is_admin(message.from_user.id):
        await message.answer("Только для админа")
        await state.clear(); return
    data = await state.get_data()
    eid = data.get('admin_target_eid')
    if not eid:
        await message.answer("Нет выбранного сотрудника.")
        await state.clear(); return
    month, year, _ = next_month_and_year()
    raw = (message.text or '').strip().lower()
    if raw in {"очистить", "очистка", "clear"}:
        DBI.clear_busy_dates(eid)
        DBI.log_busy(eid, 'clear', '-')
        DBI.unset_submitted(eid, year, month)
        await _notify_admin_busy_change(message.bot, eid, 'clear', [], message)
        await message.answer("Все даты удалены.")
        await state.clear(); return
    days = parse_days_for_month(raw, month, year)
    dates = format_busy_dates_for_month(days, month, year)
    removed = DBI.remove_busy_dates(eid, dates)
    if removed:
        DBI.log_busy(eid, 'remove', ','.join(removed))
        await _notify_admin_busy_change(message.bot, eid, 'remove', removed, message)
    remaining = [d for d in DBI.list_busy_dates(eid) if d.startswith(f"{year:04d}-{month:02d}-")]
    if not remaining:
        DBI.unset_submitted(eid, year, month)
    await message.answer(f"Удалено: {', '.join(removed) if removed else 'ничего не удалено'}")
    await state.clear()
# ====== Entrypoint ======
async def monthly_broadcast_task(bot: Bot):
    # runs forever; checks hourly
    while True:
        try:
            today = date.today()
            next_m, next_y, mname = next_month_and_year(today)
            DBI.ensure_window(next_y, next_m)
            wnd = DBI.get_window(next_y, next_m)
            sent = wnd[3] if wnd else 0
            if today.day == 1 and not sent:
                # send to all with tg_id
                for eid, disp, tg in DBI.list_employees_with_tg():
                    try:
                        await bot.send_message(tg, f"{disp}, пришлите занятые даты за {mname}", reply_markup=get_user_busy_reply_kb(tg))
                    except Exception:
                        continue
                DBI.mark_broadcast_sent(next_y, next_m)
        except Exception:
            pass
        await asyncio.sleep(3600)

async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("Не указан BOT_TOKEN (добавьте его в .env)")

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(cmd_start, CommandStart())
    dp.message.register(handle_spectacles, F.text.lower() == "спектакли")
    dp.message.register(handle_workers, F.text.lower() == "сотрудники")
    dp.message.register(handle_auto_assign, F.text.regexp(r"(?i)^автоназначение"))
    dp.message.register(handle_make_schedule, F.text.lower() == "сделать график")
    # Inline callbacks (precise filters)
    dp.callback_query.register(edit_employees_toggle, F.data.startswith('edittoggle:'))
    dp.callback_query.register(edit_employees_done,   F.data.startswith('editdone:'))
    dp.callback_query.register(edit_employees_start,  F.data.startswith('editstart:'))
    dp.callback_query.register(spectacles_menu_router, (F.data == 'add_spectacle') | F.data.startswith('title:'))
    dp.callback_query.register(emp_del_yes,  F.data.startswith('emp:del:yes:'))
    dp.callback_query.register(emp_del_ask,  F.data.startswith('emp:del:ask:'))
    dp.callback_query.register(emp_del_no,   F.data == 'emp:del:no')
    dp.callback_query.register(emp_tg_start, F.data.startswith('emp:tg:start:'))
    dp.callback_query.register(employees_menu_router,  (F.data == 'emp:add') | F.data.startswith('emp:show:'))
    # Состояния FSM
    dp.message.register(add_spectacle_name, AddSpectacle.waiting_for_name)
    dp.message.register(add_spectacle_employees, AddSpectacle.waiting_for_employees)
    # AddEmployee FSM
    dp.message.register(add_employee_last_name, AddEmployee.waiting_for_last_name)
    dp.message.register(add_employee_first_name, AddEmployee.waiting_for_first_name)
    dp.message.register(add_employee_tg, AddEmployee.waiting_for_tg_id)

    # EditEmployeeTg FSM
    dp.message.register(emp_tg_set_value, EditEmployeeTg.waiting_for_tg)

    # Register admin panel and busy flow
    dp.message.register(admin_busy_panel, F.text.lower() == "busy_admin")
    dp.callback_query.register(admin_map_tg, F.data.startswith('maptg:'))
    dp.callback_query.register(busy_submit, F.data == 'busy:submit')
    dp.callback_query.register(busy_view,   F.data == 'busy:view')
    dp.callback_query.register(busy_add,    F.data == 'busy:add')
    dp.callback_query.register(busy_remove, F.data == 'busy:remove')
    dp.message.register(handle_busy_add_text,    BusyInput.waiting_for_add_user)
    dp.message.register(handle_busy_remove_text, BusyInput.waiting_for_remove_user)
    dp.message.register(busy_submit_text, F.text.regexp(r"^Подать даты за "))
    dp.message.register(busy_view_text,   F.text.lower() == "посмотреть свои даты")
    dp.callback_query.register(emp_busy_view,         F.data.startswith('emp:busy:view:'))
    dp.callback_query.register(emp_busy_add_start,    F.data.startswith('empbusy:add:'))
    dp.callback_query.register(emp_busy_remove_start, F.data.startswith('empbusy:remove:'))

    dp.message.register(admin_handle_busy_add_text,    AdminBusyInput.waiting_for_add)
    dp.message.register(admin_handle_busy_remove_text, AdminBusyInput.waiting_for_remove)

    # Excel upload handlers
    dp.message.register(handle_excel_upload, F.document)
    dp.callback_query.register(handle_excel_month_pick, F.data.startswith('xlsmonth:'))
    dp.callback_query.register(handle_make_schedule_pick, F.data.startswith('mkmonth:') | F.data.startswith('xlsmonth:'))

    # background monthly broadcast
    asyncio.create_task(monthly_broadcast_task(bot))

    print("Bot is running… Press Ctrl+C to stop.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
