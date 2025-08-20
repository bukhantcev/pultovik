import asyncio
import os
import sqlite3
from typing import List
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from dotenv import load_dotenv
from datetime import datetime, date, UTC

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
def _is_admin(user_id: int) -> bool:
    return bool(ADMIN_ID) and str(user_id) == str(ADMIN_ID)

def can_show_user_busy_buttons(user_id: int, today: date | None = None) -> bool:
    d = today or date.today()
    # обычным пользователям inline/reply-кнопки показываем до 25-го, админу — всегда
    return _is_admin(user_id) or d.day < 25

def get_user_busy_reply_kb(user_id: int) -> ReplyKeyboardMarkup:
    # Главные кнопки доступны только админу
    base_rows = [[KeyboardButton(text="Спектакли"), KeyboardButton(text="Сотрудники")]] if _is_admin(user_id) else []

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
    txt = ", ".join(dates) if dates else "пусто"
    await message.answer(f"Ваши даты: {txt}", reply_markup=get_user_busy_manage_kb(user_id=message.from_user.id))

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
    txt = ", ".join(dates) if dates else "пусто"
    await callback.message.answer(f"Ваши даты: {txt}", reply_markup=get_user_busy_manage_kb(user_id=callback.from_user.id))
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
    txt = ", ".join(dates) if dates else "пусто"
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

    # background monthly broadcast
    asyncio.create_task(monthly_broadcast_task(bot))

    print("Bot is running… Press Ctrl+C to stop.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
