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

# ====== Config ======
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

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

# ====== UI helpers ======
def get_employees_kb(selected=None):
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
    await message.answer("Выберите раздел:", reply_markup=MAIN_KB)

async def handle_spectacles(message: Message, state: FSMContext):
    txt = "Выберите спектакль или добавьте новый:" if DBI.list_spectacles() else "Список пуст. Нажмите \"➕ Добавить\"."
    await message.answer(txt, reply_markup=get_spectacles_inline_kb())

async def handle_workers(message: Message, state: FSMContext):
    txt = "Выберите сотрудника или добавьте нового:" if DBI.list_employees() else "Список пуст. Нажмите \"➕ Добавить\"."
    await message.answer(txt, reply_markup=get_employees_inline_kb())

async def spectacles_menu_router(callback: CallbackQuery, state: FSMContext):
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
    await callback.answer("Отменено")

async def emp_tg_start(callback: CallbackQuery, state: FSMContext):
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
        await message.answer("Изменение отменено.", reply_markup=MAIN_KB)
        await state.clear()
        return
    if raw.lower() in {'очистить', 'удалить', '-'}:
        DBI.set_employee_tg_by_id(eid, None)
        await message.answer("Telegram ID очищен ✅", reply_markup=MAIN_KB)
        await state.clear()
        return
    # save new TG id
    DBI.set_employee_tg_by_id(eid, raw)
    await message.answer("Telegram ID обновлён ✅", reply_markup=MAIN_KB)
    await state.clear()

async def edit_employees_start(callback: CallbackQuery, state: FSMContext):
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
        await message.answer("Сначала добавьте сотрудников в разделе ‘Сотрудники’.", reply_markup=MAIN_KB)
        await state.clear()
        return
    if txt == "✅ Готово":
        name = data.get("name")
        DBI.set_spectacle_employees(name, selected)
        await state.clear()
        await message.answer(f"Сохранено!\nСпектакль «{name}» добавлен.", reply_markup=MAIN_KB)
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
    await message.answer(f"Сотрудник сохранён: {ln} {fn}", reply_markup=MAIN_KB)

# ====== Entrypoint ======
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
    dp.callback_query.register(employees_menu_router,  F.data.startswith('emp:'))
    # Состояния FSM
    dp.message.register(add_spectacle_name, AddSpectacle.waiting_for_name)
    dp.message.register(add_spectacle_employees, AddSpectacle.waiting_for_employees)
    # AddEmployee FSM
    dp.message.register(add_employee_last_name, AddEmployee.waiting_for_last_name)
    dp.message.register(add_employee_first_name, AddEmployee.waiting_for_first_name)
    dp.message.register(add_employee_tg, AddEmployee.waiting_for_tg_id)

    # EditEmployeeTg FSM
    dp.message.register(emp_tg_set_value, EditEmployeeTg.waiting_for_tg)

    print("Bot is running… Press Ctrl+C to stop.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")
