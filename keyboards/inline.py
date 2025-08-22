# keyboards/inline.py
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import RU_MONTHS
from db import DBI
from datetime import date

def get_spectacles_inline_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    # ultra-short callback data to fit Telegram's 64-byte limit
    for sid, name in DBI.list_spectacles_with_ids():
        if sid is None:
            continue
        cb = f"t:{int(sid)}"
        # Guard against invalid payloads
        if not cb or len(cb.encode('utf-8')) > 64:
            continue
        b.button(text=str(name), callback_data=cb)
    b.button(text="➕ Добавить", callback_data="add_spectacle")
    b.adjust(1)
    return b.as_markup()

def get_employees_inline_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    for disp in DBI.list_employees():
        b.button(text=disp, callback_data=f"emp:show:{disp}")
    b.button(text="➕ Добавить", callback_data="emp:add")
    b.adjust(1)
    return b.as_markup()

def get_edit_employees_inline_kb(sid: int) -> InlineKeyboardMarkup:
    with DBI._conn() as con:
        rows = con.execute("SELECT employee_id FROM spectacle_employees WHERE spectacle_id=?", (sid,)).fetchall()
        current = {r[0] for r in rows}
    b = InlineKeyboardBuilder()
    for eid, disp in DBI.list_employees_full():
        mark = "✅ " if eid in current else ""
        b.button(text=f"{mark}{disp}", callback_data=f"edittoggle:{sid}:{eid}")
    b.button(text="✅ Готово", callback_data=f"editdone:{sid}")
    b.adjust(1)
    return b.as_markup()


def get_spectacle_info_kb(sid: int) -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="✏️ Изменить", callback_data=f"edit_spectacle:{sid}")
    b.button(text="🗑 Удалить", callback_data=f"del_spectacle:{sid}")
    b.adjust(1)
    return b.as_markup()

def get_user_busy_manage_kb() -> InlineKeyboardMarkup:
    b = InlineKeyboardBuilder()
    b.button(text="➕ Добавить", callback_data="busy:add")
    b.button(text="➖ Убрать", callback_data="busy:remove")
    b.adjust(2)
    return b.as_markup()

def get_month_pick_inline(today: date | None = None, prefix: str = "xlsmonth:") -> InlineKeyboardMarkup:
    d = today or date.today()
    rows = []
    for i in range(3):
        m = d.month + i
        y = d.year
        if m > 12:
            m -= 12
            y += 1
        rows.append([InlineKeyboardButton(text=f"{RU_MONTHS[m-1]} {y}", callback_data=f"{prefix}{y:04d}-{m:02d}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)