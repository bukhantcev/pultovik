# services/busy_flow.py
from typing import Union
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from db import DBI
from config import ADMIN_ID

async def ensure_known_user_or_report_message(event: Union[Message, CallbackQuery]) -> int | None:
    """
    Works for both Message and CallbackQuery:
    - If user is known, returns employee_id.
    - If unknown, notifies admin with mapping buttons + '➕ Добавить сотрудника'
      and informs the user.
    """
    # Common accessors
    user = event.from_user
    bot = event.bot

    row = DBI.get_employee_by_tg(user.id)
    if row:
        return row[0]

    if ADMIN_ID:
        info = (
            f"Неизвестный пользователь\n"
            f"ID: {user.id}\n"
            f"Имя: {user.first_name}\n"
            f"Фамилия: {user.last_name}\n"
            f"Username: @{user.username if user.username else '-'}"
        )
        kb = InlineKeyboardBuilder()
        for eid, disp in DBI.list_employees_full():
            kb.button(text=disp, callback_data=f"maptg:{eid}:{user.id}")
        kb.button(text="➕ Добавить сотрудника", callback_data=f"emp:add_unknown:{user.id}")
        kb.adjust(1)
        try:
            await bot.send_message(ADMIN_ID, info, reply_markup=kb.as_markup())
        except Exception:
            pass

    # Answer to the user depending on event type
    try:
        if isinstance(event, Message):
            await event.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
        else:
            await event.message.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
    except Exception:
        pass

    return None

async def notify_admin_busy_change(bot, employee_id: int, action: str, items: list[str], user: Message | CallbackQuery):
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