# services/busy_flow.py
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from db import DBI
from config import ADMIN_ID

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