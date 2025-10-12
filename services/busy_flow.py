# services/busy_flow.py
from typing import Union
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from db import DBI
from config import ADMIN_ID
from datetime import date
from utils.dates import next_month_and_year

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

# --- PERIODIC REMINDERS (12th and 24th of each month) ---
_last_reminder_stamp: tuple[int, int, int] | None = None  # (YYYY, MM, DD)

async def monthly_reminders_task(bot):
    """
    Periodic background task: on the 12th and 24th of each month
    reminds employees (who haven't submitted yet) to provide busy dates
    for the *next* month (the same logic we use elsewhere).
    Runs forever, sleeping ~1 hour between checks.
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    global _last_reminder_stamp
    while True:
        try:
            now = datetime.now(ZoneInfo("Europe/Moscow"))

            # Run reminder exactly at 12:00 Moscow time
            if now.hour == 12 and now.minute == 0:
                today = now.date()
                # Fire only on 12th or 24th and only once per calendar day
                if today.day in (12, 24):
                    stamp = (today.year, today.month, today.day)
                    if _last_reminder_stamp != stamp:
                        # Next month/year & name (e.g., "Октябрь")
                        next_m, next_y, mname = next_month_and_year(today)

                        # Who hasn't submitted yet for next month
                        to_notify = []
                        try:
                            rows = DBI.list_employees_with_tg()  # [(id, display, tg_id_int)]
                            for eid, disp, tg in rows:
                                try:
                                    if not DBI.has_submitted(eid, next_y, next_m):
                                        to_notify.append((eid, disp, tg))
                                except Exception:
                                    continue
                        except Exception:
                            rows = []
                            to_notify = []

                        if to_notify:
                            # Day-specific phrasing
                            suffix = " Сегодня последний день!" if today.day == 24 else ""
                            for _, disp, tg in to_notify:
                                try:
                                    text = f"{disp}, напомню: пришлите занятые даты за {mname} до 25 числа.{suffix}"
                                    await bot.send_message(tg, text)
                                except Exception:
                                    # ignore individual delivery errors
                                    pass

                        _last_reminder_stamp = stamp
        except Exception:
            # swallow errors to keep the task alive
            pass

        # Check roughly hourly
        try:
            import asyncio
            await asyncio.sleep(3600)
        except Exception:
            # If event loop is shutting down, just break
            break