# handlers/admin.py
import asyncio
from datetime import date
from aiogram.types import Message
from config import is_admin
from db import DBI
from keyboards.reply import get_user_busy_reply_kb
from services.auto_assign import auto_assign_events_for_month

async def handle_auto_assign(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); return
    import re
    m = re.search(r"(?i)^автоназначение\\s*(\\d{4})-(\\d{2})\\s*$", message.text or "")
    if m:
        y = int(m.group(1)); mo = int(m.group(2))
        updated = auto_assign_events_for_month(y, mo)
        await message.answer(f"Автоназначение за {mo:02d}.{y}: обновлено {updated}")
    else:
        updated = auto_assign_events_for_month()
        await message.answer(f"Автоназначение (все события): обновлено {updated}")

async def monthly_broadcast_task(bot):
    while True:
        try:
            today = date.today()
            m = today.month + 1
            y = today.year + (1 if m == 13 else 0)
            m = 1 if m == 13 else m
            DBI.ensure_window(y, m)
            wnd = DBI.get_window(y, m)
            sent = wnd[3] if wnd else 0
            if today.day == 1 and not sent:
                for eid, disp, tg in DBI.list_employees_with_tg():
                    try:
                        await bot.send_message(tg, f"{disp}, пришлите занятые даты за {['Январь','Февраль','Март','Апрель','Май','Июнь','Июль','Август','Сентябрь','Октябрь','Ноябрь','Декабрь'][m-1]}", reply_markup=get_user_busy_reply_kb(tg))
                    except Exception:
                        continue
                DBI.mark_broadcast_sent(y, m)
        except Exception:
            pass
        await asyncio.sleep(3600)