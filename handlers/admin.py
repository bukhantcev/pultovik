# handlers/admin.py
import asyncio
from datetime import date
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from config import is_admin
from db import DBI
from keyboards.reply import get_user_busy_reply_kb
from services.auto_assign import auto_assign_events_for_month
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

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

class NewAuthEmployee(StatesGroup):
    waiting_for_last_name = State()
    waiting_for_first_name = State()

# ===== Authorization (admin side) =====
async def auth_list_employees(callback: CallbackQuery):
    # admin only
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ""
    # format: auth:list:<tg>
    try:
        _, _, tg_s = data.split(":", 2)
        target_tg = int(tg_s)
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True); return

    # собрать список сотрудников
    rows = DBI.list_all_employees()
    if not rows:
        kb = InlineKeyboardBuilder()
        kb.button(text="➕ Новый сотрудник", callback_data=f"auth:new:{target_tg}")
        kb.button(text="Отклонить", callback_data=f"auth:deny:{target_tg}")
        kb.adjust(1)
        await callback.message.answer("Нет сотрудников в базе. Добавить нового?", reply_markup=kb.as_markup())
        await callback.answer(); return

    kb = InlineKeyboardBuilder()
    for eid, disp in rows:
        kb.button(text=disp, callback_data=f"auth:approve:{eid}:{target_tg}")
    # создать нового и сразу привязать к этому TG
    kb.button(text="➕ Новый сотрудник", callback_data=f"auth:new:{target_tg}")
    kb.button(text="Отклонить", callback_data=f"auth:deny:{target_tg}")
    kb.adjust(1)
    await callback.message.answer(
        f"Выберите сотрудника для TG {target_tg}", reply_markup=kb.as_markup()
    )
    await callback.answer()

async def auth_approve(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ""
    # format: auth:approve:<eid>:<tg>
    try:
        _, _, eid_s, tg_s = data.split(":", 3)
        eid = int(eid_s); target_tg = int(tg_s)
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True); return

    # Привязываем TG к сотруднику
    DBI.set_employee_tg_by_id(eid, str(target_tg))
    DBI.delete_pending_auth(target_tg)

    # Узнаем отображаемое имя
    with DBI._conn() as con:
        row = con.execute("SELECT display FROM employees WHERE id=?", (eid,)).fetchone()
    disp = row[0] if row else "сотрудник"

    # Уведомляем пользователя
    try:
        await callback.bot.send_message(
            chat_id=target_tg,
            text=f"Вы авторизованы как: {disp}",
            reply_markup=get_user_busy_reply_kb(target_tg)
        )
    except Exception:
        pass

    await callback.message.answer(f"Авторизация подтверждена: {disp} (TG {target_tg})")
    await callback.answer("Готово")

async def auth_deny(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ""
    # format: auth:deny:<tg>
    try:
        _, _, tg_s = data.split(":", 2)
        target_tg = int(tg_s)
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True); return

    DBI.delete_pending_auth(target_tg)
    # Уведомляем пользователя
    try:
        await callback.bot.send_message(chat_id=target_tg, text="Авторизация отклонена. Обратитесь к администратору.")
    except Exception:
        pass

    await callback.message.answer(f"Заявка отклонена (TG {target_tg})")
    await callback.answer("Отклонено")

async def auth_new_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ""
    # format: auth:new:<tg>
    try:
        _, _, tg_s = data.split(":", 2)
        target_tg = int(tg_s)
    except Exception:
        await callback.answer("Ошибка данных", show_alert=True); return
    await state.update_data(auth_new_tg=target_tg)
    await state.set_state(NewAuthEmployee.waiting_for_last_name)
    await callback.message.answer("Фамилия нового сотрудника:")
    await callback.answer()

async def auth_new_last_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); return
    ln = (message.text or '').strip()
    if not ln:
        await message.answer("Фамилия обязательна. Введите ещё раз.")
        return
    await state.update_data(new_emp_ln=ln)
    await state.set_state(NewAuthEmployee.waiting_for_first_name)
    await message.answer("Имя нового сотрудника:")

async def auth_new_first_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); return
    data = await state.get_data()
    ln = (data.get('new_emp_ln') or '').strip()
    fn = (message.text or '').strip()
    if not fn:
        await message.answer("Имя обязательно. Введите ещё раз.")
        return
    target_tg = data.get('auth_new_tg')
    try:
        # создаём и сразу привязываем TG
        DBI.upsert_employee(ln, fn, str(target_tg))
        try:
            DBI.delete_pending_auth(target_tg)
        except Exception:
            pass
        disp = f"{ln} {fn}".strip()
        # уведомляем пользователя
        try:
            await message.bot.send_message(
                chat_id=target_tg,
                text=f"Вы авторизованы как: {disp}",
                reply_markup=get_user_busy_reply_kb(target_tg)
            )
        except Exception:
            pass
        await message.answer(f"Создан сотрудник и привязан к TG {target_tg}: {disp}")
    except Exception as e:
        await message.answer(f"Ошибка создания сотрудника: {e}")
    await state.clear()