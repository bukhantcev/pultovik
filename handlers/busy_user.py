# handlers/busy_user.py
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from keyboards.inline import get_user_busy_manage_kb
from keyboards.reply import get_user_busy_reply_kb
from db import DBI
from utils.dates import next_month_and_year, parse_days_for_month, format_busy_dates_for_month, human_ru_date
from services.busy_flow import ensure_known_user_or_report_message, notify_admin_busy_change
from datetime import date
from config import ADMIN_ID
from aiogram.utils.keyboard import InlineKeyboardBuilder

class BusyInput(StatesGroup):
    waiting_for_add_user = State()
    waiting_for_remove_user = State()

def _is_admin(user_id: int | None) -> bool:
    return user_id is not None and str(user_id) == str(ADMIN_ID)

def _after_25_for_non_admin(user_id: int | None) -> bool:
    try:
        return (not _is_admin(user_id)) and date.today().day >= 25
    except Exception:
        return False

async def busy_submit_text(message: Message, state: FSMContext):
    eid = await ensure_known_user_or_report_message(message)
    if eid is None: return
    if _after_25_for_non_admin(message.from_user.id):
        await message.answer("Подать даты можно с 1 по 25 числа.")
        return
    _, _, mname = next_month_and_year()
    await state.set_state(BusyInput.waiting_for_add_user)
    await message.answer(f"Введите числа за {mname} через запятую или через дефис для диапазона (пример: 1,3,5-7)")

async def busy_view_text(message: Message, state: FSMContext):
    eid = await ensure_known_user_or_report_message(message)
    if eid is None: return
    dates = DBI.list_busy_dates(eid)
    txt = "\n".join(human_ru_date(d) for d in dates) if dates else "пусто"
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить", callback_data="busy:add")
    kb.button(text="➖ Убрать", callback_data="busy:remove")
    kb.adjust(2)
    await message.answer(f"Ваши даты:\n{txt}", reply_markup=kb.as_markup())

async def busy_submit(callback: CallbackQuery, state: FSMContext):
    row = DBI.get_employee_by_tg(callback.from_user.id)
    if not row:
        await callback.message.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
        await callback.answer(); return
    if _after_25_for_non_admin(callback.from_user.id):
        await callback.message.answer("Подать даты можно с 1 по 25 числа.")
        await callback.answer()
        return
    _, _, mname = next_month_and_year()
    await state.set_state(BusyInput.waiting_for_add_user)
    await callback.message.answer(f"Введите числа за {mname} через запятую или через дефис (пример: 2,4,10-12)")
    await callback.answer()

async def busy_view(callback: CallbackQuery, state: FSMContext):
    row = DBI.get_employee_by_tg(callback.from_user.id)
    if not row:
        await callback.message.answer("Неизвестный пользователь. Администратор сопоставит ваш аккаунт.")
        await callback.answer(); return
    eid = row[0]
    dates = DBI.list_busy_dates(eid)
    txt = "\n".join(human_ru_date(d) for d in dates) if dates else "пусто"
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить", callback_data="busy:add")
    kb.button(text="➖ Убрать", callback_data="busy:remove")
    kb.adjust(2)
    await callback.message.answer(f"Ваши даты:\n{txt}", reply_markup=kb.as_markup())
    await callback.answer()

async def handle_busy_add_text(message: Message, state: FSMContext):
    eid = await ensure_known_user_or_report_message(message)
    if eid is None: await state.clear(); return
    if _after_25_for_non_admin(message.from_user.id):
        await message.answer("Подать даты можно с 1 по 25 числа.", reply_markup=get_user_busy_reply_kb(message.from_user.id))
        await state.clear()
        return
    month, year, _ = next_month_and_year()
    days = parse_days_for_month(message.text, month, year)
    dates = format_busy_dates_for_month(days, month, year)
    added = DBI.add_busy_dates(eid, dates)
    if added:
        DBI.set_submitted(eid, year, month)
        await notify_admin_busy_change(message.bot, eid, 'add', added, message)
    await message.answer(f"Добавлено: {', '.join(added) if added else 'ничего нового'}", reply_markup=get_user_busy_reply_kb(message.from_user.id))
    await state.clear()

async def handle_busy_remove_text(message: Message, state: FSMContext):
    eid = await ensure_known_user_or_report_message(message)
    if eid is None: await state.clear(); return
    month, year, _ = next_month_and_year()
    raw = (message.text or '').strip().lower()
    if raw in {"очистить","очистка","clear"}:
        DBI.clear_busy_dates(eid)
        DBI.unset_submitted(eid, year, month)
        await notify_admin_busy_change(message.bot, eid, 'clear', [], message)
        await message.answer("Все даты удалены.", reply_markup=get_user_busy_reply_kb(message.from_user.id))
        await state.clear(); return
    days = parse_days_for_month(raw, month, year)
    dates = format_busy_dates_for_month(days, month, year)
    removed = DBI.remove_busy_dates(eid, dates)
    if removed:
        await notify_admin_busy_change(message.bot, eid, 'remove', removed, message)
    remaining = [d for d in DBI.list_busy_dates(eid) if d.startswith(f"{year:04d}-{month:02d}-")]
    if not remaining:
        DBI.unset_submitted(eid, year, month)
    await message.answer(f"Удалено: {', '.join(removed) if removed else 'ничего не удалено'}", reply_markup=get_user_busy_reply_kb(message.from_user.id))
    await state.clear()


# New handlers for busy add/remove via callback
async def busy_add(callback: CallbackQuery, state: FSMContext):
    """Начать ввод занятых дат пользователем (кнопка "➕ Добавить")."""
    if _after_25_for_non_admin(callback.from_user.id):
        await callback.message.answer("Подать даты можно с 1 по 25 числа.")
        await callback.answer()
        return
    try:
        _, _, mname = next_month_and_year()
    except Exception:
        mname = "следующий месяц"
    await state.set_state(BusyInput.waiting_for_add_user)
    await callback.message.answer(
        f"Введите числа за {mname} через запятую или через дефис для диапазона (пример: 1,3,5-7)"
    )
    await callback.answer()

async def busy_remove(callback: CallbackQuery, state: FSMContext):
    """Начать удаление занятых дат пользователем (кнопка "➖ Убрать")."""
    if _after_25_for_non_admin(callback.from_user.id):
        await callback.message.answer("Редактировать даты можно с 1 по 25 числа.")
        await callback.answer()
        return
    await state.set_state(BusyInput.waiting_for_remove_user)
    await callback.message.answer(
        "Введите число для удаления или напишите 'очистить' чтобы удалить все даты"
    )
    await callback.answer()