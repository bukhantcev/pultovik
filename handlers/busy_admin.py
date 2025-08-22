# handlers/busy_admin.py
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from config import is_admin
from db import DBI
from utils.dates import next_month_and_year, parse_days_for_month, format_busy_dates_for_month, human_ru_date

class AdminBusyInput(StatesGroup):
    waiting_for_add = State()
    waiting_for_remove = State()

async def admin_busy_panel(message: Message):
    if not is_admin(message.from_user.id):
        return
    m, y, mname = next_month_and_year()
    submitted, missing = [], []
    for eid, disp in DBI.list_all_employees():
        (submitted if DBI.has_submitted(eid, y, m) else missing).append(disp)
    text = [f"Статус подачи за {mname}:"]
    text.append("\nПодали (" + str(len(submitted)) + "): " + (", ".join(submitted) if submitted else "—"))
    text.append("Не подали (" + str(len(missing)) + "): " + (", ".join(missing) if missing else "—"))
    await message.answer("\n".join(text))

async def emp_busy_view(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        eid = int((callback.data or "").split(":", 3)[3])
    except Exception:
        await callback.answer("Ошибка", show_alert=True); return
    dates = DBI.list_busy_dates(eid)
    txt = "\n".join(human_ru_date(d) for d in dates) if dates else "пусто"
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить", callback_data=f"empbusy:add:{eid}")
    kb.button(text="➖ Убрать", callback_data=f"empbusy:remove:{eid}")
    kb.adjust(2)
    await callback.message.answer(f"Даты сотрудника: {txt}", reply_markup=kb.as_markup())
    await callback.answer()

async def emp_busy_add_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    eid = int((callback.data or "").split(":", 2)[2])
    await state.update_data(admin_target_eid=eid)
    _, _, mname = next_month_and_year()
    await state.set_state(AdminBusyInput.waiting_for_add)
    await callback.message.answer(f"Введите числа за {mname} (пример: 2,4,10-12)")
    await callback.answer()

async def emp_busy_remove_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    eid = int((callback.data or "").split(":", 2)[2])
    await state.update_data(admin_target_eid=eid)
    await state.set_state(AdminBusyInput.waiting_for_remove)
    await callback.message.answer("Введите число/диапазон для удаления или 'очистить' чтобы удалить все даты")
    await callback.answer()

async def admin_handle_busy_add_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); await state.clear(); return
    data = await state.get_data()
    eid = data.get('admin_target_eid')
    if not eid:
        await message.answer("Нет выбранного сотрудника."); await state.clear(); return
    month, year, _ = next_month_and_year()
    days = parse_days_for_month(message.text, month, year)
    dates = format_busy_dates_for_month(days, month, year)
    added = DBI.add_busy_dates(eid, dates)
    if added:
        DBI.set_submitted(eid, year, month)
    await message.answer(f"Добавлено: {', '.join(added) if added else 'ничего нового'}")
    await state.clear()

async def admin_handle_busy_remove_text(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); await state.clear(); return
    data = await state.get_data()
    eid = data.get('admin_target_eid')
    if not eid:
        await message.answer("Нет выбранного сотрудника."); await state.clear(); return
    month, year, _ = next_month_and_year()
    raw = (message.text or '').strip().lower()
    if raw in {"очистить","очистка","clear"}:
        DBI.clear_busy_dates(eid)
        DBI.unset_submitted(eid, year, month)
        await message.answer("Все даты удалены."); await state.clear(); return
    days = parse_days_for_month(raw, month, year)
    dates = format_busy_dates_for_month(days, month, year)
    removed = DBI.remove_busy_dates(eid, dates)
    remaining = [d for d in DBI.list_busy_dates(eid) if d.startswith(f"{year:04d}-{month:02d}-")]
    if not remaining:
        DBI.unset_submitted(eid, year, month)
    await message.answer(f"Удалено: {', '.join(removed) if removed else 'ничего не удалено'}")
    await state.clear()