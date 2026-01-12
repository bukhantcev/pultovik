# handlers/busy_admin.py
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from config import is_admin
from db import DBI
from utils.dates import next_month_and_year, parse_days_for_month, format_busy_dates_for_month, human_ru_date
import datetime
import traceback

class AdminBusyInput(StatesGroup):
    waiting_for_add = State()
    waiting_for_remove = State()

def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    """Return (year, month) shifted by delta months."""
    y, m = year, month
    m += delta
    while m <= 0:
        m += 12
        y -= 1
    while m > 12:
        m -= 12
        y += 1
    return y, m


def _month_title(year: int, month: int) -> str:
    # RU_MONTHS is in config; but in this handler we already use next_month_and_year() for naming.
    # We'll reuse it only for the month name list via DBI/config is not available here, so make a simple "MM.YYYY".
    return f"{month:02d}.{year}"  # компактно

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
    try:
        print("emp_busy_view callback_data:", callback.data)
        # callback_data formats:
        #   NEW: empbusy:view:<eid>[:YYYY-MM]
        #   OLD: emp:busy:view:<eid>[:YYYY-MM]
        parts = (callback.data or "").split(":")

        eid = None
        month_part = None

        # NEW
        if len(parts) >= 3 and parts[0] == "empbusy" and parts[1] == "view":
            eid = parts[2]
            month_part = parts[3] if len(parts) >= 4 else None

        # OLD
        elif len(parts) >= 4 and parts[0] == "emp" and parts[1] == "busy" and parts[2] == "view":
            eid = parts[3]
            month_part = parts[4] if len(parts) >= 5 else None

        if not eid:
            await callback.answer("Ошибка", show_alert=True)
            return

        try:
            eid = int(eid)
        except Exception:
            await callback.answer("Ошибка", show_alert=True)
            return

        # выбранный месяц
        today = datetime.date.today()
        view_year, view_month = today.year, today.month
        if month_part:
            try:
                view_year = int(month_part[0:4])
                view_month = int(month_part[5:7])
            except Exception:
                view_year, view_month = today.year, today.month

        prefix = f"{view_year:04d}-{view_month:02d}-"
        dates = [d for d in DBI.list_busy_dates(eid) if d.startswith(prefix)]
        txt = "\n".join(human_ru_date(d) for d in dates) if dates else "пусто"

        kb = InlineKeyboardBuilder()

        # навигация по месяцам: prev / current / next
        py, pm = _add_months(view_year, view_month, -1)
        ny, nm = _add_months(view_year, view_month, +1)
        kb.button(text="◀️ пред", callback_data=f"empbusy:view:{eid}:{py:04d}-{pm:02d}")
        kb.button(text="текущий", callback_data=f"empbusy:view:{eid}:{today.year:04d}-{today.month:02d}")
        kb.button(text="след ▶️", callback_data=f"empbusy:view:{eid}:{ny:04d}-{nm:02d}")

        # админские кнопки только для админа
        if is_admin(callback.from_user.id):
            kb.button(text="➕ Добавить", callback_data=f"empbusy:add:{eid}")
            kb.button(text="➖ Убрать", callback_data=f"empbusy:remove:{eid}")
            kb.adjust(3, 2)
        else:
            kb.adjust(3)

        title = _month_title(view_year, view_month)
        try:
            await callback.message.edit_text(f"{title}\nДаты сотрудника:\n{txt}", reply_markup=kb.as_markup())
        except TelegramBadRequest:
            # если сообщение нельзя отредактировать — отправим новое
            await callback.message.answer(f"{title}\nДаты сотрудника:\n{txt}", reply_markup=kb.as_markup())
        await callback.answer()
    except Exception as e:
        traceback.print_exc()
        try:
            await callback.answer(f"Ошибка: {e}", show_alert=True)
        except Exception:
            pass

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