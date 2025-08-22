# handlers/employees.py
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from config import is_admin
from db import DBI
from keyboards.inline import get_employees_inline_kb
from utils.dates import human_ru_date

class AddEmployee(StatesGroup):
    waiting_for_last_name = State()
    waiting_for_first_name = State()
    waiting_for_tg_id = State()

class EditEmployeeTg(StatesGroup):
    waiting_for_tg = State()

async def handle_workers(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); return
    txt = "Выберите сотрудника или добавьте нового:" if DBI.list_employees() else "Список пуст. Нажмите «➕ Добавить»."
    await message.answer(txt, reply_markup=get_employees_inline_kb())

async def employees_menu_router(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ""
    if data == "emp:add":
        await state.set_state(AddEmployee.waiting_for_last_name)
        await callback.message.answer("Напиши фамилию сотрудника", reply_markup=ReplyKeyboardRemove())
        await callback.answer(); return
    if data.startswith("emp:show:"):
        disp = data.split(":", 2)[2]
        with DBI._conn() as con:
            row = con.execute("SELECT id, last_name, first_name, tg_id FROM employees WHERE display=?", (disp,)).fetchone()
        if not row:
            await callback.message.answer("Не найден сотрудник"); await callback.answer(); return
        eid, ln, fn, tg = row
        tg_text = tg if tg else "—"
        kb = InlineKeyboardBuilder()
        kb.button(text="✏️ Изменить TG ID", callback_data=f"emp:tg:start:{eid}")
        kb.button(text="🗑 Удалить", callback_data=f"emp:del:ask:{eid}")
        kb.button(text="📅 Показать даты", callback_data=f"emp:busy:view:{eid}")
        kb.adjust(1)
        await callback.message.answer(f"Сотрудник:\nФамилия: {ln}\nИмя: {fn}\nTelegram ID: {tg_text}", reply_markup=kb.as_markup())
        await callback.answer(); return

async def emp_del_ask(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        eid = int((callback.data or "").split(":", 3)[3])
    except Exception:
        await callback.answer("Ошибка", show_alert=True); return
    kb = InlineKeyboardBuilder()
    kb.button(text="Да, удалить", callback_data=f"emp:del:yes:{eid}")
    kb.button(text="Отмена", callback_data="emp:del:no")
    kb.adjust(1)
    await callback.message.answer("Точно удалить сотрудника?", reply_markup=kb.as_markup())
    await callback.answer()

async def emp_del_yes(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        eid = int((callback.data or "").split(":", 3)[3])
    except Exception:
        await callback.answer("Ошибка", show_alert=True); return
    DBI.delete_employee(eid)
    await callback.message.answer("Сотрудник удалён ✅")
    await callback.answer()

async def emp_del_no(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    await callback.answer("Отменено")

async def emp_tg_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        eid = int((callback.data or "").split(":", 3)[3])
    except Exception:
        await callback.answer("Ошибка", show_alert=True); return
    await state.update_data(edit_emp_id=eid)
    await state.set_state(EditEmployeeTg.waiting_for_tg)
    await callback.message.answer("Пришли новый Telegram ID (или напиши 'Пропустить' / 'Очистить')")
    await callback.answer()

async def emp_tg_set_value(message: Message, state: FSMContext):
    data = await state.get_data()
    eid = data.get('edit_emp_id')
    if eid is None:
        await message.answer("Нет выбранного сотрудника."); await state.clear(); return
    raw = (message.text or '').strip().lower()
    if raw == 'пропустить':
        await message.answer("Изменение отменено."); await state.clear(); return
    if raw in {'очистить','удалить','-'}:
        DBI.set_employee_tg_by_id(eid, None)
        await message.answer("Telegram ID очищен ✅"); await state.clear(); return
    DBI.set_employee_tg_by_id(eid, (message.text or '').strip())
    await message.answer("Telegram ID обновлён ✅")
    await state.clear()