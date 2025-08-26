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
    emps = DBI.list_employees()
    txt = "Выберите сотрудника или добавьте нового:" if emps else "Список пуст. Нажмите «➕ Добавить»."
    # если сотрудников нет — показываем хотя бы одну кнопку «➕ Добавить»
    if emps:
        kb = get_employees_inline_kb()
    else:
        builder = InlineKeyboardBuilder()
        builder.button(text="➕ Добавить", callback_data="emp:add")
        builder.adjust(1)
        kb = builder.as_markup()
    await message.answer(txt, reply_markup=kb)

async def employees_menu_router(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ""
    if data == "emp:add":
        await state.set_state(AddEmployee.waiting_for_last_name)
        await callback.message.answer("Введите фамилию нового сотрудника:", reply_markup=ReplyKeyboardRemove())
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

async def add_employee_last_name(message: Message, state: FSMContext):
    # Admin-only guard
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа");
        return
    ln = (message.text or '').strip()
    if not ln:
        await message.answer("Фамилия обязательна. Введите фамилию ещё раз.")
        return
    await state.update_data(last_name=ln)
    await state.set_state(AddEmployee.waiting_for_first_name)
    await message.answer("Теперь напиши имя сотрудника")

async def add_employee_first_name(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа");
        return
    fn = (message.text or '').strip()
    if not fn:
        await message.answer("Имя обязательное. Введите имя ещё раз.")
        return
    await state.update_data(first_name=fn)
    await state.set_state(AddEmployee.waiting_for_tg_id)
    await message.answer("Отправь Telegram ID (или напиши \"Пропустить\")")

async def add_employee_tg(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа");
        return
    data = await state.get_data()
    ln = data.get('last_name')
    fn = data.get('first_name')
    if not ln or not fn:
        await message.answer("Нет данных для сохранения. Начните заново через меню ‘Сотрудники’.")
        await state.clear()
        return
    tg_raw = (message.text or '').strip()
    tg_id = None if tg_raw.lower() == 'пропустить' else tg_raw
    try:
        DBI.upsert_employee(ln, fn, tg_id)
        await message.answer(f"Сотрудник сохранён: {ln} {fn}")
    except Exception as e:
        await message.answer(f"Ошибка сохранения: {e}")
        await state.clear()
        return
    await state.clear()
    # Показать актуальный список после добавления
    txt = "Выберите сотрудника или добавьте нового:" if DBI.list_employees() else "Список пуст. Нажмите «➕ Добавить»."
    await message.answer(txt, reply_markup=get_employees_inline_kb())