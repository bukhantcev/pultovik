# handlers/excel.py
from pathlib import Path
import tempfile
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram import Router, F
from aiogram.utils.keyboard import InlineKeyboardBuilder
from config import is_admin, RU_MONTHS
from keyboards.inline import get_month_pick_inline, get_edit_employees_inline_kb
from services.excel_import import import_events_from_excel
from services.excel_export import export_month_schedule, file_as_input, month_caption
from services.auto_assign import auto_assign_events_for_month
from db import DBI

router = Router()

class UploadExcel(StatesGroup):
    waiting_for_file = State()
    waiting_for_month = State()

class AssignUnknown(StatesGroup):
    waiting = State()

async def _ask_unknown_spectacle(message: Message, state: FSMContext, title: str):
    data = await state.get_data()
    selected = set(data.get('current_selected') or [])
    # загрузим всех сотрудников
    with DBI._conn() as con:
        rows = con.execute("SELECT id, first_name, last_name FROM employees ORDER BY last_name, first_name").fetchall()
    kb = InlineKeyboardBuilder()
    for emp_id, fn, ln in rows:
        mark = "✅" if emp_id in selected else "☐"
        kb.button(text=f"{mark} {ln} {fn}", callback_data=f"unkemp:{emp_id}")
    kb.button(text="Сохранить", callback_data="unksave")
    kb.adjust(1)
    await message.answer(
        f"Неизвестный спектакль:\n<b>{title}</b>\nВыберите сотрудников и нажмите «Сохранить».",
        reply_markup=kb.as_markup()
    )

# Handler for starting import
@router.message(F.text == "Импорт расписания")
async def import_schedule_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.set_state(UploadExcel.waiting_for_file)
    await message.answer("Пришлите файл расписания (Excel/PDF/фото). После приёма я спрошу: «На какой месяц?»")

@router.message(F.document)
async def handle_excel_upload(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    # реагируем на документ только если запущен режим импорта
    current = await state.get_state()
    if current != UploadExcel.waiting_for_file:
        return
    doc = message.document
    if not doc:
        return
    file = await message.bot.get_file(doc.file_id)
    suffix = Path(doc.file_name or 'upload.xlsx').suffix or '.xlsx'
    tmp = Path(tempfile.gettempdir()) / f"pultovik_upload_{message.from_user.id}{suffix}"
    await message.bot.download_file(file.file_path, destination=tmp)
    await state.update_data(upload_path=str(tmp))
    await state.set_state(UploadExcel.waiting_for_month)
    await message.answer("На какой месяц?", reply_markup=get_month_pick_inline(prefix='xlsmonth:'))

@router.callback_query(F.data.startswith('xlsmonth:'))
async def handle_excel_month_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ''
    if not data.startswith('xlsmonth:'):
        await callback.answer(); return
    try:
        year, month = map(int, data.split(':',1)[1].split('-',1))
    except Exception:
        await callback.answer("Неверный месяц", show_alert=True); return
    st = await state.get_data()
    p = st.get('upload_path')
    if not p:
        await callback.answer("Файл не найден. Пришлите Excel заново.", show_alert=True); return
    try:
        unknown_count, inserted, titles = import_events_from_excel(Path(p), year, month)
    except Exception as e:
        await callback.message.answer(f"Ошибка импорта: {e}")
        await state.clear(); await callback.answer(); return

    if unknown_count > 0:
        # Оставим в очереди только реально неизвестные названия
        known = set(DBI.list_spectacles())
        unknown_titles = [t for t in titles if t and t not in known]
        if unknown_titles:
            await state.update_data(unknown_titles=unknown_titles, import_year=year, import_month=month, import_path=str(p))
            # start with first unknown title (пошаговый опрос)
            await state.update_data(current_selected=[])
            await state.set_state(AssignUnknown.waiting)
            first_title = unknown_titles[0]
            await _ask_unknown_spectacle(callback.message, state, first_title)
            await callback.answer()
            return

    await state.clear()
    await callback.message.answer(f"Импорт завершён: {inserted} записей за {RU_MONTHS[month-1]} {year}")
    await callback.answer("Готово")

@router.message(F.text == "Сделать график")
async def handle_make_schedule(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); return
    await message.answer("На какой месяц сформировать график?", reply_markup=get_month_pick_inline(prefix='mkmonth:'))

@router.callback_query(F.data.startswith('mkmonth:'))
async def handle_make_schedule_pick(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ''
    if not data.startswith('mkmonth:'):
        await callback.answer(); return
    try:
        year, month = map(int, data.split(':',1)[1].split('-',1))
    except Exception:
        await callback.answer("Неверный месяц", show_alert=True); return

    updated = auto_assign_events_for_month(year, month)
    path, _ = export_month_schedule(year, month)
    try:
        await callback.message.answer_document(file_as_input(path), caption=month_caption(year, month, updated))
    except Exception as e:
        await callback.message.answer(f"Не удалось отправить файл: {e}")
    await callback.answer("Готово")

@router.callback_query(F.data.startswith('unkemp:'), AssignUnknown.waiting)
async def unknown_toggle_employee(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        emp_id = int((callback.data or '').split(':', 1)[1])
    except Exception:
        await callback.answer(); return
    data = await state.get_data()
    selected = set(data.get('current_selected') or [])
    if emp_id in selected:
        selected.remove(emp_id)
    else:
        selected.add(emp_id)
    await state.update_data(current_selected=list(selected))

    # Перерисуем клавиатуру на текущем сообщении
    with DBI._conn() as con:
        rows = con.execute("SELECT id, first_name, last_name FROM employees ORDER BY last_name, first_name").fetchall()
    kb = InlineKeyboardBuilder()
    for rid, fn, ln in rows:
        mark = "✅" if rid in selected else "☐"
        kb.button(text=f"{mark} {ln} {fn}", callback_data=f"unkemp:{rid}")
    kb.button(text="Сохранить", callback_data="unksave")
    kb.adjust(1)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb.as_markup())
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data == 'unksave', AssignUnknown.waiting)
async def unknown_save_current(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = await state.get_data()
    queue = list(data.get('unknown_titles') or [])
    if not queue:
        await state.clear(); await callback.answer(); return
    title = queue.pop(0)
    selected = list(set(data.get('current_selected') or []))

    # создаём/получаем спектакль и сохраняем сотрудников
    DBI.upsert_spectacle(title)
    sid = DBI.get_spectacle_id(title)
    with DBI._conn() as con:
        con.execute("DELETE FROM spectacle_employees WHERE spectacle_id=?", (sid,))
        for eid in selected:
            con.execute(
                "INSERT OR IGNORE INTO spectacle_employees(spectacle_id, employee_id) VALUES(?,?)",
                (sid, eid),
            )
        con.commit()

    await state.update_data(unknown_titles=queue, current_selected=[])
    await callback.message.answer(f"Сохранено: «{title}» — назначено: {len(selected)}")

    if queue:
        # следующий неизвестный
        await _ask_unknown_spectacle(callback.message, state, queue[0])
        await callback.answer()
        return

    # Очередь закончилась — повторяем импорт
    st = await state.get_data()
    p = st.get('import_path')
    year = int(st.get('import_year'))
    month = int(st.get('import_month'))
    unknown_count, inserted, _ = import_events_from_excel(Path(p), year, month)
    await state.clear()
    if unknown_count:
        await callback.message.answer("Ещё остались неизвестные названия, повторим цикл импорта…")
    else:
        await callback.message.answer(f"Импорт завершён: {inserted} записей за {RU_MONTHS[month-1]} {year}")
    await callback.answer()