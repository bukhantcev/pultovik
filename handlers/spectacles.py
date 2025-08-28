# handlers/spectacles.py
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from config import is_admin
from db import DBI
from keyboards.inline import get_spectacles_inline_kb, get_edit_employees_inline_kb, get_spectacle_info_kb

class AddSpectacle(StatesGroup):
    waiting_for_name = State()

class RenameSpectacle(StatesGroup):
    waiting_for_title = State()

async def handle_spectacles(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); return
    txt = "Выберите спектакль или добавьте новый:" if DBI.list_spectacles() else "Список пуст. Нажмите «➕ Добавить»."
    await message.answer(txt, reply_markup=get_spectacles_inline_kb())

async def spectacles_menu_router(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = callback.data or ""
    if data == "add_spectacle":
        await state.set_state(AddSpectacle.waiting_for_name)
        await callback.message.answer("Напиши название спектакля", reply_markup=ReplyKeyboardRemove())
        await callback.answer()
        return
    if data.startswith("t:"):
        # Parse short callback payload `t:<sid>`
        try:
            sid = int(data.split(":", 1)[1])
        except Exception:
            await callback.answer("Ошибка формата", show_alert=True); return
        # Fetch title and employees by spectacle id
        with DBI._conn() as con:
            row = con.execute("SELECT title FROM spectacles WHERE id=?", (sid,)).fetchone()
            if not row:
                await callback.answer("Не найдено", show_alert=True); return
            title = row[0]
            rows = con.execute(
                """
                SELECT e.display FROM spectacle_employees se
                JOIN employees e ON e.id = se.employee_id
                WHERE se.spectacle_id=?
                ORDER BY e.last_name, e.first_name
                """,
                (sid,),
            ).fetchall()
        emps = [r[0] for r in rows]
        await callback.message.answer(
            f"Спектакль: {title}\nСотрудники: {', '.join(emps) if emps else 'нет'}",
            reply_markup=get_spectacle_info_kb(sid),
        )
        await callback.answer()
        return
    if data.startswith(("edit_spectacle:", "editstart:", "spec:edit:")):
        # Open inline list of employees with checkmarks for this spectacle
        try:
            sid = int(data.rsplit(":", 1)[-1])
        except Exception:
            await callback.answer("Ошибка формата", show_alert=True); return
        await callback.message.answer("Изменение списка сотрудников:", reply_markup=get_edit_employees_inline_kb(sid))
        await callback.answer()
        return
    await callback.answer("Выберите спектакль из списка или '➕ Добавить'.", show_alert=True)

async def edit_employees_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    data = (callback.data or "")
    try:
        sid = int(data.rsplit(":", 1)[-1])
    except Exception:
        await callback.answer("Ошибка формата", show_alert=True); return
    await callback.message.answer("Изменение списка сотрудников:", reply_markup=get_edit_employees_inline_kb(sid))
    await callback.answer()

async def edit_employees_toggle(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        _, sid_s, eid_s = (callback.data or "").split(":", 2)
        sid = int(sid_s); eid = int(eid_s)
    except Exception:
        await callback.answer("Ошибка формата", show_alert=True); return
    DBI.toggle_spectacle_employee(sid, eid)
    kb = get_edit_employees_inline_kb(sid)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb)
    except TelegramBadRequest as e:
        # Игнорируем ситуацию, когда разметка не изменилась (например, повторный клик)
        if "message is not modified" not in str(e).lower():
            raise
    await callback.answer()

async def edit_employees_done(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        sid = int((callback.data or "").split(":", 1)[1])
    except Exception:
        await callback.answer("Готово"); return
    with DBI._conn() as con:
        row = con.execute("SELECT title FROM spectacles WHERE id=?", (sid,)).fetchone()
        title = row[0] if row else "Спектакль"
        rows = con.execute("""
            SELECT e.display FROM spectacle_employees se
            JOIN employees e ON e.id = se.employee_id
            WHERE se.spectacle_id=?
            ORDER BY e.last_name, e.first_name
        """, (sid,)).fetchall()
    final_list = ", ".join(r[0] for r in rows) if rows else "нет"
    await callback.message.answer(f"Сохранено. {title}: {final_list}")
    await callback.answer()

async def add_spectacle_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Название не может быть пустым. Введите ещё раз."); return
    DBI.upsert_spectacle(name)
    await state.clear()
    await message.answer(f"Сохранено!\nСпектакль «{name}» добавлен.")

async def delete_spectacle(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        sid = int((callback.data or "").split(":", 1)[1])
    except Exception:
        await callback.answer("Ошибка формата", show_alert=True); return
    with DBI._conn() as con:
        row = con.execute("SELECT title FROM spectacles WHERE id=?", (sid,)).fetchone()
        if not row:
            await callback.answer("Не найдено", show_alert=True); return
        title = row[0]
        con.execute("DELETE FROM spectacles WHERE id=?", (sid,))
        con.execute("DELETE FROM spectacle_employees WHERE spectacle_id=?", (sid,))
        con.commit()
    await callback.message.answer(f"Спектакль «{title}» удалён.")
    await callback.answer()

async def rename_spectacle_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True); return
    try:
        _, _, sid_s = (callback.data or "").split(":", 2)
        sid = int(sid_s)
    except Exception:
        await callback.answer("Ошибка формата", show_alert=True); return

    await state.update_data(rename_sid=sid)
    await state.set_state(RenameSpectacle.waiting_for_title)
    await callback.message.answer("Пришлите новое название спектакля", reply_markup=ReplyKeyboardRemove())
    await callback.answer()

async def rename_spectacle_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа"); return
    data = await state.get_data()
    sid = data.get("rename_sid")
    new_title = (message.text or "").strip()
    if not sid:
        await message.answer("Не выбран спектакль. Откройте карточку ещё раз.");
        await state.clear()
        return
    if not new_title:
        await message.answer("Название пустое. Отправьте корректный текст.");
        return

    try:
        with DBI._conn() as con:
            con.execute("UPDATE spectacles SET title=? WHERE id=?", (new_title, int(sid)))
            con.commit()
    except Exception as e:
        await message.answer("Ошибка при сохранении названия: " + str(e))
        return

    await state.clear()
    await message.answer(f"Название обновлено: «{new_title}».")

async def edit_spectacle_start(callback: CallbackQuery, state: FSMContext):
    """Открыть клавиатуру редактирования сотрудников для выбранного спектакля.
    Обрабатывает payload'ы вида: edit_spectacle:<sid> | editstart:<sid> | spec:edit:<sid>
    """
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    data = (callback.data or "")
    try:
        # sid всегда в конце после последнего ':'
        sid = int(data.rsplit(":", 1)[-1])
    except Exception:
        await callback.answer("Ошибка формата", show_alert=True)
        return

    await callback.message.answer(
        "Изменение списка сотрудников:",
        reply_markup=get_edit_employees_inline_kb(sid)
    )
    await callback.answer()