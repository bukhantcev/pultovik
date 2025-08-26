# handlers/ai_fill.py
from __future__ import annotations
from pathlib import Path
from datetime import date
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from config import is_admin, ENABLE_AI_FILL, ADMIN_ID
from config import build_playbill_url
from services.ai_fill import build_excel_from_file
from services.ai_fill import build_excel_from_site
from services.scrape_site import site_to_excel
# ↓ добавим попытку импортировать шаблон URL
try:
    from config import SITE_PLAYBILL_URL_TMPL, SITE_PLAYBILL_URL
except Exception:
    SITE_PLAYBILL_URL_TMPL = None
    SITE_PLAYBILL_URL = None


class AIFillStates(StatesGroup):
    waiting_for_file = State()


def _month_pick_kb(prefix: str = "ai:sitepick:") -> InlineKeyboardMarkup:
    """Кнопки: текущий и два следующих месяца."""
    today = date.today()
    buttons = []
    for i in range(3):
        m = (today.month - 1 + i) % 12 + 1
        y = today.year + ((today.month - 1 + i) // 12)
        ru_months = [
            "Январь","Февраль","Март","Апрель","Май","Июнь",
            "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"
        ]
        label = f"{ru_months[m-1]} {y}"
        cb = f"{prefix}{y:04d}-{m:02d}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=cb)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def ai_fill_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if not ENABLE_AI_FILL:
        await message.answer("Функция отключена админом.")
        return
    await state.set_state(AIFillStates.waiting_for_file)
    kb = InlineKeyboardBuilder()
    kb.button(text="🗓 Расписание с сайта", callback_data="ai:site")
    kb.adjust(1)
    await message.answer("Пришлите файл (фото) или нажмите «🗓 Расписание с сайта». Отмена — текстом «Отмена».", reply_markup=kb.as_markup())


async def ai_fill_cancel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    cur = await state.get_state()
    if cur != AIFillStates.waiting_for_file.state:
        return
    await state.clear()
    await message.answer("Отменено.")


async def ai_fill_receive(message: Message, state: FSMContext):
    print("ai_fill_receive called", flush=True)
    if not is_admin(message.from_user.id):
        return
    cur = await state.get_state()
    print(f"FSM state: {cur}", flush=True)
    if cur != AIFillStates.waiting_for_file.state:
        return
    # Cancel via text
    if (message.text or '').strip().lower() in {"отмена", "cancel", "stop"}:
        await state.clear()
        await message.answer("Отменено.")
        return

    temp_path: Path | None = None
    try:
        if message.photo:
            print("Handling photo upload", flush=True)
            photo = message.photo[-1]
            file = await message.bot.get_file(photo.file_id)
            temp_path = Path.cwd() / f"ai_in_{message.from_user.id}.jpg"
            await message.bot.download_file(file.file_path, destination=temp_path)
        elif message.document:
            print("Handling document upload", flush=True)
            file = await message.bot.get_file(message.document.file_id)
            suffix = Path(message.document.file_name or 'upload.bin').suffix or '.bin'
            temp_path = Path.cwd() / f"ai_in_{message.from_user.id}{suffix}"
            await message.bot.download_file(file.file_path, destination=temp_path)
        else:
            await message.answer("Формат не поддерживается. Пришлите файл (Excel/PDF/фото) или 'Отмена'.")
            return

        print(f"Calling build_excel_from_file with {temp_path}", flush=True)
        out_excel = await build_excel_from_file(temp_path)
        print("build_excel_from_file completed successfully", flush=True)

        # Отправляем админу, если он настроен, иначе пользователю
        if ADMIN_ID:
            await message.bot.send_document(ADMIN_ID, out_excel, caption="AI: импорт по шаблону")
            await message.answer("Готово. Файл отправлен администратору.")
        else:
            await message.answer_document(out_excel, caption="AI: импорт по шаблону")

        await state.clear()
    except Exception as e:
        print(f"Exception in ai_fill_receive: {e}", flush=True)
        await message.answer(f"Ошибка обработки: {e}")
    finally:
        try:
            if temp_path and temp_path.exists():
                temp_path.unlink(missing_ok=True)
        except Exception:
            pass
        print("ai_fill_receive finished", flush=True)


# ====== НОВОЕ: выбор месяца и запуск логики по «Расписание с сайта» ======

async def ai_fill_site_start(callback: CallbackQuery, state: FSMContext):
    """Показать три кнопки месяцев после нажатия «Расписание с сайта»."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return
    if not ENABLE_AI_FILL:
        await callback.answer("Функция отключена", show_alert=True)
        return
    await callback.message.answer("Выберите месяц:", reply_markup=_month_pick_kb())
    await callback.answer()


async def ai_fill_site_pick(callback: CallbackQuery, state: FSMContext):
    """Получить выбранный месяц, подставить в URL и запустить дальнейшую логику (пока заглушка вызова)."""
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для админа", show_alert=True)
        return

    data = callback.data or ""
    # формат: ai:sitepick:YYYY-MM
    try:
        _, _, ym = data.split(":", 2)
        year_s, month_s = ym.split("-", 1)
        year = int(year_s)
        month = int(month_s)
    except Exception:
        await callback.answer("Неверный формат месяца", show_alert=True)
        return

    url = build_playbill_url(month, year)

    await callback.answer("Начинаю сбор расписания…")

    try:
        out_excel, count = await site_to_excel(url, month=month, year=year)
        # Отправляем файл админу
        await callback.message.answer_document(
            out_excel,
            caption=f"AI: расписание с сайта → {month:02d}.{year}\nИсточник: {url}\nНайдено карточек: {count}"
        )
    except Exception as e:
        await callback.message.answer(f"Ошибка при сборе расписания: {e}\nURL: {url}")
