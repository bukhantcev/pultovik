

# handlers/ai_fill.py
from __future__ import annotations
from pathlib import Path
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from config import is_admin, ENABLE_AI_FILL, ADMIN_ID
from services.ai_fill import build_excel_from_file

class AIFillStates(StatesGroup):
    waiting_for_file = State()

async def ai_fill_start(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    if not ENABLE_AI_FILL:
        await message.answer("Функция отключена админом.")
        return
    await state.set_state(AIFillStates.waiting_for_file)
    await message.answer("Пришлите файл (Excel/PDF/фото) или напишите 'Отмена'.")

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

        # Отправляем админу, если он настроен, иначе отправляем пользователю
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