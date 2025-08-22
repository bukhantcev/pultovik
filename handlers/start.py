# handlers/start.py
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder

from keyboards.reply import get_user_busy_reply_kb
from db import DBI
from config import ADMIN_ID


async def cmd_start(message: Message, state: FSMContext):
    # Всегда чистим состояние
    await state.clear()

    user = message.from_user
    tg_id = user.id

    # Если уже авторизован — показываем обычное меню (админские/пользовательские кнопки решает reply-клавиатура)
    if DBI.is_authorized(tg_id):
        await message.answer("Выберите раздел:", reply_markup=get_user_busy_reply_kb(tg_id))
        return

    # Если уже есть заявка на авторизацию — просто напоминаем
    if DBI.get_pending_auth(tg_id):
        await message.answer("Новый пользователь. Нужна авторизация.\nЗаявка уже отправлена администратору — ожидайте.")
        return

    # Кладём запрос в очередь
    DBI.add_pending_auth(
        tg_id=tg_id,
        first_name=user.first_name,
        last_name=user.last_name,
        username=user.username,
    )

    # Уведомляем администратора с кнопками: Привязать / Отклонить
    if ADMIN_ID:
        try:
            b = InlineKeyboardBuilder()
            b.button(text="Привязать", callback_data=f"auth:list:{tg_id}")
            b.button(text="Отклонить", callback_data=f"auth:deny:{tg_id}")
            b.adjust(2)
            info = (
                "Новый запрос на авторизацию\n"
                f"ID: {tg_id}\n"
                f"Имя: {user.first_name or '-'}\n"
                f"Фамилия: {user.last_name or '-'}\n"
                f"Username: @{user.username if user.username else '-'}"
            )
            await message.bot.send_message(chat_id=ADMIN_ID, text=info, reply_markup=b.as_markup())
        except Exception:
            # молча игнорируем сбой доставки админу
            pass

    # Сообщаем пользователю
    await message.answer("Новый пользователь. Нужна авторизация.\nЗапрос отправлен администратору.")