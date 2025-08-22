# handlers/start.py
from aiogram.types import Message
from aiogram.fsm.context import FSMContext
from keyboards.reply import get_user_busy_reply_kb

async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Выберите раздел:", reply_markup=get_user_busy_reply_kb(message.from_user.id))