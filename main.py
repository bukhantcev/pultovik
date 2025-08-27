# main.py
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import BOT_TOKEN
from routing import register
from handlers.admin import monthly_broadcast_task
from services.busy_flow import monthly_reminders_task

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не указан BOT_TOKEN (добавьте его в .env)")
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    register(dp)
    asyncio.create_task(monthly_broadcast_task(bot))
    asyncio.create_task(monthly_reminders_task(bot))
    print("Bot is running… Press Ctrl+C to stop.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")