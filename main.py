# main.py
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from config import BOT_TOKEN
from routing import register

# фоновые задачи
from handlers.admin import monthly_broadcast_task
from services.busy_flow import monthly_reminders_task

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("Не указан BOT_TOKEN (добавьте его в .env)")

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    register(dp)

    # ---- старт фоновых задач (живут всё время работы процесса) ----
    bg_tasks = [
        asyncio.create_task(monthly_broadcast_task(bot)),   # hourly loop
        asyncio.create_task(monthly_reminders_task(bot)),   # reminders on 12 и 24
    ]

    print("Bot is running… Press Ctrl+C to stop.")
    try:
        await dp.start_polling(bot)
    finally:
        # корректно гасим фоновые корутины
        for t in bg_tasks:
            t.cancel()
        await asyncio.gather(*bg_tasks, return_exceptions=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Bot stopped")