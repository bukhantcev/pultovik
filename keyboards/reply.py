# keyboards/reply.py
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from config import is_admin, ENABLE_AI_FILL
from db import DBI
from utils.dates import next_month_and_year
from datetime import date

def get_user_busy_reply_kb(user_id: int) -> ReplyKeyboardMarkup:
    from config import ADMIN_ID
    print(f"[KB] build for user={user_id} is_admin={is_admin(user_id)} ADMIN_ID={ADMIN_ID}", flush=True)
    base_rows = []
    if is_admin(user_id):
        base_rows = [
            [KeyboardButton(text="Спектакли"), KeyboardButton(text="Сотрудники")],
            [KeyboardButton(text="Сделать график")],
            [KeyboardButton(text="Импорт расписания")]
        ]
        if ENABLE_AI_FILL:
            base_rows.append([KeyboardButton(text="AI заполнить шаблон")])

    m, y, mname = next_month_and_year()
    has_busy = False
    row = DBI.get_employee_by_tg(user_id)
    if row:
        has_busy = DBI.count_busy_for_month(row[0], y, m) > 0

    # After the 25th, non-admin users should not see "Подать даты" — only "Посмотреть свои даты"
    today = date.today()
    show_submit = True
    if not is_admin(user_id) and today.day >= 30:
        show_submit = False

    if has_busy or not show_submit:
        busy_rows = [[KeyboardButton(text="Посмотреть свои даты")]]
    else:
        busy_rows = [[KeyboardButton(text=f"Подать даты за {mname}")]]
    kb = ReplyKeyboardMarkup(keyboard=base_rows + busy_rows, resize_keyboard=True)
    return kb