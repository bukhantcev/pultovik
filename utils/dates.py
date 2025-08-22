# utils/dates.py
import calendar
from datetime import date, datetime
from config import RU_MONTHS, RU_MONTHS_GEN

def next_month_and_year(today: date | None = None) -> tuple[int, int, str]:
    d = today or date.today()
    m = d.month + 1
    y = d.year
    if m > 12:
        m = 1
        y += 1
    return m, y, RU_MONTHS[m-1]

def human_ru_date(date_str: str) -> str:
    try:
        y, m, d = map(int, date_str.split("-"))
        return f"{d} {RU_MONTHS_GEN[m-1]} {y}"
    except Exception:
        return date_str

def parse_days_for_month(text: str, month: int, year: int) -> list[int]:
    s = (text or "").replace(" ", "")
    if not s:
        return []
    max_day = calendar.monthrange(year, month)[1]
    out: set[int] = set()
    for part in s.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            try:
                a_i, b_i = int(a), int(b)
            except ValueError:
                continue
            if a_i > b_i:
                a_i, b_i = b_i, a_i
            for dd in range(a_i, b_i + 1):
                if 1 <= dd <= max_day:
                    out.add(dd)
        else:
            try:
                v = int(part)
                if 1 <= v <= max_day:
                    out.add(v)
            except ValueError:
                continue
    return sorted(out)

def format_busy_dates_for_month(days: list[int], month: int, year: int) -> list[str]:
    return [f"{year:04d}-{month:02d}-{d:02d}" for d in days]