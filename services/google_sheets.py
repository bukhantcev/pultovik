# google_sheets.py
from __future__ import annotations
import os
import json
import base64
from typing import Optional, Tuple

import pandas as pd

# -------------------------
# Нормализация DataFrame расписания
# -------------------------
_EXPECTED_ORDER = [
    "Дата",
    "Тип",
    "Название",
    "Время",
    "Локация",
    "Город",
    "Сотрудник",
    "Дежурный сотрудник",
    "Инфо",
]

# Возможные внутренние названия -> русские заголовки
_CANON_MAP = {
    "date": "Дата",
    "type": "Тип",
    "title": "Название",
    "time": "Время",
    "location": "Локация",
    "city": "Город",
    "employee": "Сотрудник",
    "duty_employee": "Дежурный сотрудник",
    "info": "Инфо",
}

def _normalize_schedule_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Приводит колонки к ожидаемым русским заголовкам и гарантирует наличие
    столбца «Дежурный сотрудник». Также старается сохранить порядок.
    """
    if df is None or df.empty:
        # даже для пустого — вернём каркас с нужными заголовками
        return pd.DataFrame(columns=_EXPECTED_ORDER)

    # 1) Попробовать переименовать известные внутренние имена -> русские
    rename_map = {}
    lower_to_col = {str(c).strip().lower(): c for c in df.columns}

    # Вариант 1: внутренние имена -> русские заголовки
    for k_lower, ru in _CANON_MAP.items():
        if k_lower in lower_to_col:
            rename_map[lower_to_col[k_lower]] = ru

    # Вариант 2: русские алиасы -> канонический русский заголовок
    # Поддержим «Дежурный сотрудник» и короткий «Дежурный»
    for alias in ("дежурный сотрудник", "дежурный"):
        if alias in lower_to_col and "Дежурный сотрудник" not in rename_map.values():
            rename_map[lower_to_col[alias]] = "Дежурный сотрудник"

    df2 = df.rename(columns=rename_map)

    # 2) Гарантировать наличие столбца «Дежурный сотрудник»
    if "Дежурный сотрудник" not in df2.columns:
        df2["Дежурный сотрудник"] = ""

    # 3) Собрать финальный порядок: сначала ожидаемые, затем остальные
    ordered_cols = [c for c in _EXPECTED_ORDER if c in df2.columns]
    tail_cols = [c for c in df2.columns if c not in ordered_cols]
    df2 = df2[ordered_cols + tail_cols]

    return df2

# gspread + creds
import gspread
from google.oauth2.service_account import Credentials


# -------------------------
# Русские месяцы (именительный падеж) + форматирование
# -------------------------
RU_MONTHS = [
    "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
]

def month_title_ru(year: int, month: int) -> str:
    """Возвращает заголовок листа, например: 'Сентябрь 2025'."""
    if not (1 <= month <= 12):
        raise ValueError("month must be in 1..12")
    return f"{RU_MONTHS[month - 1]} {year}"


# -------------------------
# Загрузка учётки сервис-аккаунта
# -------------------------
def _load_service_account_credentials() -> Credentials:
    """
    Способы:
      1) GOOGLE_SERVICE_ACCOUNT_JSON — путь к JSON файлу
      2) GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 — Base64 строка с содержимым JSON
    Обязательные scope для Google Sheets/Drive.
    """
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    json_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    json_b64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64")

    if json_path and os.path.exists(json_path):
        return Credentials.from_service_account_file(json_path, scopes=scopes)

    if json_b64:
        try:
            data = base64.b64decode(json_b64).decode("utf-8")
            info = json.loads(data)
            return Credentials.from_service_account_info(info, scopes=scopes)
        except Exception as e:
            raise RuntimeError(f"Не удалось декодировать GOOGLE_SERVICE_ACCOUNT_JSON_BASE64: {e}")

    raise RuntimeError(
        "Не найдены креды сервис-аккаунта. "
        "Укажите GOOGLE_SERVICE_ACCOUNT_JSON (путь к .json) "
        "или GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 (base64 содержимое json)."
    )


def get_gspread_client() -> gspread.Client:
    creds = _load_service_account_credentials()
    return gspread.authorize(creds)


# -------------------------
# Вспомогательные
# -------------------------
def _resolve_spreadsheet_id() -> str:
    """
    Ищем ID таблицы в окружении:
      - GOOGLE_SHEET_ID
      - GOOGLE_SHEETS_ID
      - GOOGLE_SHEETS_SPREADSHEET_ID
    """
    for key in ("GOOGLE_SHEET_ID", "GOOGLE_SHEETS_ID", "GOOGLE_SHEETS_SPREADSHEET_ID"):
        val = os.getenv(key)
        if val:
            return val
    raise RuntimeError("Не задан ID Google Sheet. Укажите GOOGLE_SHEET_ID (или GOOGLE_SHEETS_ID).")


def _dataframe_to_rows(df: pd.DataFrame) -> list[list]:
    """
    Преобразует DataFrame в список списков с первой строкой заголовков.
    Все значения приводятся к str (Google Sheets любит явные строки).
    """
    headers = list(df.columns)
    values = df.astype(object).where(pd.notnull(df), "").values.tolist()
    # строка заголовков + данные
    rows = [headers] + [[str(v) for v in row] for row in values]
    return rows


def _delete_worksheet_if_exists(sh: gspread.Spreadsheet, title: str) -> None:
    try:
        ws = sh.worksheet(title)
        sh.del_worksheet(ws)
    except gspread.exceptions.WorksheetNotFound:
        pass  # нет листа — ок


def _create_worksheet(sh: gspread.Spreadsheet, title: str, rows: int = 1000, cols: int = 26) -> gspread.Worksheet:
    # Ограничение на длину заголовка листа у Google: <= 100 символов
    if len(title) > 100:
        title = title[:100]
    return sh.add_worksheet(title=title, rows=rows, cols=cols)


# -------------------------
# Публичная функция
# -------------------------
def publish_schedule_to_sheets(year: int, month: int, df: pd.DataFrame, sheet_id: Optional[str] = None) -> Tuple[str, str]:
    """
    Публикует расписание в Google Sheets.

    - Открывает таблицу по ID (из env или аргумента).
    - Лист называется 'Месяц Год' (например, 'Сентябрь 2025').
    - Если лист уже есть — удаляет, создаёт заново.
    - Заливает весь DataFrame.

    Возвращает кортеж: (URL Google Sheet, title листа).
    """
    title = month_title_ru(year, month)
    spreadsheet_id = sheet_id or _resolve_spreadsheet_id()

    # Нормализуем DF, чтобы точно была колонка «Дежурный сотрудник»
    df = _normalize_schedule_df(df)

    gc = get_gspread_client()
    sh = gc.open_by_key(spreadsheet_id)

    # Пересоздаём лист
    _delete_worksheet_if_exists(sh, title)
    ws = _create_worksheet(sh, title, rows=max(len(df) + 10, 100), cols=max(len(df.columns) + 2, 10))

    # Загрузка данных
    rows = _dataframe_to_rows(df)
    # Google API допускает 5 млн ячеек — мы отправляем одним update()
    ws.update("A1", rows, value_input_option="USER_ENTERED")

    # Небольшой бонус: автоширина колонок (через batch_update)
    try:
        sh.batch_update({
            "requests": [{
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": 0,
                        "endIndex": len(df.columns)
                    }
                }
            }]
        })
    except Exception:
        # Автоширина — nice-to-have; молча глотаем ошибки
        pass

    return sh.url, title


# -------------------------
# Чтение расписания на определённую дату
# -------------------------

def fetch_schedule_for_date(day, sheet_id: Optional[str] = None) -> str:
    """Читает расписание на конкретную дату из Google Sheets.

    Использует тот же Spreadsheet, что и publish_schedule_to_sheets:
    - ID берётся из env (через _resolve_spreadsheet_id) или аргументом sheet_id.
    - Название листа берётся через month_title_ru(day.year, day.month), т.е. "Сентябрь 2025".

    Ожидаемый формат листа:
      - первая строка: заголовки
      - есть колонка "Дата" (если нет — берём первую колонку)
    """
    from datetime import date as _date

    if not isinstance(day, _date):
        raise TypeError("day must be datetime.date")

    spreadsheet_id = sheet_id or _resolve_spreadsheet_id()
    title = month_title_ru(day.year, day.month)

    gc = get_gspread_client()
    sh = gc.open_by_key(spreadsheet_id)

    try:
        ws = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return f"{day.strftime('%d.%m.%Y')}: графика на  {title} пока нет"

    values = ws.get_all_values()
    if not values:
        return f"{day.strftime('%d.%m.%Y')}: нет данных"

    header = values[0]
    rows = values[1:]

    # индекс колонки даты
    date_idx = 0
    for i, h in enumerate(header):
        hh = (h or '').strip().lower()
        if hh in {"дата", "date", "day"}:
            date_idx = i
            break

    target_a = day.strftime('%d.%m.%Y')
    target_b = day.strftime('%Y-%m-%d')

    # Поддержим даты, записанные словами: "2 января 2026"
    _ru_months = {
        "января": 1,
        "февраля": 2,
        "марта": 3,
        "апреля": 4,
        "мая": 5,
        "июня": 6,
        "июля": 7,
        "августа": 8,
        "сентября": 9,
        "октября": 10,
        "ноября": 11,
        "декабря": 12,
    }

    def _cell_matches_day(cell_text: str) -> bool:
        s = (cell_text or '').strip()
        if not s:
            return False

        # прямые форматы
        if s in {target_a, target_b}:
            return True

        # попробуем распарсить dd.mm.yyyy
        try:
            if len(s) >= 8 and '.' in s:
                dd, mm, yy = s.split('.', 2)
                if dd.isdigit() and mm.isdigit() and yy.isdigit():
                    if int(yy) == day.year and int(mm) == day.month and int(dd) == day.day:
                        return True
        except Exception:
            pass

        # формат "2 января 2026" (и похожие)
        s_low = ' '.join(s.lower().split())
        parts = s_low.split(' ')
        if len(parts) >= 3 and parts[0].isdigit() and parts[2].isdigit():
            dd = int(parts[0])
            yy = int(parts[2])
            mm = _ru_months.get(parts[1])
            if mm and yy == day.year and mm == day.month and dd == day.day:
                return True

        return False

    matched = []
    for r in rows:
        if date_idx >= len(r):
            continue
        cell = (r[date_idx] or '').strip()
        if _cell_matches_day(cell):
            matched.append(r)

    if not matched:
        return f"{day.strftime('%d.%m.%Y')}\nНет событий"

    out_lines = [day.strftime('%d.%m.%Y')]

    # ожидаемые заголовки
    def col_idx(name: str) -> Optional[int]:
        for i, h in enumerate(header):
            if (h or '').strip().lower() == name.lower():
                return i
        return None

    idx_type = col_idx('Тип')
    idx_title = col_idx('Название')
    idx_loc = col_idx('Локация')
    idx_emp = col_idx('Сотрудник')
    idx_duty = col_idx('Дежурный сотрудник')
    idx_info = col_idx('Инфо')

    for r in matched:
        if idx_type is not None and idx_type < len(r) and r[idx_type]:
            out_lines.append(f"Тип: {r[idx_type]}")

        if idx_title is not None and idx_title < len(r) and r[idx_title]:
            out_lines.append(f"Название: {r[idx_title]}")

        if idx_loc is not None and idx_loc < len(r) and r[idx_loc]:
            out_lines.append(f"Локация: {r[idx_loc]}")

        emp = r[idx_emp] if idx_emp is not None and idx_emp < len(r) else ''
        duty = r[idx_duty] if idx_duty is not None and idx_duty < len(r) else ''
        if emp or duty:
            if emp and duty:
                out_lines.append(f"Сотрудник: {emp}, {duty}")
            else:
                out_lines.append(f"Сотрудник: {emp or duty}")

        if idx_info is not None and idx_info < len(r) and r[idx_info]:
            out_lines.append(f"Инфо: {r[idx_info]}")

        out_lines.append("")  # пустая строка между событиями

    return "\n".join(out_lines).rstrip()