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
    for k_lower, ru in _CANON_MAP.items():
        if k_lower in lower_to_col:
            rename_map[ lower_to_col[k_lower] ] = ru

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