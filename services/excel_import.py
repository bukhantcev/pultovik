# services/excel_import.py
from pathlib import Path
import pandas as pd
from db import DBI
import sqlite3

EXPECTED_EVENT_COLUMNS = {
    'id': 'id',
    'дата': 'date',
    'тип': 'type',
    'название': 'title',
    'время': 'time',
    'локация': 'location',
    'город': 'city',
    'сотрудник': 'employee',
    'инфо': 'info',
}

def _normalize_event_columns(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key in EXPECTED_EVENT_COLUMNS:
            mapping[col] = EXPECTED_EVENT_COLUMNS[key]
    return df.rename(columns=mapping)

def _known_spectacle_titles_lower() -> set[str]:
    try:
        with DBI._conn() as con:
            cur = con.execute("SELECT title FROM spectacles")
            return {str(r[0]).strip().lower() for r in cur.fetchall() if r and r[0]}
    except Exception:
        return set()

def import_events_from_excel(path: Path, year: int, month: int) -> tuple[int, int, list[str]]:
    df = pd.read_excel(path)
    df = _normalize_event_columns(df)
    if 'date' not in df.columns:
        raise ValueError("В Excel нет колонки 'Дата'")
    keep = ['date','type','title','time','location','city','employee','info']
    for k in keep:
        if k not in df.columns:
            df[k] = None

    import re, calendar as _cal
    max_day = _cal.monthrange(year, month)[1]
    def _to_day(v):
        if pd.isna(v): return None
        if hasattr(v, 'day'):
            d = int(getattr(v, 'day', 0));  return d if 1 <= d <= max_day else None
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            d = int(v); return d if 1 <= d <= max_day else None
        s = str(v).strip(); m = re.search(r"\d+", s)
        if m:
            d = int(m.group(0)); return d if 1 <= d <= max_day else None
        return None

    df = df.assign(_day=df['date'].map(_to_day)).query("_day.notna()")
    df['date'] = df['_day'].astype(int).map(lambda d: f"{year:04d}-{month:02d}-{d:02d}")
    df = df.drop(columns=['_day'])

    known = _known_spectacle_titles_lower()

    titles: list[str] = []
    unknown_titles: list[str] = []
    seen_lower: set[str] = set()
    for v in df['title']:
        if pd.isna(v):
            continue
        s = str(v).strip()
        if not s:
            continue
        s_l = s.lower()
        if s_l in seen_lower:
            continue
        seen_lower.add(s_l)
        titles.append(s)
        if s_l not in known:
            unknown_titles.append(s)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'date': r['date'],
            'type': None if pd.isna(r['type']) else str(r['type']),
            'title': None if pd.isna(r['title']) else str(r['title']),
            'time': None if pd.isna(r['time']) else str(r['time']),
            'location': None if pd.isna(r['location']) else str(r['location']),
            'city': None if pd.isna(r['city']) else str(r['city']),
            'employee': None if pd.isna(r['employee']) else str(r['employee']),
            'info': None if pd.isna(r['info']) else str(r['info']),
        })

    # Если в таблице есть незнакомые названия — НЕ вносим изменения в БД.
    # Хендлер должен по очереди запросить у админа назначение сотрудников и создание записей спектаклей.
    if unknown_titles:
        return (len(unknown_titles), 0, unknown_titles)

    DBI.delete_events_for_month(year, month)
    DBI.insert_events(rows)
    return (0, len(rows), [])