# services/excel_export.py
import tempfile
from pathlib import Path
import pandas as pd
from aiogram.types import FSInputFile
from db import DBI
from config import RU_MONTHS
from utils.dates import human_ru_date

def export_month_schedule(year: int, month: int) -> tuple[Path, int]:
    prefix = f"{year:04d}-{month:02d}-"
    with DBI._conn() as con:
        evs = con.execute("""
            SELECT date, COALESCE(type,''), COALESCE(title,''), COALESCE(time,''),
                   COALESCE(location,''), COALESCE(city,''), COALESCE(employee,''), COALESCE(duty_employee,''), COALESCE(info,'')
            FROM events WHERE date LIKE ? ORDER BY date, title, time
        """, (prefix+'%',)).fetchall()
    df = pd.DataFrame(evs, columns=["date","type","title","time","location","city","employee","duty_employee","info"])
    df["date"] = df["date"].map(human_ru_date)
    df = df.rename(columns={
        "date":"Дата","type":"Тип","title":"Название","time":"Время",
        "location":"Локация","city":"Город","employee":"Сотрудник","duty_employee":"Дежурный сотрудник","info":"Инфо"
    })
    out_path = Path(tempfile.gettempdir()) / f"График_{year}-{month:02d}.xlsx"
    df.to_excel(out_path, index=False)
    return out_path, len(df)

def file_as_input(path: Path) -> FSInputFile:
    return FSInputFile(str(path))

def month_caption(year: int, month: int, updated: int) -> str:
    return f"График на {RU_MONTHS[month-1]} {year}. Обновлено назначений: {updated}"