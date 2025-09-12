# services/auto_assign.py
from config import ADMIN_ID, RU_MONTHS
from aiogram import Bot
import os
import asyncio
import calendar
# services/auto_assign.py
from db import DBI

TYPE_ORDER = {"монтаж":0, "репетиция":1, "репетиции":1, "спектакль":2}

def _normalize_type(tp: str | None) -> str:
    if not tp: return "спектакль"
    s = str(tp).strip().lower()
    if s.startswith("монтаж"): return "монтаж"
    if s.startswith("репет"):  return "репетиция"
    if s.startswith("спект"):  return "спектакль"
    return s

def _get_qualified_employee_ids(title: str) -> set[int]:
    return DBI.get_spectacle_employee_ids(title)

def _date_busy_map_for_employees(emp_ids: set[int]) -> dict[int, set[str]]:
    return {eid: set(DBI.list_busy_dates(eid)) for eid in emp_ids}

def _already_assigned_dates_map(events: list[dict]) -> dict[str, set[int]]:
    m: dict[str, set[int]] = {}
    for ev in events:
        d = ev.get("date"); disp = ev.get("employee")
        if not d or not disp: continue
        eid = DBI.get_employee_id(disp)
        if eid is not None:
            m.setdefault(d, set()).add(eid)
    return m

def _pick_employee_for_block(block: list[dict], qualified_ids: set[int], busy_map: dict[int, set[str]], assigned_dates: dict[str, set[int]], prefer_not: int | None = None) -> int | None:
    if not block or not qualified_ids: return None
    date = block[0].get("date")
    if not date: return None
    candidates = []
    for eid in qualified_ids:
        if date in busy_map.get(eid, set()): continue
        if eid in assigned_dates.get(date, set()): continue
        candidates.append(eid)
    if not candidates: return None
    try:
        y = int(date[0:4]); m = int(date[5:7])
    except Exception:
        y, m = 1970, 1
    scored = [(DBI.count_assigned_for_month(eid, y, m), eid) for eid in candidates]
    scored.sort()
    ordered = [eid for _, eid in scored]
    if prefer_not is not None and len(ordered) > 1 and ordered[0] == prefer_not:
        return ordered[1]
    return ordered[0]

def _update_event_employee_by_ids(event_ids: list[int], employee_id: int):
    with DBI._conn() as con:
        row = con.execute("SELECT display FROM employees WHERE id=?", (employee_id,)).fetchone()
        if not row: return
        disp = row[0]
        for eid in event_ids:
            con.execute("UPDATE events SET employee=? WHERE id=?", (disp, eid))
        con.commit()

# Set a literal value into the employee field for given event ids
def _update_event_employee_literal(event_ids: list[int], value: str):
    if not event_ids:
        return
    with DBI._conn() as con:
        for eid in event_ids:
            con.execute("UPDATE events SET employee=? WHERE id=?", (value, eid))
        con.commit()

def _all_employee_ids() -> list[int]:
    with DBI._conn() as con:
        rows = con.execute("SELECT id FROM employees ORDER BY last_name, first_name").fetchall()
        return [r[0] for r in rows]

def _employee_display_by_id(employee_id: int) -> str | None:
    with DBI._conn() as con:
        row = con.execute("SELECT display FROM employees WHERE id=?", (employee_id,)).fetchone()
        return row[0] if row else None

def _count_duty_for_month(employee_id: int, year: int, month: int) -> int:
    prefix = f"{year:04d}-{month:02d}-"
    with DBI._conn() as con:
        row = con.execute(
            "SELECT COUNT(*) FROM events WHERE duty_employee IS NOT NULL AND duty_employee <> '' AND duty_employee IN (SELECT display FROM employees WHERE id=?) AND date LIKE ?",
            (employee_id, prefix + '%'),
        ).fetchone()
        return int(row[0] if row and row[0] is not None else 0)

def _assigned_main_set_for_date(date_str: str) -> set[int]:
    """Возвращает множество id сотрудников, назначенных в поле employee на заданную дату."""
    with DBI._conn() as con:
        rows = con.execute(
            "SELECT DISTINCT e.id FROM events ev JOIN employees e ON e.display = ev.employee WHERE ev.date=?",
            (date_str,),
        ).fetchall()
        return {r[0] for r in rows}

def _set_duty_for_date(date_str: str, duty_display: str) -> int:
    """Ставит (или обновляет) дежурного на дату. Если событий нет — добавляет пустую запись дня.
    Возвращает число обновлённых/вставленных строк events."""
    updated = 0
    with DBI._conn() as con:
        cur = con.execute("UPDATE events SET duty_employee=? WHERE date=?", (duty_display, date_str))
        updated += cur.rowcount
        if updated == 0:
            # Нет событий в этот день — создаём отдельную запись дня
            con.execute(
                "INSERT INTO events(date, type, title, time, location, city, employee, info, duty_employee) VALUES(?,?,?,?,?,?,?,?,?)",
                (date_str, None, None, None, None, None, None, None, duty_display),
            )
            updated = 1
        con.commit()
    return updated

def _set_duty_literal(date_str: str, value: str) -> int:
    """Ставит буквальный текст в duty_employee на дату (или создаёт пустую запись дня)."""
    updated = 0
    with DBI._conn() as con:
        cur = con.execute("UPDATE events SET duty_employee=? WHERE date=?", (value, date_str))
        updated += cur.rowcount
        if updated == 0:
            con.execute(
                "INSERT INTO events(date, type, title, time, location, city, employee, info, duty_employee) VALUES(?,?,?,?,?,?,?,?,?)",
                (date_str, None, None, None, None, None, None, None, value),
            )
            updated = 1
        con.commit()
    return updated

def _pick_duty_for_date(date_str: str, candidate_ids: list[int], busy_map: dict[int, set[str]], forbidden_ids: set[int], year: int, month: int, prefer_not: int | None = None) -> int | None:
    """Выбираем дежурного на дату с учётом занятости и баланса (минимум рабочих дней = employee+дюти).
    forbidden_ids — те, кто уже назначен в employee в этот день (должны отличаться)."""
    # Отфильтровать занятых и запрещённых
    pool = []
    for eid in candidate_ids:
        if eid in forbidden_ids:
            continue
        if date_str in busy_map.get(eid, set()):
            continue
        pool.append(eid)
    if not pool:
        return None
    # Баланс: считаем employee + duty за месяц
    scored = []
    for eid in pool:
        main_cnt = DBI.count_assigned_for_month(eid, year, month)
        duty_cnt = _count_duty_for_month(eid, year, month)
        scored.append((main_cnt + duty_cnt, eid))
    scored.sort()
    # Избегаем подряд того же дежурного, если есть альтернатива
    if prefer_not is not None and len(scored) > 1 and scored[0][1] == prefer_not:
        # найдём первого, кто не равен prefer_not
        for _, cand in scored:
            if cand != prefer_not:
                return cand
    return scored[0][1] if scored else None

def assign_duty_for_month(year: int, month: int) -> int:
    """Назначает дежурного сотрудника на КАЖДЫЙ день месяца.
    Правила:
      - дежурный ≠ любой из назначенных по employee в этот день;
      - учитывать занятость (employee_busy);
      - не зависит от спектаклей/связок;
      - балансировать суммарные рабочие дни за месяц (employee + дежурства).
    Возвращает число затронутых строк events."""
    # Подготовим пул кандидатов и карту занятости
    all_ids = _all_employee_ids()
    busy_map = {eid: set(DBI.list_busy_dates(eid)) for eid in all_ids}

    total_updated = 0
    days_in_month = calendar.monthrange(year, month)[1]
    last_duty_id: int | None = None
    for d in range(1, days_in_month + 1):
        date_str = f"{year:04d}-{month:02d}-{d:02d}"
        forbidden = _assigned_main_set_for_date(date_str)
        eid = _pick_duty_for_date(date_str, all_ids, busy_map, forbidden, year, month, prefer_not=last_duty_id)
        if eid is None:
            # Полная недоступность — фиксируем «НАКЛАДКА!!!» в duty_employee
            total_updated += _set_duty_literal(date_str, "НАКЛАДКА!!!")
            last_duty_id = None
            continue
        disp = _employee_display_by_id(eid)
        if not disp:
            last_duty_id = None
            continue
        total_updated += _set_duty_for_date(date_str, disp)
        last_duty_id = eid
    return total_updated

async def _notify_admin_summary(text: str):
    token = os.getenv("BOT_TOKEN")
    if not (token and ADMIN_ID):
        return
    # Open and close client session properly
    async with Bot(token=token) as bot:
        try:
            await bot.send_message(ADMIN_ID, text)
        except Exception as e:
            print("Failed to send summary to admin:", e)

def auto_assign_events_for_month(year: int | None = None, month: int | None = None) -> int:
    with DBI._conn() as con:
        if year and month:
            prefix = f"{year:04d}-{month:02d}-"
            rows = con.execute("SELECT id, date, type, title, city, employee FROM events WHERE date LIKE ?", (prefix+'%',)).fetchall()
        else:
            rows = con.execute("SELECT id, date, type, title, city, employee FROM events").fetchall()
    blocks: dict[tuple, list[dict]] = {}
    for eid, d, tp, title, city, emp in rows:
        k = (d, _normalize_type(tp), title, (city or '').strip())
        blocks.setdefault(k, []).append({'id': eid, 'date': d, 'type': tp, 'title': title, 'city': city, 'employee': emp})

    updated = 0
    all_titles = {t for (_, _, t, _) in blocks.keys() if t}
    all_emp_ids = set()
    for t in all_titles:
        all_emp_ids |= _get_qualified_employee_ids(t)
    busy_map = _date_busy_map_for_employees(all_emp_ids)
    assigned_dates = _already_assigned_dates_map([ev for v in blocks.values() for ev in v])
    last_moscow: dict[str, int] = {}

    def sort_key(k):  # (date, type, title, city)
        d, tp, title, city = k
        return (d, title or "", TYPE_ORDER.get(_normalize_type(tp), 99))

    for k in sorted(blocks, key=sort_key):
        date, tp, title, city = k
        block = blocks[k]
        if all(ev.get("employee") for ev in block):
            continue
        qualified = _get_qualified_employee_ids(title)
        prefer_not = last_moscow.get(title) if (city or '').strip().lower() == 'москва' else None
        eid = _pick_employee_for_block(block, qualified, busy_map, assigned_dates, prefer_not=prefer_not)
        if eid is None:
            # Если есть квалифицированные, но все заняты/недоступны, ставим пометку "НАКЛАДКА!!!"
            if qualified:
                # дата блока
                date = block[0].get("date")
                # есть ли хоть один свободный на эту дату
                available = [q for q in qualified if (date not in busy_map.get(q, set()) and q not in assigned_dates.get(date or "", set()))]
                if not available:
                    ids_to_update = [ev['id'] for ev in block if not ev.get('employee')]
                    if ids_to_update:
                        _update_event_employee_literal(ids_to_update, "НАКЛАДКА!!!")
                        updated += len(ids_to_update)
            continue
        ids_to_update = [ev['id'] for ev in block if not ev.get('employee')]
        _update_event_employee_by_ids(ids_to_update, eid)
        updated += len(ids_to_update)
        assigned_dates.setdefault(date, set()).add(eid)
        busy_map.setdefault(eid, set()).add(date)
        if (city or '').strip().lower() == 'москва':
            last_moscow[title] = eid
    # Подготовка отчёта по загруженности: исполнители + дежурства
    summary: dict[int, tuple[int,int]] = {}
    # Соберём всех сотрудников, участвовавших в этом месяце (по связям спектаклей) — или всех, если хочется полный отчёт
    all_emp_ids_for_report = _all_employee_ids()
    if year and month:
        for eid in all_emp_ids_for_report:
            main_cnt = DBI.count_assigned_for_month(eid, year, month)
            duty_cnt = _count_duty_for_month(eid, year, month)
            if (main_cnt + duty_cnt) > 0:
                summary[eid] = (main_cnt, duty_cnt)

    if summary and year and month:
        month_title = f"{RU_MONTHS[month-1]} {year}"
        lines = [month_title]
        items = []
        with DBI._conn() as con:
            for eid, (main_cnt, duty_cnt) in summary.items():
                row = con.execute("SELECT last_name, first_name FROM employees WHERE id=?", (eid,)).fetchone()
                if row:
                    ln, fn = row
                    items.append((ln or "", fn or "", main_cnt, duty_cnt))
        for ln, fn, main_cnt, duty_cnt in sorted(items):
            total = main_cnt + duty_cnt
            lines.append(f"{ln} {fn} — {total} (дежурств: {duty_cnt})")
        text = "\n".join(lines)
        asyncio.create_task(_notify_admin_summary(text))

    # После назначения исполнителей — назначим дежурных на каждый день выбранного месяца
    if year and month:
        try:
            duty_updated = assign_duty_for_month(year, month)
            updated += duty_updated
        except Exception as _e:
            # не валим общий процесс, просто логируем
            print("assign_duty_for_month failed:", _e)

    return updated