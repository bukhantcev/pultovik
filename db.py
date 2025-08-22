# db.py
import sqlite3
from datetime import datetime, UTC
from typing import List
from config import DB_PATH

class DB:
    def __init__(self, path: str):
        self.path = path
        self._ensure()

    def _conn(self):
        return sqlite3.connect(self.path)

    def _ensure(self):
        with self._conn() as con:
            cur = con.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS spectacles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT UNIQUE NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_name TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    tg_id TEXT,
                    display TEXT UNIQUE NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS spectacle_employees (
                    spectacle_id INTEGER NOT NULL,
                    employee_id INTEGER NOT NULL,
                    UNIQUE(spectacle_id, employee_id),
                    FOREIGN KEY(spectacle_id) REFERENCES spectacles(id) ON DELETE CASCADE,
                    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS employee_busy (
                    employee_id INTEGER NOT NULL,
                    date_str TEXT NOT NULL,
                    UNIQUE(employee_id, date_str),
                    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_submissions (
                    employee_id INTEGER NOT NULL,
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    submitted_at TEXT NOT NULL,
                    UNIQUE(employee_id, year, month),
                    FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS busy_window (
                    year INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    opened_at TEXT,
                    broadcast_sent INTEGER DEFAULT 0,
                    PRIMARY KEY(year, month)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    type TEXT,
                    title TEXT,
                    time TEXT,
                    location TEXT,
                    city TEXT,
                    employee TEXT,
                    info TEXT
                )
            """)
            con.commit()

    # --- windows / broadcast
    def ensure_window(self, year: int, month: int):
        with self._conn() as con:
            con.execute(
                "INSERT OR IGNORE INTO busy_window(year, month, opened_at) VALUES(?,?,?)",
                (year, month, datetime.now(UTC).isoformat()),
            )
            con.commit()

    def get_window(self, year: int, month: int):
        with self._conn() as con:
            return con.execute(
                "SELECT year, month, opened_at, broadcast_sent FROM busy_window WHERE year=? AND month=?",
                (year, month),
            ).fetchone()

    def mark_broadcast_sent(self, year: int, month: int):
        with self._conn() as con:
            con.execute("UPDATE busy_window SET broadcast_sent=1 WHERE year=? AND month=?", (year, month))
            con.commit()

    def list_employees_with_tg(self) -> list[tuple[int, str, int]]:
        with self._conn() as con:
            rows = con.execute("SELECT id, display, tg_id FROM employees WHERE tg_id IS NOT NULL AND LENGTH(tg_id)>0").fetchall()
            out = []
            for i, d, tg in rows:
                try:
                    out.append((i, d, int(tg)))
                except Exception:
                    continue
            return out

    # --- spectacles / employees
    def list_spectacles(self) -> List[str]:
        with self._conn() as con:
            return [r[0] for r in con.execute("SELECT title FROM spectacles ORDER BY title").fetchall()]

    def list_spectacles_with_ids(self) -> list[tuple[int, str]]:
        with self._conn() as con:
            rows = con.execute("SELECT id, title FROM spectacles ORDER BY title").fetchall()
            return [(r[0], r[1]) for r in rows]

    def upsert_spectacle(self, title: str):
        with self._conn() as con:
            con.execute("INSERT OR IGNORE INTO spectacles(title) VALUES(?)", (title,))
            con.commit()

    def get_spectacle_id(self, title: str):
        with self._conn() as con:
            row = con.execute("SELECT id FROM spectacles WHERE title=?", (title,)).fetchone()
            return row[0] if row else None

    def list_employees(self) -> List[str]:
        with self._conn() as con:
            return [r[0] for r in con.execute("SELECT display FROM employees ORDER BY last_name, first_name").fetchall()]

    def list_employees_full(self) -> list[tuple[int, str]]:
        with self._conn() as con:
            return [(r[0], r[1]) for r in con.execute("SELECT id, display FROM employees ORDER BY last_name, first_name").fetchall()]

    def upsert_employee(self, last_name: str, first_name: str, tg_id: str | None = None):
        last_name = (last_name or '').strip()
        first_name = (first_name or '').strip()
        if not last_name or not first_name:
            raise ValueError("Фамилия и Имя обязательны")
        display = f"{last_name} {first_name}".strip()
        with self._conn() as con:
            con.execute("INSERT OR IGNORE INTO employees(last_name, first_name, tg_id, display) VALUES(?,?,?,?)", (last_name, first_name, tg_id, display))
            if tg_id:
                con.execute("UPDATE employees SET tg_id=? WHERE display=?", (tg_id, display))
            con.commit()

    def delete_employee(self, employee_id: int) -> None:
        with self._conn() as con:
            con.execute("DELETE FROM employees WHERE id=?", (employee_id,))
            con.commit()

    def set_employee_tg_by_id(self, employee_id: int, tg_id: str | None):
        with self._conn() as con:
            con.execute("UPDATE employees SET tg_id=? WHERE id=?", (tg_id, employee_id))
            con.commit()

    def get_employee_id(self, display: str):
        with self._conn() as con:
            row = con.execute("SELECT id FROM employees WHERE display=?", (display,)).fetchone()
            return row[0] if row else None

    def get_employee_by_tg(self, tg_id: int | str):
        with self._conn() as con:
            return con.execute("SELECT id, display FROM employees WHERE tg_id=?", (str(tg_id),)).fetchone()

    def set_spectacle_employees(self, title: str, employee_names: List[str]):
        self.upsert_spectacle(title)
        with self._conn() as con:
            sid = con.execute("SELECT id FROM spectacles WHERE title=?", (title,)).fetchone()[0]
            con.execute("DELETE FROM spectacle_employees WHERE spectacle_id=?", (sid,))
            for nm in employee_names:
                row = con.execute("SELECT id FROM employees WHERE display=?", (nm,)).fetchone()
                if row:
                    con.execute("INSERT OR IGNORE INTO spectacle_employees(spectacle_id, employee_id) VALUES(?,?)", (sid, row[0]))
            con.commit()

    def get_spectacle_employees(self, title: str) -> List[str]:
        with self._conn() as con:
            row = con.execute("SELECT id FROM spectacles WHERE title=?", (title,)).fetchone()
            if not row:
                return []
            sid = row[0]
            rows = con.execute("""
                SELECT e.display FROM spectacle_employees se
                JOIN employees e ON e.id = se.employee_id
                WHERE se.spectacle_id=?
                ORDER BY e.last_name, e.first_name
            """, (sid,)).fetchall()
            return [r[0] for r in rows]

    def get_spectacle_employee_ids(self, title: str) -> set[int]:
        with self._conn() as con:
            row = con.execute("SELECT id FROM spectacles WHERE title=?", (title,)).fetchone()
            if not row:
                return set()
            sid = row[0]
            return {r[0] for r in con.execute("SELECT employee_id FROM spectacle_employees WHERE spectacle_id=?", (sid,)).fetchall()}

    def toggle_spectacle_employee(self, spectacle_id: int, employee_id: int) -> None:
        with self._conn() as con:
            exists = con.execute("SELECT 1 FROM spectacle_employees WHERE spectacle_id=? AND employee_id=?", (spectacle_id, employee_id)).fetchone()
            if exists:
                con.execute("DELETE FROM spectacle_employees WHERE spectacle_id=? AND employee_id=?", (spectacle_id, employee_id))
            else:
                con.execute("INSERT OR IGNORE INTO spectacle_employees(spectacle_id, employee_id) VALUES(?,?)", (spectacle_id, employee_id))
            con.commit()

    # --- busy dates
    def add_busy_dates(self, employee_id: int, dates: list[str]) -> list[str]:
        added = []
        with self._conn() as con:
            for ds in dates:
                cur = con.execute("INSERT OR IGNORE INTO employee_busy(employee_id, date_str) VALUES(?, ?)", (employee_id, ds))
                if cur.rowcount:
                    added.append(ds)
            con.commit()
        return added

    def list_busy_dates(self, employee_id: int) -> list[str]:
        with self._conn() as con:
            return [r[0] for r in con.execute("SELECT date_str FROM employee_busy WHERE employee_id=? ORDER BY date_str", (employee_id,)).fetchall()]

    def remove_busy_dates(self, employee_id: int, dates: list[str]) -> list[str]:
        removed = []
        with self._conn() as con:
            for ds in dates:
                cur = con.execute("DELETE FROM employee_busy WHERE employee_id=? AND date_str=?", (employee_id, ds))
                if cur.rowcount:
                    removed.append(ds)
            con.commit()
        return removed

    def clear_busy_dates(self, employee_id: int) -> None:
        with self._conn() as con:
            con.execute("DELETE FROM employee_busy WHERE employee_id=?", (employee_id,))
            con.commit()

    def set_submitted(self, employee_id: int, year: int, month: int):
        with self._conn() as con:
            con.execute("INSERT OR IGNORE INTO user_submissions(employee_id, year, month, submitted_at) VALUES(?,?,?,?)",
                        (employee_id, year, month, datetime.now(UTC).isoformat()))
            con.commit()

    def unset_submitted(self, employee_id: int, year: int, month: int):
        with self._conn() as con:
            con.execute("DELETE FROM user_submissions WHERE employee_id=? AND year=? AND month=?", (employee_id, year, month))
            con.commit()

    def has_submitted(self, employee_id: int, year: int, month: int) -> bool:
        with self._conn() as con:
            return bool(con.execute("SELECT 1 FROM user_submissions WHERE employee_id=? AND year=? AND month=?",(employee_id, year, month)).fetchone())

    def count_busy_for_month(self, employee_id: int, year: int, month: int) -> int:
        prefix = f"{year:04d}-{month:02d}-"
        with self._conn() as con:
            row = con.execute("SELECT COUNT(*) FROM employee_busy WHERE employee_id=? AND date_str LIKE ?", (employee_id, prefix+'%')).fetchone()
            return int(row[0] if row and row[0] is not None else 0)

    def count_assigned_for_month(self, employee_id: int, year: int, month: int) -> int:
        prefix = f"{year:04d}-{month:02d}-"
        with self._conn() as con:
            row = con.execute("""
                SELECT COUNT(*) FROM events ev
                JOIN employees e ON e.display = ev.employee
                WHERE e.id=? AND ev.date LIKE ?
            """, (employee_id, prefix+'%')).fetchone()
            return int(row[0] if row and row[0] is not None else 0)

    # --- events
    def delete_events_for_month(self, year: int, month: int):
        prefix = f"{year:04d}-{month:02d}-"
        with self._conn() as con:
            con.execute("DELETE FROM events WHERE date LIKE ?", (prefix + '%',))
            con.commit()

    def insert_events(self, rows: list[dict]):
        if not rows:
            return
        with self._conn() as con:
            con.executemany("""
                INSERT INTO events(date, type, title, time, location, city, employee, info)
                VALUES(:date, :type, :title, :time, :location, :city, :employee, :info)
            """, rows)
            con.commit()

DBI = DB(DB_PATH)