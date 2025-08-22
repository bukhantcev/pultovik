# config.py
from __future__ import annotations
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

# --- paths
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

# --- .env
if load_dotenv:
    load_dotenv(ROOT_DIR / ".env")

# --- OpenAI
OPENAI_API_KEY: str = (os.getenv("OPENAI_API_KEY") or "").strip()
GPT_MODEL: str = (os.getenv("GPT_MODEL") or "gpt-4o-mini").strip()
ENABLE_AI_FILL: bool = (os.getenv("ENABLE_AI_FILL", "1").strip().lower() not in {"0", "false", "no"})

BOT_TOKEN: str = (os.getenv("BOT_TOKEN") or "").strip()
ADMIN_ID: int | None = None
_aid = (os.getenv("ADMIN_ID") or "").strip()
if _aid.isdigit():
    ADMIN_ID = int(_aid)

DB_PATH = str(os.getenv("BOT_DB") or (ROOT_DIR / "bot.db"))

# --- locale
RU_MONTHS = [
    "Январь","Февраль","Март","Апрель","Май","Июнь",
    "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь",
]
RU_MONTHS_GEN = [
    "января","февраля","марта","апреля","мая","июня",
    "июля","августа","сентября","октября","ноября","декабря",
]

def is_admin(user_id: int | None) -> bool:
    return ADMIN_ID is not None and user_id == ADMIN_ID