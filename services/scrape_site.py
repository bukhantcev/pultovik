"""
services/site_scraper.py
Асинхронный парсер страницы с репертуаром.
Функции:
- fetch_html(url): скачать HTML с User-Agent и отключённой SSL-верификацией (часто ломают старые TLS на хостингах)
- parse_playbill_items(html, limit=None): достать тексты из всех .c-playbill--item
- join_items_as_prompt(items): удобно склеить карточки в один текст
- scrape_playbill(url, limit=None): главный пайплайн -> (items, combined)

Использование:
    from services.site_scraper import scrape_playbill
    items, combined = await scrape_playbill(url)

Зависимости: aiohttp, beautifulsoup4
    pip install aiohttp beautifulsoup4
"""
from __future__ import annotations
from typing import Tuple, List, Optional
import asyncio
import aiohttp
from bs4 import BeautifulSoup, NavigableString, Tag
from .ai_fill import build_excel_from_site

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)


async def fetch_html(url: str, timeout: int = 20) -> str:
    """
    Скачивает HTML по URL и возвращает текст.
    """
    connector = aiohttp.TCPConnector(ssl=False)
    headers = {"User-Agent": DEFAULT_UA}
    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        async with session.get(url, timeout=timeout) as resp:
            resp.raise_for_status()
            # Позволяем aiohttp самому определить кодировку по заголовкам/контенту
            return await resp.text(encoding=None)


def _clean_text(node: Tag) -> str:
    """
    Аккуратно собирает текст внутри узла, удаляя <script>/<style> и лишние пробелы.
    """
    # удалить скрипты/стили целиком
    for bad in node.find_all(["script", "style"]):
        bad.decompose()

    parts: List[str] = []
    for el in node.descendants:
        if isinstance(el, NavigableString):
            s = str(el).strip()
            if s:
                parts.append(s)

    text = " ".join(parts)
    # чуть причесать пунктуацию
    text = (
        text.replace(" ,", ",")
            .replace(" .", ".")
            .replace(" ;", ";")
            .replace(" :", ":")
    )
    # убрать множественные пробелы (вдруг остались)
    while "  " in text:
        text = text.replace("  ", " ")
    return text.strip()


def parse_playbill_items(html: str, limit: Optional[int] = None) -> List[str]:
    """
    Достаёт тексты из всех контейнеров с классом `.c-playbill--item`.
    Возвращает список строк, по одному элементу на карточку.
    """
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select(".c-playbill--item")
    if limit is not None:
        try:
            items = items[: max(0, int(limit))]
        except Exception:
            pass

    texts: List[str] = []
    for block in items:
        txt = _clean_text(block)
        if txt:
            texts.append(txt)
    return texts


def join_items_as_prompt(items: List[str]) -> str:
    """
    Склеивает карточки в один промптовый текст (разделено пустой строкой).
    """
    if not items:
        return ""
    return "\n\n".join(items)


async def scrape_playbill(url: str, limit: Optional[int] = None) -> Tuple[List[str], str]:
    """
    Главная функция парсера: тянет HTML, достаёт карточки и формирует общий текст.
    Возвращает кортеж: (список_карточек, общий_текст)
    """
    html = await fetch_html(url)
    items = parse_playbill_items(html, limit=limit)
    combined = join_items_as_prompt(items)
    return items, combined


# --- Высокоуровневая обёртка для site->excel ---
from typing import Optional

async def site_to_excel(url: str, month: int, year: int, limit: Optional[int] = None):
    """
    Высокоуровневая обёртка:
    1) тянет HTML по URL и извлекает все карточки .c-playbill--item,
    2) склеивает их в один текст,
    3) отправляет в OpenAI для сборки CSV по шаблону,
    4) конвертирует CSV в .xlsx через build_excel_from_site.

    Возвращает кортеж (fs_input_file, items_count).
    """
    items, combined = await scrape_playbill(url, limit=limit)
    if not combined.strip():
        # защита: даже если на странице ничего не нашли — вернём пустой .xlsx (получится только заголовок)
        combined = ""
    fs_file = await build_excel_from_site(combined, month=month, year=year, tmp_name="site")
    return fs_file, len(items)


# Локальный тест:
#   python -m services.site_scraper "https://mikhalkov12.ru/playbill/?month=9&year=2025" [limit]
if __name__ == "__main__":
    import sys

    async def _demo():
        if len(sys.argv) < 2:
            print("Usage: python -m services.site_scraper <URL> [limit]")
            return
        url = sys.argv[1]
        lim = int(sys.argv[2]) if len(sys.argv) > 2 else None
        items, combined = await scrape_playbill(url, limit=lim)
        print(f"Найдено карточек: {len(items)}")
        for i, t in enumerate(items, 1):
            print(f"\n--- ITEM #{i} ---\n{t}")
        print("\n=== COMBINED ===\n", combined[:2000], "..." if len(combined) > 2000 else "", sep="")

    asyncio.run(_demo())
    print("\nTIP: you can use site_to_excel(url, month, year) from bot handlers to get ready .xlsx")