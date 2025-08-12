import asyncio
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode, quote

import pandas as pd
from playwright.async_api import async_playwright

# ====== файлы и базовые настройки ======

MENU_FILE = "menu.json"
SUBJECTS_FILE = "leaf_subjects.json"
OUT_XLSX = "wb_categories.xlsx"

BASE_WIDTH = 12
NAME_WIDTH = BASE_WIDTH * 5

# ====== 1) загрузка JSON меню с сайта ======

MENU_HINTS = ("main-menu", "v3")


def looks_like_menu_json(data: Any) -> bool:
    """Проверяет, что это JSON главного меню WB по форме."""
    return isinstance(data, list) and any(
        isinstance(x, dict) and "id" in x and "name" in x for x in data[:5]
    )


def is_menu_url(url: str) -> bool:
    """Определяет, что URL указывает на JSON меню WB."""
    return url.endswith(".json") and all(h in url for h in MENU_HINTS)


async def fetch_menu(output_path: str = MENU_FILE, timeout_sec: int = 10) -> None:
    """Открывает WB, перехватывает JSON меню из Network и сохраняет в файл."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123 Safari/537.36"
            ),
            locale="ru-RU",
        )
        page = await ctx.new_page()

        fut = asyncio.get_running_loop().create_future()

        async def on_response(resp):
            if resp.request.resource_type in ("xhr", "fetch") and is_menu_url(resp.url):
                data = await resp.json()
                if looks_like_menu_json(data) and not fut.done():
                    fut.set_result(data)

        page.on("response", on_response)
        await page.goto("https://www.wildberries.ru/", wait_until="domcontentloaded", timeout=15000)

        data = await asyncio.wait_for(fut, timeout=timeout_sec)
        Path(output_path).write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        await browser.close()

# ====== 2) сбор предметов для листовых категорий ======

BASE_HOST = "https://search.wb.ru"
BASE_PATH = "/exactmatch/ru/common/v18/search"
EXPAND_PARAMS = {
    "appType": "1",
    "curr": "rub",
    "dest": "-1257786",
    "hide_dtype": "13",
    "lang": "ru",
    "filters": "ffsubject",
    "resultset": "filters",
    "spp": "30",
}


def load_json(path: str):
    """Читает JSON-файл и возвращает объект Python."""
    return json.loads(Path(path).read_text(encoding="utf-8"))


def load_menu(path: str) -> List[Dict]:
    """Читает menu.json и возвращает корневые узлы каталога."""
    return load_json(path)


def iter_leaves(tree: List[Dict]) -> List[Dict]:
    """Возвращает все листовые категории."""
    out: List[Dict] = []
    stack = list(tree)
    while stack:
        node = stack.pop()
        childs = node.get("childs") or []
        if childs:
            stack.extend(childs)
        else:
            out.append(node)
    return out


def get_leaf_info(node: Dict) -> Tuple[int, str, str, str]:
    """Извлекает id, имя, полный URL и searchQuery для листа."""
    leaf_id = int(node["id"])
    leaf_name = str(node.get("name", "")).strip()
    leaf_url = str(node.get("url", "")).strip()
    leaf_full_url = "https://www.wildberries.ru" + leaf_url if leaf_url.startswith("/") else leaf_url
    q = node["searchQuery"]
    return leaf_id, leaf_name, leaf_full_url, q


def build_filters_url(query: str) -> str:
    """Собирает URL поиска WB с расширенным фильтром (filters=ffsubject)."""
    params = dict(EXPAND_PARAMS)
    params["query"] = query
    return f"{BASE_HOST}{BASE_PATH}?{urlencode(params, quote_via=quote)}"


def extract_subjects(payload: Dict) -> List[Dict]:
    """Из ответа WB берёт фасет xsubject и возвращает [{'id','name'}, ...]."""
    for f in payload["data"]["filters"]:
        if f.get("key") == "xsubject":
            return [{"id": it["id"], "name": it["name"]} for it in f.get("items", [])]
    return []


async def fetch_json(ctx, url: str, *, referer: str) -> Dict:
    """GET через Playwright, возвращает распарсенный JSON (в т.ч. text/plain)."""
    resp = await ctx.request.get(
        url,
        headers={
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Referer": referer,
        },
        timeout=8000,
    )
    try:
        return await resp.json()
    except Exception:
        txt = await resp.text()
        return json.loads(txt.lstrip("\ufeff \t\r\n"))


async def fetch_subjects_for_leaf(ctx, node: Dict) -> Dict:
    """Для одного листа запрашивает фильтры и формирует запись с предметами."""
    leaf_id, leaf_name, leaf_full_url, q = get_leaf_info(node)
    record = {
        "leaf_id": leaf_id,
        "leaf_name": leaf_name,
        "leaf_full_url": leaf_full_url,
        "subjects": [],
        "error": None,
    }
    try:
        url = build_filters_url(q)
        data = await fetch_json(ctx, url, referer=leaf_full_url)
        record["subjects"] = extract_subjects(data)
    except Exception as e:
        record["error"] = str(e)
    return record


def select_leaves(menu: List[Dict]) -> List[Dict]:
    """Отбирает листья из /catalog с заданным searchQuery."""
    out: List[Dict] = []
    for node in iter_leaves(menu):
        url = str(node.get("url", ""))
        if url.startswith("/catalog") and node.get("searchQuery"):
            out.append(node)
    return out


async def collect_subjects(concurrency: int = 24) -> None:
    """Собирает предметы по всем листам и пишет leaf_subjects.json."""
    menu = load_menu(MENU_FILE)
    leaves = select_leaves(menu)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123 Safari/537.36"
            ),
            locale="ru-RU",
        )

        sem = asyncio.Semaphore(concurrency)

        async def worker(n: Dict) -> Dict:
            async with sem:
                return await fetch_subjects_for_leaf(ctx, n)

        tasks = [asyncio.create_task(worker(n)) for n in leaves]
        results: List[Dict] = []
        for coro in asyncio.as_completed(tasks):
            results.append(await coro)

        await browser.close()

    Path(SUBJECTS_FILE).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")


def leaves_stats(path: Path) -> Tuple[int, int]:
    """Считает (всего, без ошибок) по leaf_subjects.json."""
    data = json.loads(path.read_text(encoding="utf-8"))
    total = len(data)
    ok = sum(1 for r in data if not r.get("error"))
    return total, ok

# ====== 3) экспорт в Excel ======

def build_paths_by_root(menu: List[Dict]) -> Dict[Tuple[int, str], List[List[Tuple[int, str, int]]]]:
    """Строит пути до листьев по каждому корню (id, name, level на каждом шаге)."""
    out: Dict[Tuple[int, str], List[List[Tuple[int, str, int]]]] = {}

    def walk(node: Dict, level: int, acc: List[Tuple[int, str, int]], root_key: Tuple[int, str]):
        cur = (int(node["id"]), str(node.get("name", "")), level)
        path = acc + [cur]
        childs = node.get("childs") or []
        if childs:
            for ch in childs:
                walk(ch, level + 1, path, root_key)
        else:
            out.setdefault(root_key, []).append(path)

    for root in menu:
        root_key = (int(root["id"]), str(root.get("name", "")))
        walk(root, 0, [], root_key)
    return out


def subjects_map(items: List[Dict]) -> Dict[int, List[Dict]]:
    """Создаёт словарь: leaf_id → список предметов."""
    res: Dict[int, List[Dict]] = {}
    for r in items:
        if not r.get("error"):
            res[int(r["leaf_id"])] = r.get("subjects", []) or []
    return res


def safe_sheet_name(root_name: str) -> str:
    """Имя листа Excel '<Корень> – Категории' (≤ 31 символ)."""
    suffix = " – Категории"
    return (root_name[: 31 - len(suffix)] + suffix)[:31]


def build_rows(paths: List[List[Tuple[int, str, int]]],
               subj_by_leaf: Dict[int, List[Dict]]) -> List[Tuple[int, str, int]]:
    """Формирует строки: родители(>0) → лист(>0) → предметы(level=99)."""
    rows: List[Tuple[int, str, int]] = []
    for path in paths:
        parents = path[1:-1]
        leaf_id, leaf_name, leaf_level = path[-1]
        for pid, pname, plevel in parents:
            if plevel > 0:
                rows.append((pid, pname, plevel))
        if leaf_level > 0:
            rows.append((leaf_id, leaf_name, leaf_level))
        for s in subj_by_leaf.get(leaf_id, []):
            rows.append((int(s["id"]), str(s["name"]), 99))
    return rows


def make_excel(menu_path: str = MENU_FILE, subj_path: str = SUBJECTS_FILE, out_path: str = OUT_XLSX) -> None:
    """Создаёт Excel: по корню — лист; колонки id, name, level с нужной шириной."""
    menu = load_json(menu_path)
    subj_by_leaf = subjects_map(load_json(subj_path))
    paths_by_root = build_paths_by_root(menu)

    with pd.ExcelWriter(out_path, engine="openpyxl") as xw:
        for (_, root_name), paths in paths_by_root.items():
            rows = build_rows(paths, subj_by_leaf)
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=["id", "name", "level"])
            sheet = safe_sheet_name(root_name)
            df.to_excel(xw, sheet_name=sheet, index=False)

            ws = xw.sheets[sheet]
            ws.column_dimensions["A"].width = BASE_WIDTH   # id
            ws.column_dimensions["B"].width = NAME_WIDTH   # name
            ws.column_dimensions["C"].width = BASE_WIDTH   # level

# ====== Оркестратор ======

async def run_all(concurrency: int = 24) -> None:
    """Запускает все шаги и печатает времена и скорость."""
    t_all = time.perf_counter()

    t0 = time.perf_counter()
    await fetch_menu(MENU_FILE)
    t_fetch = time.perf_counter() - t0
    print(f"[01] Меню: {t_fetch:.2f} с → {MENU_FILE}")

    t0 = time.perf_counter()
    await collect_subjects(concurrency=concurrency)
    t_collect = time.perf_counter() - t0
    total, ok = leaves_stats(Path(SUBJECTS_FILE))
    speed = (total / t_collect) if t_collect > 0 else 0.0
    print(f"[02] Предметы: {t_collect:.2f} с, {ok}/{total} ок ({speed:.1f} лист/с) → {SUBJECTS_FILE}")

    t0 = time.perf_counter()
    make_excel(MENU_FILE, SUBJECTS_FILE, OUT_XLSX)
    t_xlsx = time.perf_counter() - t0
    print(f"[03] Excel: {t_xlsx:.2f} с → {OUT_XLSX}")

    print(f"ИТОГО: {(time.perf_counter() - t_all):.2f} с")


if __name__ == "__main__":
    asyncio.run(run_all())
