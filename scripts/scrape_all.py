#!/usr/bin/env python3
"""
Скачивание ВСЕХ текстов с пронумерованными предложениями с rus-ege.sdamgia.ru.

Задания 1-3 и 22-27 содержат тексты с разметкой вида (1)...(2)...
Скрипт:
  1. Удаляет старые HTML из data/raw/
  2. Собирает ID заданий из всех категорий
  3. Скачивает HTML-страницы
  4. Извлекает тексты, парсит предложения
  5. Дедуплицирует по data_text_id
  6. Сохраняет data/processed/sentences.jsonl

Использование:
  python scripts/scrape_all.py
  python scripts/scrape_all.py --keep-old   # не удалять старые HTML
  python scripts/scrape_all.py --dry-run    # только собрать ID, не скачивать
"""

import argparse
import hashlib
import json
import re
import shutil
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Конфигурация ───────────────────────────────────────────────────────────

BASE_URL = "https://rus-ege.sdamgia.ru"
CATALOG_URL = BASE_URL + "/test?filter=all&category_id={cat_id}&page={page}"
PROBLEM_URL = BASE_URL + "/problem?id={task_id}"

# Все категории заданий с нумерованными текстами (из каталога prob_catalog)
CATEGORIES = {
    # Задание 1: Средства связи предложений в тексте
    1: [365, 318, 289, 354],
    # Задание 2: Определение лексического значения слова
    2: [371, 316, 342, 355],
    # Задание 3: Стилистический анализ текстов
    3: [372, 357, 258, 356],
    # Задание 22: Языковые средства выразительности
    22: [400],
    # Задание 23: Смысловая и композиционная целостность текста
    23: [361, 393, 229, 312, 282],
    # Задание 24: Функционально-смысловые типы речи
    24: [366, 394, 230, 313, 283],
    # Задание 25: Лексическое значение слова
    25: [367, 395, 231, 314, 284],
    # Задание 26: Средства связи предложений в тексте
    26: [368, 396, 252, 321, 253],
    # Задание 27: Сочинение
    27: [399, 402],
}

ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"

DELAY = 1.5  # секунд между запросами
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


# ─── Сбор ID заданий ────────────────────────────────────────────────────────

def collect_task_ids_from_category(cat_id: int, max_pages: int = 50) -> list[int]:
    """Собираем ID заданий из одной категории каталога."""
    all_ids = []
    empty_streak = 0

    for page in range(1, max_pages + 1):
        url = CATALOG_URL.format(cat_id=cat_id, page=page)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"    стр. {page}: ошибка — {e}")
            empty_streak += 1
            if empty_streak >= 2:
                break
            time.sleep(DELAY)
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        links = soup.select('a[href*="/problem?id="]')
        ids_on_page = set()
        for a in links:
            m = re.search(r'id=(\d+)', a.get("href", ""))
            if m:
                ids_on_page.add(int(m.group(1)))

        if not ids_on_page:
            empty_streak += 1
            if empty_streak >= 2:
                break
        else:
            empty_streak = 0
            all_ids.extend(ids_on_page)

        time.sleep(DELAY)

    return sorted(set(all_ids))


def collect_all_task_ids() -> list[int]:
    """Собираем ID из всех категорий для заданий 1-3, 22-27."""
    all_ids = set()

    for zadanie_num, cat_ids in sorted(CATEGORIES.items()):
        print(f"\nЗадание {zadanie_num}:")
        for cat_id in cat_ids:
            ids = collect_task_ids_from_category(cat_id)
            new_ids = set(ids) - all_ids
            all_ids.update(ids)
            print(f"  категория {cat_id}: {len(ids)} ID ({len(new_ids)} новых)")

    result = sorted(all_ids)
    print(f"\nВсего уникальных ID: {len(result)}")
    return result


# ─── Скачивание HTML ────────────────────────────────────────────────────────

def fetch_task(task_id: int, retries: int = 3) -> tuple[str | None, bool]:
    """Скачивает HTML страницы задания. Atomic write + retry."""
    filepath = DATA_RAW / f"{task_id}.html"

    if filepath.exists() and filepath.stat().st_size > 100:
        return filepath.read_text(encoding="utf-8"), True

    url = PROBLEM_URL.format(task_id=task_id)
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            tmp = filepath.with_suffix(".tmp")
            tmp.write_text(resp.text, encoding="utf-8")
            tmp.rename(filepath)
            return resp.text, False
        except requests.RequestException as e:
            if attempt < retries - 1:
                wait = DELAY * (2 ** attempt)
                print(f"  Retry {attempt + 1}/{retries} для {task_id}: {e}, жду {wait:.1f}s")
                time.sleep(wait)
            else:
                print(f"  Ошибка {task_id} после {retries} попыток: {e}")
                return None, False


def download_all(task_ids: list[int]) -> None:
    """Скачиваем HTML для всех заданий."""
    downloaded, cached, errors = 0, 0, 0

    for i, task_id in enumerate(task_ids):
        html, was_cached = fetch_task(task_id)
        if html is None:
            errors += 1
        elif was_cached:
            cached += 1
        else:
            downloaded += 1
            time.sleep(DELAY)

        if (i + 1) % 50 == 0 or (i + 1) == len(task_ids):
            print(f"  Прогресс: {i + 1}/{len(task_ids)} "
                  f"(скачано: {downloaded}, кеш: {cached}, ошибок: {errors})")

    print(f"\nГотово! Скачано: {downloaded}, из кеша: {cached}, ошибок: {errors}")
    print(f"Файлов в data/raw/: {len(list(DATA_RAW.glob('*.html')))}")


# ─── Извлечение текста ───────────────────────────────────────────────────────

def extract_raw_text(html_content: str) -> tuple[str | None, str | None]:
    """Извлекает сырой текст с нумерацией из HTML страницы задания."""
    soup = BeautifulSoup(html_content, "lxml")
    probtext = soup.select_one("div.probtext")

    if not probtext:
        return None, None

    data_text_id = probtext.get("data-text_id")

    paragraphs = probtext.find_all("p")
    lines = []
    for p in paragraphs:
        first_child = next(p.children, None)
        if first_child and getattr(first_child, "name", None) == "sup":
            sup_text = first_child.get_text().strip()
            if sup_text.isdigit():
                continue

        for sup in p.select("sup"):
            sup.decompose()

        text = p.get_text()
        text = text.replace("\u00ad", "")
        text = text.replace("\u00a0", " ")
        text = text.replace("\u2061", "")
        text = text.replace("\u2060", "")
        text = text.replace("\u202f", " ")
        text = text.replace("\t", " ")
        text = text.strip()
        if not text:
            continue
        if re.match(r'^\([Пп]о\s', text) and not re.match(r'^\(\d+\)', text):
            break
        if text.startswith("Источник текста"):
            continue
        if text.startswith('*') and not re.match(r'^\(\d+\)', text):
            continue
        if re.match(r'^\d+[А-ЯЁа-яё]', text) and not re.match(r'^\(\d+\)', text):
            continue
        if re.match(r'^[А-ЯЁ][а-яё]+ [А-ЯЁ]', text) and re.search(r'\(\d{4}[−–\-]\d{4}\)', text):
            continue
        lines.append(text)

    if not lines:
        return None, data_text_id

    raw_text = " ".join(lines)

    # Post-process: inline-метаданные
    raw_text = re.sub(r'\s*\([Пп]о\s+(?:по\s+)?[^()]*[А-ЯЁ]\.[^()]*\)\s*$', '', raw_text)
    raw_text = re.sub(r'\s*\([Пп]о\s+[^()]+\*\s*\)\s*$', '', raw_text)
    raw_text = re.sub(r'\s*\*[А-ЯЁ][а-яё]+\s[А-ЯЁ].*?\(\d{4}[−–\-]\d{4}\).*$', '', raw_text)
    raw_text = re.sub(r'(?<=[.!?…»)"])\s+\d+[А-ЯЁ][а-яё]+\s*[—–\-]\s.*$', '', raw_text)
    raw_text = re.sub(r'\s*Источник текста:.*$', '', raw_text)
    raw_text = raw_text.strip()

    if not raw_text:
        return None, data_text_id

    return raw_text, data_text_id


def parse_sentences(raw_text: str) -> tuple[str, list[dict]]:
    """Парсит текст с маркерами (1)...(2)... в список предложений."""
    if not raw_text:
        return "", []

    all_markers = list(re.finditer(r'\((\d+)\)', raw_text))
    markers = [m for m in all_markers if int(m.group(1)) < 1000]

    if not markers:
        return raw_text, []

    remove_ranges = [(m.start(), m.end()) for m in markers]

    parts = []
    prev_end = 0
    for rs, re_ in remove_ranges:
        parts.append(raw_text[prev_end:rs])
        prev_end = re_
    parts.append(raw_text[prev_end:])
    clean_text = "".join(parts)
    clean_text = re.sub(r'  +', ' ', clean_text).strip()

    sentences = []
    min_pos = 0
    for i, m in enumerate(markers):
        idx = int(m.group(1))
        start_raw = m.end()
        end_raw = markers[i + 1].start() if i + 1 < len(markers) else len(raw_text)

        sent_text = raw_text[start_raw:end_raw].strip()
        if not sent_text:
            continue

        sent_text = re.sub(r'  +', ' ', sent_text)

        found = clean_text.find(sent_text, min_pos)
        if found == -1:
            continue

        start_clean = found
        end_clean = found + len(sent_text)
        min_pos = end_clean

        sentences.append({"idx": idx, "start": start_clean, "end": end_clean, "text": sent_text})

    return clean_text, sentences


# ─── Сборка датасета ─────────────────────────────────────────────────────────

def build_dataset() -> list[dict]:
    """Обрабатываем все HTML → извлекаем тексты → парсим → дедуплицируем."""
    dataset = []
    seen_text_ids = set()
    parse_errors = []

    html_files = sorted(DATA_RAW.glob("*.html"))
    print(f"\nHTML-файлов для обработки: {len(html_files)}")

    for filepath in html_files:
        task_id = filepath.stem
        html_content = filepath.read_text(encoding="utf-8")

        raw_text, data_text_id = extract_raw_text(html_content)

        if raw_text is None:
            parse_errors.append((task_id, "no probtext or empty text"))
            continue

        dedup_key = data_text_id if data_text_id else hashlib.md5(raw_text.encode()).hexdigest()
        if dedup_key in seen_text_ids:
            continue
        seen_text_ids.add(dedup_key)

        clean_text, sentences = parse_sentences(raw_text)

        if not sentences:
            parse_errors.append((task_id, "no sentences found"))
            continue

        record = {
            "id": f"text_{data_text_id}" if data_text_id else f"text_hash_{dedup_key[:8]}",
            "source": f"reshuege_{task_id}",
            "raw_text": raw_text,
            "clean_text": clean_text,
            "sentences": sentences,
            "num_sentences": len(sentences)
        }
        dataset.append(record)

    print(f"Записей в датасете: {len(dataset)}")
    print(f"Уникальных текстов (dedup): {len(seen_text_ids)}")
    if parse_errors:
        no_text = sum(1 for _, e in parse_errors if "no probtext" in e)
        no_sent = sum(1 for _, e in parse_errors if "no sentences" in e)
        print(f"Пропущено: {no_text} без текста, {no_sent} без предложений")

    return dataset


def validate_and_save(dataset: list[dict]) -> None:
    """Валидация позиций и сохранение в JSONL."""
    if not dataset:
        print("Dataset пуст — нечего сохранять")
        return

    total_errors = 0
    total_sentences = 0

    for record in dataset:
        ct = record["clean_text"]
        for s in record["sentences"]:
            total_sentences += 1
            actual = ct[s["start"]:s["end"]]
            if actual != s["text"]:
                total_errors += 1
                if total_errors <= 3:
                    print(f"ОШИБКА в {record['id']}, предложение {s['idx']}:")
                    print(f"  Ожидалось: {s['text'][:80]}")
                    print(f"  Получено:  {actual[:80]}")

    print(f"\nВалидация: {total_sentences} предложений, {total_errors} ошибок")

    # Статистика
    num_sents = [r["num_sentences"] for r in dataset]
    text_lens = [len(r["clean_text"]) for r in dataset]
    sent_lens = [len(s["text"]) for r in dataset for s in r["sentences"]]

    print(f"\n--- Статистика ---")
    print(f"Текстов: {len(dataset)}")
    print(f"Предложений всего: {sum(num_sents)}")
    print(f"Предложений на текст: min={min(num_sents)}, max={max(num_sents)}, "
          f"avg={sum(num_sents)/len(num_sents):.1f}")
    print(f"Длина текста (симв.): min={min(text_lens)}, max={max(text_lens)}, "
          f"avg={sum(text_lens)/len(text_lens):.0f}")
    print(f"Длина предложения (симв.): min={min(sent_lens)}, max={max(sent_lens)}, "
          f"avg={sum(sent_lens)/len(sent_lens):.0f}")

    # Сохранение
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    output_path = DATA_PROCESSED / "sentences.jsonl"
    tmp_path = output_path.with_suffix(".tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        for record in dataset:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    tmp_path.rename(output_path)

    print(f"\nСохранено в {output_path}")
    print(f"Размер: {output_path.stat().st_size / 1024:.1f} КБ")


# ─── Очистка ─────────────────────────────────────────────────────────────────

def clean_old_html() -> None:
    """Удаляет все старые HTML-файлы из data/raw/."""
    html_files = list(DATA_RAW.glob("*.html"))
    tmp_files = list(DATA_RAW.glob("*.tmp"))
    total = len(html_files) + len(tmp_files)

    if total == 0:
        print("data/raw/ уже пуста")
        return

    print(f"Удаляем {len(html_files)} HTML + {len(tmp_files)} tmp файлов из data/raw/...")
    for f in html_files + tmp_files:
        f.unlink()
    print("Готово")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Скачать все тексты с Решу ЕГЭ")
    parser.add_argument("--keep-old", action="store_true",
                        help="Не удалять старые HTML (по умолчанию удаляются)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Только собрать ID заданий, не скачивать")
    parser.add_argument("--skip-download", action="store_true",
                        help="Пропустить скачивание, обработать имеющиеся HTML")
    args = parser.parse_args()

    DATA_RAW.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    if not args.skip_download:
        # 1. Собираем все ID заданий
        print("=" * 60)
        print("ЭТАП 1: Сбор ID заданий из каталога")
        print("=" * 60)
        task_ids = collect_all_task_ids()

        if args.dry_run:
            print(f"\n[dry-run] Найдено {len(task_ids)} заданий. Выход.")
            return

        # 2. Удаляем старые файлы
        if not args.keep_old:
            print("\n" + "=" * 60)
            print("ЭТАП 2: Очистка старых HTML")
            print("=" * 60)
            clean_old_html()

        # 3. Скачиваем
        print("\n" + "=" * 60)
        print("ЭТАП 3: Скачивание HTML")
        print("=" * 60)
        download_all(task_ids)

    # 4. Сборка датасета
    print("\n" + "=" * 60)
    print("ЭТАП 4: Сборка и дедупликация датасета")
    print("=" * 60)
    dataset = build_dataset()

    # 5. Валидация и сохранение
    print("\n" + "=" * 60)
    print("ЭТАП 5: Валидация и сохранение")
    print("=" * 60)
    validate_and_save(dataset)


if __name__ == "__main__":
    main()
