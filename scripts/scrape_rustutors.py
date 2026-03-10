#!/usr/bin/env python3
"""
Скачивание текстов ЕГЭ с rustutors.ru/vsetekstiege/ и добавление
в существующий датасет с дедупликацией.

Дедупликация:
  1. Точное совпадение: хеш нормализованного текста (MD5)
  2. Нечёткое совпадение: триграммная Jaccard similarity > 0.85

Использование:
  python scripts/scrape_rustutors.py
  python scripts/scrape_rustutors.py --dry-run        # только собрать URL, не скачивать
  python scripts/scrape_rustutors.py --skip-download   # обработать имеющиеся HTML
  python scripts/scrape_rustutors.py --threshold 0.9   # порог similarity (по умолчанию 0.85)
"""

import argparse
import hashlib
import json
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Конфигурация ───────────────────────────────────────────────────────────

BASE_URL = "https://rustutors.ru"
LISTING_URL = BASE_URL + "/vsetekstiege/page/{page}/"

ROOT = Path(__file__).resolve().parent.parent
DATA_RAW_RUSTUTORS = ROOT / "data" / "raw_rustutors"
DATA_PROCESSED = ROOT / "data" / "processed"

DELAY = 1.5
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
}


# ─── Сбор URL текстов ───────────────────────────────────────────────────────

def collect_text_urls(max_pages: int = 50) -> list[str]:
    """Собираем URL всех текстов из каталога rustutors.ru."""
    all_urls = []
    empty_streak = 0

    for page in range(1, max_pages + 1):
        url = LISTING_URL.format(page=page)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 404:
                print(f"  стр. {page}: 404 — конец каталога")
                break
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  стр. {page}: ошибка — {e}")
            empty_streak += 1
            if empty_streak >= 2:
                break
            time.sleep(DELAY)
            continue

        soup = BeautifulSoup(resp.text, "lxml")
        # Ссылки на тексты: /vsetekstiege/CATEGORY/NNN-TEXT.html
        links = soup.select('a[href*="/vsetekstiege/"]')
        urls_on_page = set()
        for a in links:
            href = a.get("href", "")
            # Фильтруем: только конечные страницы текстов (.html), не листинги
            if href.endswith(".html") and "/vsetekstiege/" in href:
                full_url = href if href.startswith("http") else BASE_URL + href
                urls_on_page.add(full_url)

        if not urls_on_page:
            empty_streak += 1
            if empty_streak >= 2:
                print(f"  стр. {page}: пусто ({empty_streak}/2), стоп")
                break
        else:
            empty_streak = 0
            new_urls = [u for u in urls_on_page if u not in set(all_urls)]
            all_urls.extend(new_urls)
            print(f"  стр. {page}: {len(urls_on_page)} ссылок ({len(new_urls)} новых)")

        time.sleep(DELAY)

    result = list(dict.fromkeys(all_urls))  # сохраняем порядок, убираем дупли
    print(f"\nВсего уникальных URL: {len(result)}")
    return result


# ─── Скачивание HTML ────────────────────────────────────────────────────────

def url_to_filename(url: str) -> str:
    """URL → имя файла: берём последнюю часть пути."""
    path = url.rstrip("/").split("/")[-1]
    return path if path.endswith(".html") else path + ".html"


def fetch_page(url: str, retries: int = 3) -> tuple[str | None, bool]:
    """Скачивает HTML страницы текста."""
    filename = url_to_filename(url)
    filepath = DATA_RAW_RUSTUTORS / filename

    if filepath.exists() and filepath.stat().st_size > 100:
        return filepath.read_text(encoding="utf-8"), True

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
                print(f"  Retry {attempt + 1}/{retries}: {e}, жду {wait:.1f}s")
                time.sleep(wait)
            else:
                print(f"  Ошибка {filename}: {e}")
                return None, False


def download_all(urls: list[str]) -> None:
    """Скачиваем все страницы текстов."""
    downloaded, cached, errors = 0, 0, 0

    for i, url in enumerate(urls):
        html, was_cached = fetch_page(url)
        if html is None:
            errors += 1
        elif was_cached:
            cached += 1
        else:
            downloaded += 1
            time.sleep(DELAY)

        if (i + 1) % 20 == 0 or (i + 1) == len(urls):
            print(f"  Прогресс: {i + 1}/{len(urls)} "
                  f"(скачано: {downloaded}, кеш: {cached}, ошибок: {errors})")

    print(f"\nГотово! Скачано: {downloaded}, из кеша: {cached}, ошибок: {errors}")


# ─── Извлечение текста из rustutors ─────────────────────────────────────────

def extract_raw_text_rustutors(html_content: str) -> str | None:
    """Извлекает сырой текст с нумерацией из страницы rustutors.ru."""
    soup = BeautifulSoup(html_content, "lxml")

    # Основной контент: div.full_story внутри div.article-inner
    container = soup.select_one("div.full_story")
    if not container:
        container = soup.select_one(".article-inner") or soup.select_one("#dle-content")
    if not container:
        return None

    # Удаляем <sup> (сноски)
    for sup in container.select("sup"):
        sup.decompose()

    # Берём полный текст контейнера
    full_text = container.get_text()
    full_text = full_text.replace("\u00ad", "")
    full_text = full_text.replace("\u00a0", " ")
    full_text = full_text.replace("\u2061", "")
    full_text = full_text.replace("\u2060", "")
    full_text = full_text.replace("\u202f", " ")
    full_text = full_text.replace("\t", " ")

    # Находим начало нумерованного текста: первый маркер (1)
    m_start = re.search(r'\(1\)', full_text)
    if not m_start:
        return None

    raw_text = full_text[m_start.start():]
    raw_text = re.sub(r'\s*\n\s*', ' ', raw_text)  # убираем переносы строк
    raw_text = re.sub(r'  +', ' ', raw_text).strip()

    # Очистка: удаляем всё после "(По ...)" или "Примерный круг проблем"
    raw_text = re.sub(r'\s*\([Пп]о\s+(?:по\s+)?[^()]*[А-ЯЁ]\.[^()]*\)\s*.*$', '', raw_text, flags=re.DOTALL)
    raw_text = re.sub(r'\s*\([Пп]о\s+[^()]+\*\s*\)\s*.*$', '', raw_text, flags=re.DOTALL)
    raw_text = re.sub(r'\s*Примерный круг проблем.*$', '', raw_text, flags=re.DOTALL)
    raw_text = re.sub(r'\s*\*[А-ЯЁ][а-яё]+\s[А-ЯЁ].*?\(\d{4}[−–\-]\d{4}\).*$', '', raw_text, flags=re.DOTALL)
    raw_text = re.sub(r'\s*Источник текста:.*$', '', raw_text)

    # Удаляем bio-сноски: "Имя Фамилия (ГГГГ-ГГГГ) — ..."
    raw_text = re.sub(
        r'\s*[А-ЯЁ][а-яё]+ [А-ЯЁ][а-яё]+ [А-ЯЁ][а-яё]+ \(\d{4}.*?\d{4}[^)]*\)[^(]*$',
        '', raw_text, flags=re.DOTALL
    )

    raw_text = raw_text.strip()
    if not raw_text or not re.search(r'\(\d+\)', raw_text):
        return None

    return raw_text


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


# ─── Дедупликация ────────────────────────────────────────────────────────────

def normalize_text(text: str) -> str:
    """Нормализация текста для сравнения: lower, strip пробелы/пунктуация."""
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)  # убираем пунктуацию
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def text_hash(text: str) -> str:
    """MD5 хеш нормализованного текста."""
    return hashlib.md5(normalize_text(text).encode("utf-8")).hexdigest()


def char_trigrams(text: str) -> set[str]:
    """Набор символьных триграмм для Jaccard similarity."""
    norm = normalize_text(text)
    if len(norm) < 3:
        return set()
    return {norm[i:i+3] for i in range(len(norm) - 2)}


def jaccard_similarity(set_a: set, set_b: set) -> float:
    """Jaccard similarity: |A ∩ B| / |A ∪ B|."""
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union if union > 0 else 0.0


def load_existing_dataset() -> list[dict]:
    """Загружаем существующий датасет."""
    path = DATA_PROCESSED / "sentences.jsonl"
    if not path.exists():
        print("Существующий датасет не найден")
        return []

    with open(path, "r", encoding="utf-8") as f:
        data = [json.loads(line) for line in f]
    print(f"Загружен существующий датасет: {len(data)} текстов")
    return data


def build_dedup_index(dataset: list[dict]) -> tuple[set[str], list[tuple[str, set[str]]]]:
    """Строим индексы для дедупликации: хеши + триграммы."""
    hashes = set()
    trigram_index = []

    for record in dataset:
        h = text_hash(record["clean_text"])
        hashes.add(h)
        tg = char_trigrams(record["clean_text"])
        trigram_index.append((record["id"], tg))

    return hashes, trigram_index


def is_duplicate(clean_text: str, hashes: set[str],
                 trigram_index: list[tuple[str, set[str]]],
                 threshold: float = 0.85) -> tuple[bool, str]:
    """Проверяем, является ли текст дубликатом.

    Returns:
        (is_dup, reason) — True если дубликат, с описанием причины.
    """
    # 1. Точное совпадение по хешу
    h = text_hash(clean_text)
    if h in hashes:
        return True, "exact_hash"

    # 2. Нечёткое совпадение по триграммам
    tg = char_trigrams(clean_text)
    for existing_id, existing_tg in trigram_index:
        sim = jaccard_similarity(tg, existing_tg)
        if sim >= threshold:
            return True, f"similar({sim:.3f})_to_{existing_id}"

    return False, ""


# ─── Сборка датасета ─────────────────────────────────────────────────────────

def process_rustutors(threshold: float) -> list[dict]:
    """Обрабатываем все HTML из rustutors → новые уникальные тексты."""
    # Загружаем существующий датасет
    existing = load_existing_dataset()
    hashes, trigram_index = build_dedup_index(existing)
    existing_count = len(existing)

    html_files = sorted(DATA_RAW_RUSTUTORS.glob("*.html"))
    print(f"\nHTML-файлов rustutors: {len(html_files)}")

    new_records = []
    stats = {"total": 0, "no_text": 0, "no_sentences": 0,
             "exact_dup": 0, "similar_dup": 0, "added": 0}

    for filepath in html_files:
        stats["total"] += 1
        filename = filepath.stem
        html_content = filepath.read_text(encoding="utf-8")

        raw_text = extract_raw_text_rustutors(html_content)
        if raw_text is None:
            stats["no_text"] += 1
            continue

        clean_text, sentences = parse_sentences(raw_text)
        if not sentences:
            stats["no_sentences"] += 1
            continue

        # Дедупликация
        is_dup, reason = is_duplicate(clean_text, hashes, trigram_index, threshold)
        if is_dup:
            if "exact" in reason:
                stats["exact_dup"] += 1
            else:
                stats["similar_dup"] += 1
            continue

        # Новый уникальный текст
        record_id = f"text_rustutors_{filename}"
        record = {
            "id": record_id,
            "source": f"rustutors_{filename}",
            "raw_text": raw_text,
            "clean_text": clean_text,
            "sentences": sentences,
            "num_sentences": len(sentences)
        }
        new_records.append(record)

        # Добавляем в индекс для дедупликации между собой
        h = text_hash(clean_text)
        hashes.add(h)
        tg = char_trigrams(clean_text)
        trigram_index.append((record_id, tg))
        stats["added"] += 1

    print(f"\n--- Статистика обработки ---")
    print(f"Всего файлов: {stats['total']}")
    print(f"Без текста: {stats['no_text']}")
    print(f"Без предложений: {stats['no_sentences']}")
    print(f"Точные дубли: {stats['exact_dup']}")
    print(f"Похожие дубли (>{threshold}): {stats['similar_dup']}")
    print(f"Новых уникальных: {stats['added']}")

    return existing + new_records


def validate_and_save(dataset: list[dict]) -> None:
    """Валидация и сохранение объединённого датасета."""
    if not dataset:
        print("Dataset пуст")
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
                    print(f"ОШИБКА в {record['id']}, предложение {s['idx']}")

    print(f"\nВалидация: {total_sentences} предложений, {total_errors} ошибок")

    num_sents = [r["num_sentences"] for r in dataset]
    text_lens = [len(r["clean_text"]) for r in dataset]
    sent_lens = [len(s["text"]) for r in dataset for s in r["sentences"]]

    print(f"\n--- Итоговая статистика ---")
    print(f"Текстов: {len(dataset)}")
    print(f"  из sdamgia: {sum(1 for r in dataset if r['source'].startswith('reshuege'))}")
    print(f"  из rustutors: {sum(1 for r in dataset if r['source'].startswith('rustutors'))}")
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


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Скачать тексты с rustutors.ru")
    parser.add_argument("--dry-run", action="store_true",
                        help="Только собрать URL, не скачивать")
    parser.add_argument("--skip-download", action="store_true",
                        help="Обработать имеющиеся HTML")
    parser.add_argument("--threshold", type=float, default=0.85,
                        help="Порог Jaccard similarity для дубликатов (по умолчанию 0.85)")
    args = parser.parse_args()

    DATA_RAW_RUSTUTORS.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    if not args.skip_download:
        # 1. Собираем URL
        print("=" * 60)
        print("ЭТАП 1: Сбор URL текстов с rustutors.ru")
        print("=" * 60)
        urls = collect_text_urls()

        if args.dry_run:
            print(f"\n[dry-run] Найдено {len(urls)} текстов. Выход.")
            return

        # 2. Скачиваем
        print("\n" + "=" * 60)
        print("ЭТАП 2: Скачивание HTML")
        print("=" * 60)
        download_all(urls)

    # 3. Обработка + дедупликация
    print("\n" + "=" * 60)
    print("ЭТАП 3: Обработка и дедупликация")
    print("=" * 60)
    dataset = process_rustutors(args.threshold)

    # 4. Валидация и сохранение
    print("\n" + "=" * 60)
    print("ЭТАП 4: Валидация и сохранение")
    print("=" * 60)
    validate_and_save(dataset)


if __name__ == "__main__":
    main()
