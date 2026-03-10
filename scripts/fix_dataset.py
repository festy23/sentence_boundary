#!/usr/bin/env python3
"""
Скрипт для автоматического исправления проблем в sentences.jsonl.

Найденные проблемы:
1. OCR: цифра "3" вместо "З", "0" вместо "О" в начале слов
2. Утечки нумерации: (число), {число), (З), (ЗО), (1б), (4!) и т.д.
3. Пропущенные пробелы между предложениями: "вещи.Анне" -> "вещи. Анне"
4. Мягкие переносы: "¬", soft hyphen U+00AD
5. Разорванные слова: "спускать ся" -> "спускаться"
6. Пересчёт idx (0-based) и num_sentences
"""

import json
import re
import sys
from pathlib import Path

DATA_PATH = Path(__file__).parent.parent / "data" / "processed" / "sentences.jsonl"
EOS_CHARS = set(".!?…")


def fix_ocr_digits(text: str) -> str:
    """Заменяет цифру '3' на 'З' и '0' на 'О' в начале кириллических слов."""
    # 3 + строчная кириллическая буква -> З + буква
    text = re.sub(r'(?<![0-9])3(?=[а-яё])', 'З', text)
    # 0 + строчная кириллическая буква -> О + буква
    text = re.sub(r'(?<![0-9])0(?=[а-яё])', 'О', text)
    # 3 + заглавная кириллическая буква (если не часть числа) -> З
    text = re.sub(r'(?<![0-9])3(?=[А-ЯЁ][а-яё])', 'З', text)
    # 0 + заглавная кириллическая -> О
    text = re.sub(r'(?<![0-9])0(?=[А-ЯЁ][а-яё])', 'О', text)
    return text


def fix_numbering_artifacts(text: str) -> str:
    """Удаляет утечки нумерации вида (число), {число), (З), (ЗО), (1б), (4!) и т.д."""
    # Паттерны нумерации: (N), {N), (N ), где N — цифры, кириллические цифро-подобные
    # Включает: (1), (23), (З), (ЗО), (ЗЗ), (З0), (З1), (З6), (Зб),
    #           (И), (Ю), (б), (1б), (4!), (7.), {6), {9), {20), (23 ), (41 ), (34 )
    patterns = [
        r'\([0-9ЗзИЮб]{1,3}[\s!.)]*\)\s*',  # (число) с возможными пробелами/! после
        r'\{[0-9ЗзИЮб]{1,3}[\s!.)]*\)\s*',   # {число)
        r'\([0-9]{1,3}\s+\)',                    # (34 ) — число с пробелом перед )
        r'\([0-9]{1,3}\s+',                      # (10 — незакрытая скобка с числом и пробелом
    ]
    for pat in patterns:
        text = re.sub(pat, '', text)
    return text


def fix_soft_hyphens(text: str) -> str:
    """Удаляет мягкие переносы ¬ и U+00AD."""
    text = text.replace('¬', '')
    text = text.replace('\u00ad', '')
    return text


def fix_broken_words(text: str) -> str:
    """Склеивает разорванные слова: 'спускать ся' -> 'спускаться'."""
    # Паттерн: кириллическая буква + пробел + 'ся'/'сь' на границе слова
    text = re.sub(r'([а-яё])\s+(ся|сь)(?=\s|[.,!?;:»\)]|$)', r'\1\2', text)
    # "проши пел" — общий паттерн сложнее, пока не трогаем (нужен словарь)
    return text


def fix_missing_spaces(text: str) -> str:
    """Добавляет пробел между предложениями, где он пропущен.

    Паттерн: точка/!/? + заглавная буква без пробела -> добавить пробел.
    Но НЕ трогаем сокращения и инициалы (одна буква перед точкой).
    """
    # Точка/!/? + заглавная -> вставить пробел
    # Исключаем: инициалы (одна заглавная + точка), сокращения
    def add_space(m):
        before = m.group(1)
        punct = m.group(2)
        after = m.group(3)
        return before + punct + ' ' + after

    text = re.sub(r'([а-яё]{2,})([.!?…])([А-ЯЁ])', add_space, text)
    return text


def fix_author_attribution(text: str) -> str:
    """Удаляет атрибуции авторов в конце текста: (В. Солоухин*)*."""
    # Паттерн: (Автор*) или (Автор*)* в конце текста
    text = re.sub(r'\s*\([А-ЯЁ][^()]{3,40}\*\)\*?\s*$', '', text)
    # Паттерн: (По Автору) в конце
    text = re.sub(r'\s*\(По\s+[А-ЯЁ][^()]{3,40}\)\s*$', '', text)
    return text


def reparse_sentences(clean_text: str, old_sentences: list) -> list:
    """Перепарсивает предложения после изменения clean_text.

    Использует старые границы как подсказку, но пересчитывает start/end/text.
    """
    # Если текст не изменился, просто переиндексируем
    new_sentences = []
    for i, sent in enumerate(old_sentences):
        start = sent["start"]
        end = sent["end"]

        # Проверяем, что границы валидны
        if start >= len(clean_text):
            continue
        end = min(end, len(clean_text))
        if start >= end:
            continue

        text = clean_text[start:end]
        new_sentences.append({
            "idx": i,  # 0-based
            "start": start,
            "end": end,
            "text": text,
        })

    return new_sentences


def rebuild_sentences_from_text(clean_text: str, original_sentences: list) -> list:
    """Полностью перестраивает предложения из clean_text,
    используя оригинальные границы как ориентир.

    Нужно когда clean_text был модифицирован (удалены артефакты, добавлены пробелы).
    """
    if not original_sentences:
        return []

    # Собираем оригинальные тексты предложений (уже очищенные от артефактов)
    orig_texts = []
    for s in original_sentences:
        t = s["text"]
        # Чистим текст предложения теми же фильтрами
        t = fix_ocr_digits(t)
        t = fix_numbering_artifacts(t)
        t = fix_soft_hyphens(t)
        t = fix_broken_words(t)
        t = t.strip()
        if t:
            orig_texts.append(t)

    # Находим каждый текст предложения в clean_text последовательно
    new_sentences = []
    search_from = 0

    for i, sent_text in enumerate(orig_texts):
        idx = clean_text.find(sent_text, search_from)
        if idx == -1:
            # Попробуем найти начало (первые 20 символов)
            prefix = sent_text[:min(20, len(sent_text))]
            idx = clean_text.find(prefix, search_from)
            if idx == -1:
                # Не нашли — пропускаем
                continue
            # Находим конец предложения
            end_idx = idx + len(sent_text)
            if end_idx > len(clean_text):
                end_idx = len(clean_text)
        else:
            end_idx = idx + len(sent_text)

        new_sentences.append({
            "idx": i,
            "start": idx,
            "end": end_idx,
            "text": clean_text[idx:end_idx],
        })
        search_from = end_idx

    return new_sentences


def fix_record(rec: dict) -> dict:
    """Исправляет одну запись."""
    clean_text = rec["clean_text"]
    original_clean = clean_text

    # 1. Фиксим OCR
    clean_text = fix_ocr_digits(clean_text)

    # 2. Убираем утечки нумерации
    clean_text = fix_numbering_artifacts(clean_text)

    # 3. Убираем мягкие переносы
    clean_text = fix_soft_hyphens(clean_text)

    # 4. Склеиваем разорванные слова
    clean_text = fix_broken_words(clean_text)

    # 5. Добавляем пропущенные пробелы
    clean_text = fix_missing_spaces(clean_text)

    # 6. Убираем атрибуции авторов
    clean_text = fix_author_attribution(clean_text)

    # 7. Убираем лишние пробелы
    clean_text = re.sub(r'  +', ' ', clean_text)
    clean_text = clean_text.strip()

    rec["clean_text"] = clean_text

    # Если текст изменился, перестраиваем предложения
    if clean_text != original_clean:
        sentences = rebuild_sentences_from_text(clean_text, rec["sentences"])
    else:
        # Просто переиндексируем (0-based)
        sentences = []
        for i, s in enumerate(rec["sentences"]):
            sentences.append({
                "idx": i,
                "start": s["start"],
                "end": s["end"],
                "text": s["text"],
            })

    rec["sentences"] = sentences
    rec["num_sentences"] = len(sentences)

    return rec


def validate_record(rec: dict) -> list[str]:
    """Проверяет запись после фикса, возвращает список проблем."""
    issues = []
    ct = rec["clean_text"]
    sents = rec["sentences"]

    # num_sentences
    if rec["num_sentences"] != len(sents):
        issues.append(f"num_sentences={rec['num_sentences']} != len(sentences)={len(sents)}")

    # idx sequential from 0
    for i, s in enumerate(sents):
        if s["idx"] != i:
            issues.append(f"idx[{i}]={s['idx']} (expected {i})")
            break

    # text == clean_text[start:end]
    for s in sents:
        actual = ct[s["start"]:s["end"]]
        if actual != s["text"]:
            issues.append(f"idx={s['idx']}: text mismatch at [{s['start']}:{s['end']}]")
            break

    # No overlaps
    for i in range(1, len(sents)):
        if sents[i]["start"] < sents[i-1]["end"]:
            issues.append(f"overlap: sent {i-1} end={sents[i-1]['end']} > sent {i} start={sents[i]['start']}")
            break

    # OCR check
    if re.search(r'(?<![0-9])3(?=[а-яё])', ct):
        issues.append("still has OCR digit '3' for 'З'")
    if re.search(r'(?<![0-9])0(?=[а-яё])', ct):
        issues.append("still has OCR digit '0' for 'О'")

    # Numbering artifacts
    if re.search(r'\([0-9ЗзИЮб]{1,3}[)!.]\)', ct):
        issues.append("still has numbering artifacts")

    return issues


def main():
    print(f"Читаем {DATA_PATH}...")
    with open(DATA_PATH, encoding="utf-8") as f:
        records = [json.loads(line) for line in f]

    print(f"Загружено {len(records)} записей")

    fixed_count = 0
    issue_count = 0
    post_issues = []

    for rec in records:
        old_text = rec["clean_text"]
        old_sents_count = len(rec["sentences"])

        rec = fix_record(rec)

        if rec["clean_text"] != old_text or len(rec["sentences"]) != old_sents_count:
            fixed_count += 1

        # Валидация после фикса
        issues = validate_record(rec)
        if issues:
            issue_count += 1
            post_issues.append((rec["id"], issues))

    print(f"\nИсправлено записей: {fixed_count}")
    print(f"Записей с проблемами после фикса: {issue_count}")

    if post_issues:
        print("\nОставшиеся проблемы:")
        for rec_id, issues in post_issues[:20]:
            for issue in issues:
                print(f"  {rec_id}: {issue}")
        if len(post_issues) > 20:
            print(f"  ... и ещё {len(post_issues) - 20}")

    # Сохраняем
    backup_path = DATA_PATH.with_suffix(".jsonl.bak")
    print(f"\nБэкап: {backup_path}")
    DATA_PATH.rename(backup_path)

    with open(DATA_PATH, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"Сохранено {len(records)} записей в {DATA_PATH}")

    # Статистика
    total_sents = sum(r["num_sentences"] for r in records)
    total_chars = sum(len(r["clean_text"]) for r in records)
    print(f"Предложений: {total_sents}, символов: {total_chars}")


if __name__ == "__main__":
    main()
