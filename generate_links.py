"""
ГЕНЕРАЦИЯ ССЫЛОК НА КОМПАНИИ FSA
Создает файл all_links.txt со всеми ссылками
"""
import sys
import os
import time
from datetime import datetime
from config import BASE_URL, ID_RANGE, LINKS_FILE

def log(message):
    """Логирование с временем"""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {message}")

def generate_links():
    """Генерирует все ссылки по диапазону ID"""
    log("=" * 70)
    log("ГЕНЕРАЦИЯ ССЫЛОК НА КОМПАНИИ FSA")
    log("=" * 70)
    
    start_id = ID_RANGE["start"]
    end_id = ID_RANGE["end"]
    step = ID_RANGE["step"]
    
    total_ids = (end_id - start_id) // step + 1
    log(f"Диапазон ID: {start_id} - {end_id} (шаг: {step})")
    log(f"Всего ID для обработки: {total_ids:,}")
    
    # Проверяем существующие ссылки
    existing_links = set()
    if os.path.exists(LINKS_FILE):
        try:
            with open(LINKS_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    link = line.strip()
                    if link:
                        existing_links.add(link)
            log(f"Загружено существующих ссылок: {len(existing_links):,}")
        except Exception as e:
            log(f"Ошибка загрузки файла: {str(e)}")
    
    # Генерируем новые ссылки
    new_links = []
    generated_count = 0
    
    for company_id in range(start_id, end_id + 1, step):
        link = f"{BASE_URL}/ral/view/{company_id}/current-aa"
        
        # Проверяем, есть ли уже такая ссылка
        if link not in existing_links:
            new_links.append(link)
            existing_links.add(link)
        
        generated_count += 1
        
        # Прогресс каждые 1000 ссылок
        if generated_count % 1000 == 0:
            log(f"Сгенерировано: {generated_count:,}/{total_ids:,} ({generated_count/total_ids*100:.1f}%)")
    
    # Сохраняем все ссылки
    if new_links:
        log(f"Найдено новых ссылок: {len(new_links):,}")
        
        try:
            # Открываем файл в режиме добавления
            with open(LINKS_FILE, 'a', encoding='utf-8') as f:
                for link in new_links:
                    f.write(f"{link}\n")
            
            log(f"Новые ссылки сохранены в файл: {LINKS_FILE}")
        except Exception as e:
            log(f"Ошибка сохранения ссылок: {str(e)}")
    else:
        log("Новых ссылок не найдено")
    
    # Создаем отдельный файл со всеми ссылками (перезаписываем)
    all_links = list(existing_links)
    try:
        with open(LINKS_FILE, 'w', encoding='utf-8') as f:
            for link in all_links:
                f.write(f"{link}\n")
        
        log(f"Всего сохранено ссылок: {len(all_links):,}")
        
        # Создаем тестовый файл с первыми 100 ссылками
        if len(all_links) > 100:
            with open("test_100_links.txt", 'w', encoding='utf-8') as f:
                for link in all_links[:100]:
                    f.write(f"{link}\n")
            log("Создан тестовый файл: test_100_links.txt")
    
    except Exception as e:
        log(f"Ошибка записи файла: {str(e)}")
    
    # Статистика
    stats = {
        "generation_date": datetime.now().isoformat(),
        "id_range": ID_RANGE,
        "total_links": len(all_links),
        "new_links_added": len(new_links),
        "links_file": LINKS_FILE,
        "first_5_links": all_links[:5] if all_links else []
    }
    
    # Сохраняем статистику
    import json
    with open("links_generation_stats.json", 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    log(f"Статистика сохранена: links_generation_stats.json")
    
    # Итоги
    log("\n" + "=" * 70)
    log("ГЕНЕРАЦИЯ ССЫЛОК ЗАВЕРШЕНА!")
    log("=" * 70)
    log(f"Всего ссылок: {len(all_links):,}")
    log(f"Файл со ссылками: {LINKS_FILE}")
    
    if all_links:
        log("\nПримеры ссылок:")
        for i, link in enumerate(all_links[:5], 1):
            log(f"  {i}. {link}")
    
    return all_links

def main():
    """Основная функция"""
    try:
        links = generate_links()
        
        # Предложение запустить парсинг
        if links:
            print("\n" + "=" * 70)
            choice = input("Запустить парсинг сгенерированных ссылок? (y/n): ").strip().lower()
            if choice == 'y':
                log("Запуск основного парсера...")
                import parser
                import asyncio
                asyncio.run(parser.main())
    
    except KeyboardInterrupt:
        log("\nГенерация прервана пользователем")
    except Exception as e:
        log(f"Ошибка: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Исправление кодировки для Windows
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    main()
