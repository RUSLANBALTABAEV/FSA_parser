"""
УЛУЧШЕННЫЙ СКРИПТ ДЛЯ СБОРА ССЫЛОК НА КОМПАНИИ С FSA
С ФИЛЬТРОМ ПО СТАТУСУ "ДЕЙСТВУЕТ"
"""
import asyncio
import aiohttp
import json
import time
import sys
import os
from pathlib import Path

# Импортируем настройки из config.py
try:
    from config import TOKEN, BASE_URL, API_BASE
except ImportError:
    print("ОШИБКА: Создайте файл config.py с токеном!")
    sys.exit(1)

# Настройки
COMPANIES_API = f"{API_BASE}/ral/common/companies"
OUTPUT_FILE = "all_links.txt"
BATCH_SIZE = 1000  # Размер пачки
TEMP_FILE = "links_temp.txt"

# Заголовки
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9",
    "authorization": TOKEN,
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "referer": f"{BASE_URL}/ral",
    "origin": BASE_URL
}

def log(msg):
    """Простой лог"""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

def save_links_to_file(links, filename):
    """Сохраняет ссылки в файл"""
    try:
        with open(filename, "a", encoding="utf-8") as f:
            for link in links:
                f.write(f"{link}\n")
        log(f"Сохранено {len(links)} ссылок в {filename}")
    except Exception as e:
        log(f"Ошибка при сохранении в {filename}: {str(e)}")

def load_existing_links(filename):
    """Загружает уже существующие ссылки из файла"""
    existing_links = set()
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                for line in f:
                    link = line.strip()
                    if link:
                        existing_links.add(link)
            log(f"Загружено {len(existing_links)} существующих ссылок из {filename}")
        except Exception as e:
            log(f"Ошибка при загрузке {filename}: {str(e)}")
    return existing_links

async def get_total_companies(session):
    """Получить общее количество компаний со статусом 'Действует'"""
    params = {
        "page": 0,
        "size": 1,
        "sort": "id,asc",
        "idStatus": 6  # ВАЖНО: 6 = "Действует"
    }
    
    try:
        log(f"Запрос к API: {COMPANIES_API}")
        log(f"Параметры: {params}")
        
        async with session.get(COMPANIES_API, params=params, headers=HEADERS) as response:
            log(f"Статус запроса: {response.status}")
            
            if response.status == 200:
                data = await response.json()
                total_elements = data.get("totalElements", 0)
                log(f"Всего компаний со статусом 'Действует': {total_elements:,}")
                log(f"Количество страниц: {data.get('totalPages', 0)}")
                
                return total_elements
            elif response.status == 401:
                log("ОШИБКА 401: Неверный или устаревший токен!")
                log("Получите новый токен:")
                log("1. Откройте https://pub.fsa.gov.ru/ral")
                log("2. Нажмите F12 -> Network")
                log("3. Обновите страницу")
                log("4. Найдите запрос к API")
                log("5. Скопируйте заголовок Authorization")
                log("6. Вставьте в файл config.py")
                return 0
            else:
                try:
                    text = await response.text()
                    log(f"Ошибка при получении данных: {response.status}")
                    log(f"Текст ответа: {text[:500]}")
                except:
                    log(f"Ошибка {response.status} без текста ответа")
                return 0
    except Exception as e:
        log(f"Исключение при получении общего количества: {str(e)}")
        return 0

async def fetch_companies_page(session, page, size=BATCH_SIZE, retry=5):
    """Получить одну страницу компаний"""
    params = {
        "page": page,
        "size": size,
        "sort": "id,asc",
        "idStatus": 6  # ВАЖНО: 6 = "Действует"
    }
    
    for attempt in range(retry):
        try:
            log(f"Запрос страницы {page + 1} (пакет {size}, попытка {attempt + 1}/{retry})")
            
            async with session.get(COMPANIES_API, params=params, headers=HEADERS, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if "content" not in data:
                        log(f"Странная структура ответа на странице {page + 1}")
                        log(f"Ключи в ответе: {list(data.keys())}")
                        return None
                    
                    return data
                elif response.status == 429:  # Too Many Requests
                    wait = (attempt + 1) * 10
                    log(f"Слишком много запросов. Жду {wait} секунд...")
                    await asyncio.sleep(wait)
                    continue
                elif response.status == 401:
                    log("ОШИБКА 401: Токен недействителен!")
                    return None
                elif response.status == 403:
                    log("ОШИБКА 403: Доступ запрещен!")
                    return None
                elif response.status == 500:
                    log(f"Ошибка 500 на сервере. Жду {attempt + 1}0 секунд...")
                    await asyncio.sleep((attempt + 1) * 10)
                    continue
                else:
                    try:
                        text = await response.text()
                        log(f"Ошибка {response.status} на странице {page + 1}: {text[:200]}")
                    except:
                        log(f"Ошибка {response.status} на странице {page + 1}")
                    return None
        except asyncio.TimeoutError:
            log(f"Таймаут на странице {page + 1}")
            if attempt < retry - 1:
                await asyncio.sleep(5)
            continue
        except Exception as e:
            log(f"Исключение на странице {page + 1}: {str(e)}")
            if attempt < retry - 1:
                await asyncio.sleep(2)
    
    log(f"Не удалось получить страницу {page + 1} после {retry} попыток")
    return None

async def collect_all_links():
    """Собрать все ссылки на компании со статусом 'Действует'"""
    log("=" * 70)
    log("НАЧИНАЕМ СБОР ССЫЛОК НА КОМПАНИИ СО СТАТУСОМ 'ДЕЙСТВУЕТ'")
    log("=" * 70)
    log(f"Используем API: {COMPANIES_API}")
    log(f"BASE_URL: {BASE_URL}")
    log(f"API_BASE: {API_BASE}")
    log(f"Размер пачки: {BATCH_SIZE}")
    log(f"Фильтр: idStatus=6 (Действует)")
    
    # Загружаем уже существующие ссылки
    existing_links = load_existing_links(OUTPUT_FILE)
    all_links = list(existing_links)
    all_links_set = set(all_links)
    
    log(f"Уже есть {len(existing_links)} ссылок, собираем новые...")
    
    timeout = aiohttp.ClientTimeout(total=600)
    
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        # Получаем общее количество компаний
        log("Получаю общее количество компаний со статусом 'Действует'...")
        total = await get_total_companies(session)
        
        if total == 0:
            log("Не удалось получить данные о компаниях")
            return all_links
        
        # Рассчитываем количество страниц
        total_pages = (total + BATCH_SIZE - 1) // BATCH_SIZE
        log(f"Всего компаний со статусом 'Действует': {total:,}")
        log(f"Всего страниц для обработки: {total_pages:,}")
        
        if total_pages == 0:
            log("Нет страниц для обработки")
            return all_links
        
        # Собираем ссылки со всех страниц
        start_time = time.time()
        
        for page in range(total_pages):
            current_time = time.time()
            elapsed_time = current_time - start_time
            
            # Оценка оставшегося времени
            if page > 0:
                avg_time_per_page = elapsed_time / page
                remaining_pages = total_pages - page
                estimated_time_remaining = avg_time_per_page * remaining_pages
                
                hours = int(estimated_time_remaining // 3600)
                minutes = int((estimated_time_remaining % 3600) // 60)
                seconds = int(estimated_time_remaining % 60)
                
                log(f"Обрабатываю страницу {page + 1}/{total_pages:,} | "
                    f"Прогресс: {(page + 1) / total_pages * 100:.1f}% | "
                    f"Осталось: {hours:02d}:{minutes:02d}:{seconds:02d}")
            else:
                log(f"Обрабатываю страницу {page + 1}/{total_pages:,}")
            
            data = await fetch_companies_page(session, page)
            
            if data is None:
                log(f"Пропущена страница {page + 1} из-за ошибки")
                continue
            
            # Проверяем структуру ответа
            if "content" not in data:
                log(f"На странице {page + 1} нет поля 'content'")
                continue
            
            companies = data["content"]
            
            if not companies:
                log(f"Страница {page + 1} пуста")
                continue
            
            # Обрабатываем компании с этой страницы
            new_links = []
            for idx, company in enumerate(companies):
                company_id = company.get("id")
                if company_id:
                    link = f"{BASE_URL}/ral/view/{company_id}/current-aa"
                    
                    # Проверяем, есть ли уже эта ссылка
                    if link not in all_links_set:
                        new_links.append(link)
                        all_links_set.add(link)
            
            # Добавляем новые ссылки к общему списку
            if new_links:
                all_links.extend(new_links)
                
                # Сохраняем новые ссылки в файл
                save_links_to_file(new_links, OUTPUT_FILE)
                
                # Также сохраняем во временный файл для надежности
                save_links_to_file(new_links, TEMP_FILE)
                
                log(f"Добавлено {len(new_links)} новых ссылок с страницы {page + 1}")
            
            log(f"Всего собрано ссылок: {len(all_links):,}")
            
            # Пауза между запросами для избежания блокировки
            if (page + 1) % 10 == 0:
                wait_time = 2
                log(f"Делаю паузу {wait_time} секунды...")
                await asyncio.sleep(wait_time)
            
            # Сохраняем статистику каждые 50 страниц
            if (page + 1) % 50 == 0 or page == total_pages - 1:
                save_stats(all_links, page + 1, total_pages, total)
    
    # Удаляем временный файл после успешного завершения
    if os.path.exists(TEMP_FILE):
        try:
            os.remove(TEMP_FILE)
            log(f"Временный файл {TEMP_FILE} удален")
        except:
            pass
    
    return all_links

def save_stats(links, current_page, total_pages, total_companies):
    """Сохраняет статистику сбора"""
    stats = {
        "total_links": len(links),
        "total_companies_with_status_active": total_companies,
        "current_page": current_page,
        "total_pages": total_pages,
        "collection_date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "https://pub.fsa.gov.ru/ral",
        "filter": "idStatus=6 (Действует)",
        "batch_size": BATCH_SIZE,
        "links_sample": links[:5] if links else []
    }
    
    with open("links_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    log(f"Статистика сохранена. Всего ссылок: {len(links):,}")

def check_and_update_config():
    """Проверяет и обновляет конфигурацию"""
    # Проверяем токен
    if not TOKEN or TOKEN == "Bearer ВАШ_ТОКЕН" or "ВСТАВЬТЕ_СЮДА_ВАШ_ТОКЕН" in TOKEN:
        log("ОШИБКА: Токен не установлен или установлен неправильно!")
        log("Откройте файл config.py и вставьте правильный токен.")
        log("Формат: TOKEN = 'Bearer eyJhbGciOiJFZERTQSJ9...'")
        return False
    
    # Проверяем формат токена
    if not TOKEN.startswith("Bearer "):
        log("ОШИБКА: Токен должен начинаться с 'Bearer '!")
        return False
    
    log(f"Токен проверен (первые 50 символов): {TOKEN[:50]}...")
    return True

async def main():
    """Основная функция"""
    try:
        log("=" * 70)
        log("ЗАПУСК СКРИПТА СБОРА ССЫЛОК (ТОЛЬКО 'ДЕЙСТВУЕТ')")
        log("=" * 70)
        
        # Проверяем конфигурацию
        if not check_and_update_config():
            return
        
        # Проверяем, нужно ли продолжить с предыдущего места
        if os.path.exists(TEMP_FILE):
            log(f"Найден временный файл {TEMP_FILE}. Продолжаем сбор...")
            temp_links = load_existing_links(TEMP_FILE)
            log(f"Из временного файла загружено {len(temp_links)} ссылок")
        
        # Собираем ссылки
        links = await collect_all_links()
        
        # Финальная статистика
        if links:
            log("\n" + "=" * 70)
            log("СБОР ССЫЛОК ЗАВЕРШЕН УСПЕШНО!")
            log("=" * 70)
            log(f"Всего собрано ссылок: {len(links):,}")
            log(f"Результат сохранен в: {OUTPUT_FILE}")
            log(f"Статистика сохранена в: links_stats.json")
            
            # Создаем тестовый файл с первыми 100 ссылок
            if len(links) > 100:
                test_file = "test_100_links.txt"
                with open(test_file, "w", encoding="utf-8") as f:
                    for link in links[:100]:
                        f.write(f"{link}\n")
                log(f"Тестовый файл создан: {test_file}")
            
            log(f"\nПримеры ссылок (первые 5):")
            for i, link in enumerate(links[:5], 1):
                log(f"  {i}. {link}")
            
        else:
            log("Не удалось собрать ссылки")
            
    except KeyboardInterrupt:
        log("\nСбор ссылок прерван пользователем")
        log(f"Текущий прогресс сохранен в {TEMP_FILE}")
    except Exception as e:
        log(f"Критическая ошибка: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Устанавливаем кодировку для Windows
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='ignore')
    
    asyncio.run(main())
