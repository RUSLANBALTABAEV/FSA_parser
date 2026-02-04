"""
УЛУЧШЕННЫЙ СКРИПТ ДЛЯ СБОРА ВСЕХ ССЫЛОК НА КОМПАНИИ
Использует официальное API FSA для получения полного списка
"""
import asyncio
import aiohttp
import json
import time
import sys
import os
from datetime import datetime

# Импортируем настройки
try:
    from config import TOKEN, BASE_URL, API_BASE
except ImportError:
    print("ОШИБКА: Создайте файл config.py с токеном!")
    print("Сначала запустите get_token.py")
    sys.exit(1)

# Настройки
COMPANIES_API = f"{API_BASE}/ral/common/companies"
OUTPUT_FILE = "all_links.txt"
BATCH_SIZE = 1000  # Компаний за один запрос
TEMP_FILE = "temp_links.txt"
STATS_FILE = "links_stats.json"

# Заголовки запросов
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
    """Вывод сообщений с временной меткой"""
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
        log(f"Ошибка при сохранении: {str(e)}")

def load_existing_links(filename):
    """Загружает уже собранные ссылки"""
    existing_links = set()
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                for line in f:
                    link = line.strip()
                    if link:
                        existing_links.add(link)
            log(f"Загружено {len(existing_links)} существующих ссылок")
        except Exception as e:
            log(f"Ошибка при загрузке: {str(e)}")
    return existing_links

async def get_total_companies(session):
    """Получает общее количество компаний"""
    params = {"page": 0, "size": 1, "sort": "id,asc"}
    
    try:
        log("Запрашиваю общее количество компаний...")
        async with session.get(COMPANIES_API, params=params, headers=HEADERS) as response:
            if response.status == 200:
                data = await response.json()
                total = data.get("totalElements", 0)
                pages = data.get("totalPages", 0)
                
                log(f"✓ Всего компаний в реестре: {total:,}")
                log(f"✓ Количество страниц: {pages:,}")
                return total
            elif response.status == 401:
                log("✗ ОШИБКА 401: Неверный или устаревший токен!")
                log("Запустите get_token.py для получения нового токена")
                return 0
            else:
                text = await response.text()
                log(f"✗ Ошибка {response.status}: {text[:200]}")
                return 0
    except Exception as e:
        log(f"✗ Исключение: {str(e)}")
        return 0

async def fetch_companies_page(session, page, size=BATCH_SIZE):
    """Получает одну страницу компаний"""
    params = {
        "page": page,
        "size": size,
        "sort": "id,asc"
    }
    
    for attempt in range(5):  # 5 попыток
        try:
            log(f"Страница {page + 1} (пакет {size}, попытка {attempt + 1}/5)")
            
            async with session.get(COMPANIES_API, params=params, headers=HEADERS, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:  # Слишком много запросов
                    wait = (attempt + 1) * 10
                    log(f"Слишком много запросов. Жду {wait} секунд...")
                    await asyncio.sleep(wait)
                else:
                    log(f"Ошибка {response.status} на странице {page + 1}")
                    return None
        except asyncio.TimeoutError:
            log(f"Таймаут на странице {page + 1}")
            await asyncio.sleep(5)
        except Exception as e:
            log(f"Исключение: {str(e)}")
            await asyncio.sleep(2)
    
    log(f"Не удалось получить страницу {page + 1}")
    return None

async def collect_all_links():
    """Собирает ВСЕ ссылки на компании"""
    log("=" * 70)
    log("НАЧИНАЮ СБОР ВСЕХ ССЫЛОК НА КОМПАНИИ")
    log("=" * 70)
    
    # Загружаем уже собранные ссылки
    existing_links = load_existing_links(OUTPUT_FILE)
    all_links = list(existing_links)
    all_links_set = set(all_links)
    
    log(f"Уже есть {len(existing_links)} ссылок, ищу новые...")
    
    timeout = aiohttp.ClientTimeout(total=300)  # 5 минут таймаут
    
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        # Получаем общее количество
        total = await get_total_companies(session)
        
        if total == 0:
            log("Не удалось получить данные. Проверьте токен.")
            return all_links
        
        # Рассчитываем количество страниц
        total_pages = (total + BATCH_SIZE - 1) // BATCH_SIZE
        log(f"Всего страниц для обработки: {total_pages:,}")
        
        start_time = time.time()
        
        # Обрабатываем все страницы
        for page in range(total_pages):
            # Прогресс и оценка времени
            elapsed = time.time() - start_time
            if page > 0:
                pages_left = total_pages - page
                time_per_page = elapsed / page
                eta_seconds = time_per_page * pages_left
                
                hours = int(eta_seconds // 3600)
                minutes = int((eta_seconds % 3600) // 60)
                seconds = int(eta_seconds % 60)
                
                progress = (page + 1) / total_pages * 100
                log(f"Страница {page + 1:,}/{total_pages:,} ({progress:.1f}%) | "
                    f"Осталось: {hours:02d}:{minutes:02d}:{seconds:02d}")
            
            # Получаем данные страницы
            data = await fetch_companies_page(session, page)
            
            if data is None:
                log(f"Пропущена страница {page + 1}")
                continue
            
            # Извлекаем ссылки
            companies = data.get("content", [])
            if not companies:
                log(f"Страница {page + 1} пуста")
                continue
            
            new_links = []
            for company in companies:
                company_id = company.get("id")
                if company_id:
                    # Формируем ссылку по шаблону
                    link = f"{BASE_URL}/ral/view/{company_id}/current-aa"
                    
                    if link not in all_links_set:
                        new_links.append(link)
                        all_links_set.add(link)
            
            # Сохраняем новые ссылки
            if new_links:
                all_links.extend(new_links)
                save_links_to_file(new_links, OUTPUT_FILE)
                save_links_to_file(new_links, TEMP_FILE)  # Резервная копия
                log(f"Добавлено {len(new_links)} новых ссылок")
            
            log(f"Всего собрано: {len(all_links):,} ссылок")
            
            # Пауза между запросами
            if (page + 1) % 20 == 0:
                await asyncio.sleep(1)
            
            # Сохраняем статистику каждые 50 страниц
            if (page + 1) % 50 == 0 or page == total_pages - 1:
                save_statistics(all_links, page + 1, total_pages, total)
    
    # Удаляем временный файл
    if os.path.exists(TEMP_FILE):
        os.remove(TEMP_FILE)
        log(f"Временный файл удален")
    
    return all_links

def save_statistics(links, current_page, total_pages, total_companies):
    """Сохраняет статистику сбора"""
    stats = {
        "total_links_collected": len(links),
        "total_companies_in_api": total_companies,
        "pages_processed": current_page,
        "total_pages": total_pages,
        "collection_date": datetime.now().isoformat(),
        "source": "https://pub.fsa.gov.ru/ral",
        "output_file": "all_links.txt",
        "links_sample": links[:10] if links else []
    }
    
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2, default=str)
    
    log(f"Статистика сохранена в {STATS_FILE}")

async def main():
    """Основная функция"""
    try:
        log("=" * 70)
        log("ЗАПУСК СБОРЩИКА ССЫЛОК")
        log("=" * 70)
        
        # Проверяем токен
        if not TOKEN or TOKEN == "Bearer ВСТАВЬТЕ_ВАШ_ТОКЕН_ЗДЕСЬ":
            log("✗ ОШИБКА: Токен не установлен!")
            log("Запустите get_token.py для получения токена")
            return
        
        log(f"Токен обнаружен (первые 50 символов): {TOKEN[:50]}...")
        
        # Проверяем временный файл для возобновления
        if os.path.exists(TEMP_FILE):
            log("Обнаружен временный файл. Продолжаю сбор...")
        
        # Собираем ссылки
        links = await collect_all_links()
        
        # Итоги
        if links:
            log("\n" + "=" * 70)
            log("СБОР ЗАВЕРШЕН УСПЕШНО!")
            log("=" * 70)
            log(f"Всего собрано ссылок: {len(links):,}")
            log(f"Результат сохранен в: {OUTPUT_FILE}")
            log(f"Статистика в: {STATS_FILE}")
            
            # Создаем тестовый файл
            if len(links) >= 100:
                with open("test_100_links.txt", "w", encoding="utf-8") as f:
                    for link in links[:100]:
                        f.write(f"{link}\n")
                log(f"Тестовый файл: test_100_links.txt")
            
            log("\nПримеры собранных ссылок:")
            for i, link in enumerate(links[:5], 1):
                log(f"  {i}. {link}")
        else:
            log("Не удалось собрать ссылки")
            
    except KeyboardInterrupt:
        log("\nСбор прерван пользователем")
        log(f"Прогресс сохранен в {TEMP_FILE}")
    except Exception as e:
        log(f"Критическая ошибка: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Исправляем кодировку для Windows
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    asyncio.run(main())
