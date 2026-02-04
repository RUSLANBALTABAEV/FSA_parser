"""
ГЛАВНЫЙ ПАРСЕР ДАННЫХ FSA
Парсит ВСЕ данные со всех найденных ссылок
"""
import asyncio
import aiohttp
import time
import json
import sys
import os
from datetime import datetime
from openpyxl import Workbook
import csv

# Импортируем настройки
try:
    from config import TOKEN, API_BASE, BASE_URL
except ImportError:
    print("ОШИБКА: Создайте config.py с токеном!")
    sys.exit(1)

# Настройки
LINKS_FILE = "all_links.txt"
OUTPUT_EXCEL = f"FSA_all_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
OUTPUT_CSV = f"FSA_all_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
STATS_FILE = "parsing_stats.json"

# Параметры парсинга
CONCURRENT_REQUESTS = 20  # Одновременных запросов
TIMEOUT = 30  # Секунд на запрос
RETRY_COUNT = 3  # Попыток при ошибке

# Заголовки
HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9",
    "authorization": TOKEN,
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "referer": f"{BASE_URL}/ral",
}

def log(message):
    """Логирование с временем"""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {message}")

def extract_company_id(url):
    """Извлекает ID компании из URL"""
    try:
        # URL вида: https://pub.fsa.gov.ru/ral/view/38453/current-aa
        parts = url.rstrip('/').split('/')
        return parts[-2] if parts[-1] == 'current-aa' else parts[-1]
    except:
        return ""

def create_output_files():
    """Создает файлы для результатов"""
    # Excel файл
    wb = Workbook()
    ws = wb.active
    ws.title = "Компании FSA"
    
    # Заголовки столбцов
    headers = [
        "ID компании", "Ссылка", "Полное название", "Сокращенное название",
        "ИНН", "ОГРН", "Статус", "Дата регистрации", "Номер аккредитации",
        "Дата начала аккредитации", "Дата окончания аккредитации",
        "Телефон", "Email", "Веб-сайт", "Адрес", "Регион", "Город",
        "Руководитель", "Должность руководителя", "Время парсинга"
    ]
    
    ws.append(headers)
    wb.save(OUTPUT_EXCEL)
    log(f"Создан Excel файл: {OUTPUT_EXCEL}")
    
    # CSV файл
    with open(OUTPUT_CSV, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(headers)
    log(f"Создан CSV файл: {OUTPUT_CSV}")
    
    return headers

def append_to_excel(data_row):
    """Добавляет строку в Excel файл"""
    try:
        from openpyxl import load_workbook
        
        wb = load_workbook(OUTPUT_EXCEL)
        ws = wb.active
        ws.append(data_row)
        wb.save(OUTPUT_EXCEL)
        return True
    except Exception as e:
        log(f"Ошибка записи в Excel: {str(e)}")
        return False

def append_to_csv(data_row):
    """Добавляет строку в CSV файл"""
    try:
        with open(OUTPUT_CSV, 'a', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(data_row)
        return True
    except Exception as e:
        log(f"Ошибка записи в CSV: {str(e)}")
        return False

async def fetch_company_data(session, url, company_id, semaphore):
    """Получает данные одной компании"""
    async with semaphore:
        start_time = time.time()
        
        # Подготовка данных
        company_data = {
            "id": company_id,
            "url": url,
            "full_name": "",
            "short_name": "",
            "inn": "",
            "ogrn": "",
            "status": "",
            "reg_date": "",
            "accreditation_number": "",
            "accreditation_start": "",
            "accreditation_end": "",
            "phone": "",
            "email": "",
            "website": "",
            "address": "",
            "region": "",
            "city": "",
            "head_name": "",
            "head_position": "",
            "parsing_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            # Запрос к API
            api_url = f"{API_BASE}/ral/common/companies/{company_id}"
            
            for attempt in range(RETRY_COUNT):
                try:
                    async with session.get(api_url, timeout=TIMEOUT) as response:
                        if response.status == 200:
                            data = await response.json()
                            
                            # Основные данные
                            company_data["full_name"] = data.get("fullName", "")
                            company_data["short_name"] = data.get("shortName", "")
                            company_data["inn"] = data.get("inn", "")
                            company_data["ogrn"] = data.get("ogrn", "")
                            company_data["reg_date"] = data.get("regDate", "")
                            
                            # Статус
                            status_info = data.get("status", {})
                            if isinstance(status_info, dict):
                                company_data["status"] = status_info.get("name", "")
                            else:
                                company_data["status"] = str(status_info)
                            
                            # Номер аккредитации
                            reg_numbers = data.get("regNumbers", [])
                            if reg_numbers and isinstance(reg_numbers, list):
                                for reg in reg_numbers:
                                    if reg.get("active"):
                                        company_data["accreditation_number"] = reg.get("regNumber", "")
                                        company_data["accreditation_start"] = reg.get("beginDate", "")
                                        company_data["accreditation_end"] = reg.get("endDate", "")
                                        break
                            
                            # Контакты
                            contacts = data.get("contacts", [])
                            for contact in contacts:
                                contact_type = contact.get("idType")
                                value = contact.get("value", "")
                                
                                if contact_type == 1:  # Телефон
                                    company_data["phone"] = value
                                elif contact_type == 2:  # Факс
                                    pass  # Пропускаем
                                elif contact_type == 3:  # Сайт
                                    company_data["website"] = value
                                elif contact_type == 4:  # Email
                                    company_data["email"] = value
                            
                            # Адрес
                            addresses = data.get("addresses", [])
                            if addresses and isinstance(addresses, list):
                                # Берем первый адрес
                                address = addresses[0]
                                company_data["address"] = address.get("fullAddress", "")
                                
                                # Пытаемся извлечь регион и город
                                unique_addr = address.get("uniqueAddress", "")
                                if unique_addr:
                                    parts = unique_addr.split(',')
                                    if len(parts) > 0:
                                        company_data["region"] = parts[0].strip()
                                    if len(parts) > 1:
                                        company_data["city"] = parts[1].strip()
                            
                            # Руководитель
                            head_person = data.get("headPerson", {})
                            if head_person:
                                surname = head_person.get("surname", "")
                                name = head_person.get("name", "")
                                patronymic = head_person.get("patronymic", "")
                                company_data["head_name"] = f"{surname} {name} {patronymic}".strip()
                            
                            company_data["head_position"] = data.get("headPost", "")
                            
                            break  # Успешно получили данные
                            
                        elif response.status == 404:
                            log(f"Компания {company_id} не найдена (404)")
                            company_data["status"] = "НЕ НАЙДЕНА"
                            break
                        elif response.status == 401:
                            log("ОШИБКА 401: Токен недействителен!")
                            company_data["status"] = "ОШИБКА АВТОРИЗАЦИИ"
                            break
                        else:
                            log(f"Ошибка {response.status} для компании {company_id}")
                            if attempt < RETRY_COUNT - 1:
                                await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка
                            else:
                                company_data["status"] = f"ОШИБКА {response.status}"
                except asyncio.TimeoutError:
                    log(f"Таймаут для компании {company_id}, попытка {attempt + 1}")
                    if attempt < RETRY_COUNT - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        company_data["status"] = "ТАЙМАУТ"
                except Exception as e:
                    log(f"Исключение для компании {company_id}: {str(e)}")
                    if attempt < RETRY_COUNT - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        company_data["status"] = f"ОШИБКА: {str(e)}"
        
        except Exception as e:
            log(f"Критическая ошибка для {company_id}: {str(e)}")
            company_data["status"] = f"КРИТИЧЕСКАЯ ОШИБКА"
        
        # Время обработки
        processing_time = time.time() - start_time
        if processing_time > 5:
            log(f"Долгая обработка компании {company_id}: {processing_time:.1f} сек")
        
        return company_data

async def parse_all_companies():
    """Парсит ВСЕ компании"""
    log("=" * 70)
    log("ЗАПУСК ПАРСЕРА ВСЕХ КОМПАНИЙ FSA")
    log("=" * 70)
    
    # Проверяем файл со ссылками
    if not os.path.exists(LINKS_FILE):
        log(f"✗ Файл {LINKS_FILE} не найден!")
        log("Сначала запустите collect_links.py для сбора ссылок")
        return None
    
    # Читаем ссылки
    with open(LINKS_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]
    
    total = len(urls)
    log(f"Найдено ссылок для обработки: {total:,}")
    
    if total == 0:
        log("Нет ссылок для обработки")
        return None
    
    # Создаем файлы для результатов
    headers = create_output_files()
    
    # Статистика
    stats = {
        "start_time": datetime.now().isoformat(),
        "total_companies": total,
        "processed": 0,
        "successful": 0,
        "failed": 0,
        "total_time": 0
    }
    
    # Создаем сессию
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async with aiohttp.ClientSession(headers=HEADERS, timeout=timeout) as session:
        # Создаем задачи для всех компаний
        tasks = []
        company_map = {}  # Для связи задачи и компании
        
        for i, url in enumerate(urls, 1):
            company_id = extract_company_id(url)
            if company_id:
                task = asyncio.create_task(
                    fetch_company_data(session, url, company_id, semaphore)
                )
                tasks.append(task)
                company_map[task] = (i, company_id, url)
        
        # Обрабатываем результаты по мере готовности
        completed = 0
        batch_size = 50
        batch_data = []
        
        for task in asyncio.as_completed(tasks):
            try:
                data = await task
                i, company_id, url = company_map[task]
                
                completed += 1
                stats["processed"] = completed
                
                # Формируем строку для записи
                row = [
                    data["id"],
                    data["url"],
                    data["full_name"],
                    data["short_name"],
                    data["inn"],
                    data["ogrn"],
                    data["status"],
                    data["reg_date"],
                    data["accreditation_number"],
                    data["accreditation_start"],
                    data["accreditation_end"],
                    data["phone"],
                    data["email"],
                    data["website"],
                    data["address"],
                    data["region"],
                    data["city"],
                    data["head_name"],
                    data["head_position"],
                    data["parsing_time"]
                ]
                
                batch_data.append(row)
                
                # Подсчет статистики
                if data["status"] not in ["ОШИБКА", "ТАЙМАУТ", "НЕ НАЙДЕНА"]:
                    stats["successful"] += 1
                else:
                    stats["failed"] += 1
                
                # Логируем прогресс
                if completed % 100 == 0:
                    progress = (completed / total) * 100
                    log(f"Обработано: {completed:,}/{total:,} ({progress:.1f}%)")
                
                # Периодическое сохранение
                if completed % batch_size == 0:
                    for row_data in batch_data:
                        append_to_excel(row_data)
                        append_to_csv(row_data)
                    batch_data = []
                    
                    # Сохраняем промежуточную статистику
                    stats["current_progress"] = f"{completed}/{total}"
                    with open("temp_stats.json", "w", encoding="utf-8") as f:
                        json.dump(stats, f, ensure_ascii=False, indent=2)
            
            except Exception as e:
                log(f"Ошибка при обработке задачи: {str(e)}")
                stats["failed"] += 1
        
        # Сохраняем оставшиеся данные
        if batch_data:
            for row_data in batch_data:
                append_to_excel(row_data)
                append_to_csv(row_data)
    
    # Финальная статистика
    stats["end_time"] = datetime.now().isoformat()
    stats["total_time"] = time.time() - time.mktime(
        datetime.fromisoformat(stats["start_time"]).timetuple()
    )
    
    if total > 0:
        stats["success_rate"] = (stats["successful"] / total) * 100
    
    # Сохраняем статистику
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2, default=str)
    
    return stats

def print_summary(stats):
    """Выводит итоговую статистику"""
    if not stats:
        return
    
    log("\n" + "=" * 70)
    log("ИТОГИ ПАРСИНГА:")
    log("=" * 70)
    log(f"   Всего компаний: {stats['total_companies']:,}")
    log(f"   Успешно обработано: {stats['successful']:,}")
    log(f"   С ошибками: {stats['failed']:,}")
    
    if 'success_rate' in stats:
        log(f"   Успешность: {stats['success_rate']:.1f}%")
    
    log(f"   Общее время: {stats['total_time']:.0f} сек")
    
    if stats['processed'] > 0:
        avg_time = stats['total_time'] / stats['processed']
        log(f"   Среднее время на компанию: {avg_time:.2f} сек")
    
    log(f"\nФайлы с данными:")
    log(f"   Excel: {OUTPUT_EXCEL}")
    log(f"   CSV: {OUTPUT_CSV}")
    log(f"   Статистика: {STATS_FILE}")
    log("=" * 70)

async def main():
    """Основная функция"""
    try:
        # Проверяем токен
        if not TOKEN or TOKEN == "Bearer ВСТАВЬТЕ_ВАШ_ТОКЕН_ЗДЕСЬ":
            log("✗ ОШИБКА: Токен не установлен!")
            log("Запустите get_token.py для получения токена")
            return
        
        log(f"Используется токен (первые 30 символов): {TOKEN[:30]}...")
        
        # Запускаем парсинг
        stats = await parse_all_companies()
        
        # Выводим результаты
        if stats:
            print_summary(stats)
            log("\n✓ Парсинг завершен успешно!")
        else:
            log("\n✗ Парсинг не удался")
            
    except KeyboardInterrupt:
        log("\n✗ Парсинг прерван пользователем")
        log("Промежуточные результаты сохранены")
    except Exception as e:
        log(f"\n✗ Критическая ошибка: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Исправляем кодировку для Windows
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    
    asyncio.run(main())
