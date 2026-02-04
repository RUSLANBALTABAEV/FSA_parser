"""
ОСНОВНОЙ СКРИПТ ПАРСИНГА ДАННЫХ FSA
"""
import asyncio
import aiohttp
import time
import json
import sys
import os
from pathlib import Path
from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter
from datetime import datetime
import csv

# Импортируем настройки
try:
    from config import TOKEN, API_BASE, BASE_URL
except ImportError:
    print("ОШИБКА: Создайте файл config.py с токеном!")
    sys.exit(1)

# Настройки
LINKS_FILE = "all_links.txt"
OUTPUT_FILE = f"FSA_реестры_все_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
OUTPUT_CSV = f"FSA_реестры_все_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
STATS_FILE = "parsing_stats.json"

CONCURRENCY = 25  # Увеличил для обработки 38000+ компаний
REQUEST_TIMEOUT = 60
RETRY_COUNT = 3
RETRY_DELAY = 3

# Заголовки
BASE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "ru-RU,ru;q=0.9",
    "authorization": TOKEN,
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "referer": "https://pub.fsa.gov.ru/ral",
    "origin": "https://pub.fsa.gov.ru"
}

# Словарь для преобразования английских названий столбцов в русские
COLUMN_TRANSLATIONS = {
    # Основные поля
    "source_url": "Ссылка на компанию",
    "company_id": "ID компании",
    "parsing_timestamp": "Время парсинга",
    "processing_time": "Время обработки (сек)",
    "error": "Ошибка",
    "company_error": "Ошибка компании",
    "declaration_error": "Ошибка декларации",
    "declaration_doc_id": "ID документа декларации",
    
    # Поля компании
    "company.id": "ID",
    "company.fullName": "Полное наименование",
    "company.shortName": "Сокращенное наименование",
    "company.inn": "ИНН",
    "company.kpp": "КПП",
    "company.ogrn": "ОГРН",
    "company.ogrnDate": "Дата ОГРН",
    "company.okpo": "ОКПО",
    "company.okato": "ОКАТО",
    "company.oktmo": "ОКТМО",
    "company.okfs": "ОКФС",
    "company.okopf": "ОКОПФ",
    "company.okogu": "ОКОГУ",
    "company.okved": "ОКВЭД",
    "company.phone": "Телефон",
    "company.fax": "Факс",
    "company.email": "Email",
    "company.website": "Веб-сайт",
    "company.address": "Адрес",
    "company.address.postalCode": "Индекс",
    "company.address.region": "Регион",
    "company.address.city": "Город",
    "company.address.street": "Улица",
    "company.address.house": "Дом",
    "company.address.building": "Корпус",
    "company.address.apartment": "Квартира",
    "company.head.lastName": "Фамилия руководителя",
    "company.head.firstName": "Имя руководителя",
    "company.head.middleName": "Отчество руководителя",
    "company.head.position": "Должность руководителя",
    "company.status": "Статус",
    "company.status.code": "Код статуса",
    "company.status.name": "Наименование статуса",
    "company.status.date": "Дата статуса",
    "company.accreditation.id": "ID аккредитации",
    "company.accreditation.number": "Номер аккредитации",
    "company.accreditation.dateFrom": "Дата начала аккредитации",
    "company.accreditation.dateTo": "Дата окончания аккредитации",
    "company.accreditation.status": "Статус аккредитации",
    "company.accreditation.status.code": "Код статуса аккредитации",
    "company.accreditation.status.name": "Наименование статуса аккредитации",
    "company.accreditation.idAccredScopeFile": "ID файла области аккредитации",
    
    # Поля декларации
    "declaration.id": "ID декларации",
    "declaration.number": "Номер декларации",
    "declaration.date": "Дата декларации",
    "declaration.status": "Статус декларации",
    "declaration.status.code": "Код статуса декларации",
    "declaration.status.name": "Наименование статуса декларации",
    "declaration.declarant.lastName": "Фамилия декларанта",
    "declaration.declarant.firstName": "Имя декларанта",
    "declaration.declarant.middleName": "Отчество декларанта",
    "declaration.declarant.position": "Должность декларанта",
    "declaration.accreditationScope": "Область аккредитации",
    "declaration.accreditationScope.code": "Код области аккредитации",
    "declaration.accreditationScope.name": "Наименование области аккредитации",
    "declaration.files": "Файлы декларации",
    "declaration.documents": "Документы декларации"
}

def log(msg):
    """Логирование"""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")

def translate_column_name(column_name):
    """Перевод названия столбца на русский"""
    return COLUMN_TRANSLATIONS.get(column_name, column_name)

def extract_company_id(url):
    """Извлечение ID компании из URL"""
    try:
        url = url.rstrip('/')
        if url.endswith('/current-aa'):
            url = url[:-len('/current-aa')]
        parts = url.split('/')
        return parts[-1] if parts else ""
    except:
        return ""

def flatten(obj, parent="", out=None):
    """Преобразование JSON в плоскую структуру"""
    if out is None:
        out = {}
    
    if obj is None:
        out[parent] = ""
    elif isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent}.{k}" if parent else k
            flatten(v, new_key, out)
    elif isinstance(obj, list):
        if not obj:
            out[parent] = ""
        else:
            # Для списков сохраняем как JSON строку
            try:
                out[parent] = json.dumps(obj, ensure_ascii=False)
            except:
                out[parent] = str(obj)
    else:
        if isinstance(obj, bool):
            out[parent] = "Да" if obj else "Нет"
        elif isinstance(obj, (int, float)):
            out[parent] = obj
        else:
            out[parent] = str(obj)
    
    return out

def extract_doc_id(company_json):
    """Извлечение ID документа аккредитации"""
    try:
        acc = company_json.get("accreditation")
        if isinstance(acc, dict):
            doc_id = acc.get("idAccredScopeFile")
            if doc_id:
                return str(doc_id)
        return None
    except:
        return None

async def fetch_with_retry(session, url, params=None):
    """Запрос с повторными попытками"""
    for attempt in range(RETRY_COUNT):
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 401:
                    return {"_error": "AUTH_ERROR", "_status": 401}
                elif resp.status == 404:
                    return {"_error": "NOT_FOUND", "_status": 404}
                elif resp.status == 429:
                    wait = (attempt + 1) * 5
                    log(f"Слишком много запросов. Жду {wait} сек...")
                    await asyncio.sleep(wait)
                    continue
                else:
                    log(f"Ошибка {resp.status} для {url}")
                    if attempt < RETRY_COUNT - 1:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    return {"_error": f"HTTP_{resp.status}", "_status": resp.status}
        except asyncio.TimeoutError:
            log(f"Таймаут на попытке {attempt + 1}")
            if attempt < RETRY_COUNT - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            return {"_error": "TIMEOUT"}
        except Exception as e:
            log(f"Ошибка на попытке {attempt + 1}: {str(e)}")
            if attempt < RETRY_COUNT - 1:
                await asyncio.sleep(RETRY_DELAY)
                continue
            return {"_error": str(e)}
    
    return {"_error": "MAX_RETRIES"}

async def process_company(session, url, idx, total, stats, semaphore):
    """Обработка одной компании"""
    async with semaphore:
        start_time = time.time()
        
        # Логируем прогресс
        if idx % 100 == 0:
            progress = (idx / total) * 100
            log(f"Прогресс: {idx}/{total:,} ({progress:.1f}%)")
        
        # Подготовка строки данных
        row = {
            "source_url": url,
            "parsing_timestamp": datetime.now().isoformat()
        }
        
        try:
            # Извлекаем ID компании
            company_id = extract_company_id(url)
            if not company_id:
                row["error"] = "Неверный URL"
                stats["errors"] += 1
                return row
            
            row["company_id"] = company_id
            
            # 1. Получаем данные компании
            company_url = f"{API_BASE}/ral/common/companies/{company_id}"
            company_data = await fetch_with_retry(session, company_url)
            
            if "_error" in company_data:
                if company_data.get("_status") == 401:
                    log("ОШИБКА: Токен недействителен!")
                    stats["auth_errors"] += 1
                row["company_error"] = f"API ошибка: {company_data['_error']}"
                stats["api_errors"] += 1
            else:
                # Добавляем данные компании
                row.update(flatten(company_data, "company"))
                stats["success_companies"] += 1
            
            # 2. Получаем декларацию (если есть)
            doc_id = extract_doc_id(company_data)
            if doc_id and "_error" not in company_data:
                decl_url = f"{API_BASE}/oa/accreditation/declaration/view/"
                params = {
                    "docId": doc_id,
                    "alType": 5,
                    "validate": "false"
                }
                
                decl_data = await fetch_with_retry(session, decl_url, params)
                
                if "_error" not in decl_data:
                    row.update(flatten(decl_data, "declaration"))
                    row["declaration_doc_id"] = doc_id
                    stats["success_declarations"] += 1
                else:
                    row["declaration_error"] = decl_data["_error"]
            
        except Exception as e:
            row["error"] = str(e)
            stats["exceptions"] += 1
        
        # Время обработки
        row["processing_time"] = round(time.time() - start_time, 2)
        stats["total_time"] += row["processing_time"]
        
        return row

def create_excel_with_headers(columns, filename):
    """Создает Excel файл с заголовками"""
    try:
        wb = Workbook()
        ws = wb.active
        ws.title = "Реестры FSA"
        
        # Сортируем колонки
        sorted_columns = sorted(columns)
        
        # Переводим заголовки на русский
        russian_headers = [translate_column_name(col) for col in sorted_columns]
        
        # Записываем заголовки
        ws.append(russian_headers)
        
        # Сохраняем файл только с заголовками
        wb.save(filename)
        
        log(f"Создан файл с заголовками: {filename}")
        return sorted_columns
        
    except Exception as e:
        log(f"Ошибка при создании Excel файла: {str(e)}")
        return None

def append_data_to_excel(rows, sorted_columns, filename):
    """Добавляет данные в существующий Excel файл"""
    try:
        # Загружаем существующий файл
        wb = load_workbook(filename)
        ws = wb.active
        
        # Добавляем данные
        for row in rows:
            data_row = []
            for col in sorted_columns:
                value = row.get(col, "")
                # Обработка специальных случаев
                if value is None:
                    value = ""
                elif isinstance(value, (dict, list)):
                    try:
                        value = json.dumps(value, ensure_ascii=False)
                    except:
                        value = str(value)
                elif isinstance(value, bool):
                    value = "Да" if value else "Нет"
                data_row.append(value)
            ws.append(data_row)
        
        # Сохраняем
        wb.save(filename)
        
    except Exception as e:
        log(f"Ошибка при добавлении данных в Excel: {str(e)}")
        raise

def save_to_csv(rows, columns, filename):
    """Сохраняет данные в CSV файл (альтернативный формат)"""
    try:
        sorted_columns = sorted(columns)
        russian_headers = [translate_column_name(col) for col in sorted_columns]
        
        with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            
            # Записываем заголовки
            writer.writerow(russian_headers)
            
            # Записываем данные
            for row in rows:
                data_row = []
                for col in sorted_columns:
                    value = row.get(col, "")
                    if value is None:
                        value = ""
                    elif isinstance(value, (dict, list)):
                        try:
                            value = json.dumps(value, ensure_ascii=False)
                        except:
                            value = str(value)
                    elif isinstance(value, bool):
                        value = "Да" if value else "Нет"
                    data_row.append(str(value))
                writer.writerow(data_row)
        
        log(f"Данные сохранены в CSV: {filename}")
        
    except Exception as e:
        log(f"Ошибка при сохранении CSV: {str(e)}")

def save_stats(stats, total_companies):
    """Сохраняет статистику парсинга"""
    stats["end_time"] = datetime.now().isoformat()
    if total_companies > 0:
        stats["success_rate"] = round((stats["success_companies"] / total_companies) * 100, 2)
        stats["avg_time_per_company"] = round(stats["total_time"] / total_companies, 2)
    else:
        stats["success_rate"] = 0
        stats["avg_time_per_company"] = 0
    
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    
    log(f"Статистика сохранена: {STATS_FILE}")

def print_results(stats):
    """Выводит итоговые результаты"""
    log("\n" + "=" * 70)
    log("ИТОГИ ПАРСИНГА:")
    log("=" * 70)
    log(f"   Всего компаний: {stats['total_companies']:,}")
    log(f"   Успешно обработано: {stats['success_companies']:,} ({stats['success_rate']}%)")
    log(f"   Деклараций получено: {stats['success_declarations']:,}")
    log(f"   Ошибок API: {stats['api_errors']}")
    log(f"   Ошибок авторизации: {stats['auth_errors']}")
    log(f"   Исключений: {stats['exceptions']}")
    log(f"   Общее время: {stats['total_time']:.2f} сек")
    log(f"   Среднее время на компанию: {stats['avg_time_per_company']:.2f} сек")
    log("=" * 70)

async def main():
    """Основная функция"""
    log("Запуск парсера FSA для 38000+ реестров")
    log("=" * 70)
    
    # Проверяем файл со ссылками
    if not Path(LINKS_FILE).exists():
        log(f"ОШИБКА: Файл {LINKS_FILE} не найден!")
        log("Сначала запустите collect_links.py для сбора ссылок")
        return
    
    # Читаем ссылки
    with open(LINKS_FILE, "r", encoding="utf-8") as f:
        links = [line.strip() for line in f if line.strip()]
    
    total = len(links)
    log(f"Найдено ссылок для обработки: {total:,}")
    log(f"Параллельных запросов: {CONCURRENCY}")
    
    if total == 0:
        log("Нет ссылок для обработки")
        return
    
    # Статистика
    stats = {
        "start_time": datetime.now().isoformat(),
        "total_companies": total,
        "success_companies": 0,
        "success_declarations": 0,
        "errors": 0,
        "api_errors": 0,
        "auth_errors": 0,
        "exceptions": 0,
        "total_time": 0
    }
    
    # Создаем Excel файл с заголовками заранее
    # Сначала соберем базовые колонки, которые точно будут
    base_columns = {
        "source_url", "company_id", "parsing_timestamp", 
        "processing_time", "error", "company_error", 
        "declaration_error", "declaration_doc_id"
    }
    
    log("Создаю Excel файл с базовыми заголовками...")
    sorted_columns = create_excel_with_headers(base_columns, OUTPUT_FILE)
    if not sorted_columns:
        log("ОШИБКА: Не удалось создать Excel файл")
        return
    
    # Собираем все данные
    all_rows = []
    all_columns = set(base_columns)
    
    # Создаем сессию
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    
    async with aiohttp.ClientSession(headers=BASE_HEADERS, timeout=timeout) as session:
        # Создаем задачи для всех ссылок
        tasks = []
        for i, url in enumerate(links, 1):
            task = process_company(session, url, i, total, stats, semaphore)
            tasks.append(task)
        
        # Обрабатываем результаты по мере готовности
        completed = 0
        batch_rows = []  # Временный буфер для пакетной записи
        batch_size = 100
        
        for future in asyncio.as_completed(tasks):
            try:
                row = await future
                batch_rows.append(row)
                
                # Обновляем набор колонок
                all_columns.update(row.keys())
                
                completed += 1
                
                # Периодическое сохранение
                if completed % batch_size == 0:
                    log(f"Обработано: {completed:,}/{total:,} ({completed/total*100:.1f}%)")
                    
                    # Обновляем файл с новыми данными
                    try:
                        # Создаем новый файл с полным набором колонок, если он изменился
                        current_columns = sorted(all_columns)
                        if set(current_columns) != set(sorted_columns):
                            # Создаем новый файл с обновленными заголовками
                            log("Обнаружены новые колонки, обновляю заголовки...")
                            sorted_columns = create_excel_with_headers(all_columns, OUTPUT_FILE)
                            # Перезаписываем все данные
                            append_data_to_excel(all_rows, sorted_columns, OUTPUT_FILE)
                        else:
                            # Просто добавляем новые данные
                            append_data_to_excel(batch_rows, sorted_columns, OUTPUT_FILE)
                    except Exception as e:
                        log(f"Ошибка при промежуточном сохранении: {str(e)}")
                        # Сохраняем в CSV как запасной вариант
                        save_to_csv(batch_rows, all_columns, f"backup_{completed}.csv")
                    
                    # Добавляем пакет к основным данным и очищаем буфер
                    all_rows.extend(batch_rows)
                    batch_rows = []
                    
            except Exception as e:
                log(f"Ошибка в задаче: {str(e)}")
                stats["exceptions"] += 1
    
    # Добавляем оставшиеся данные
    if batch_rows:
        try:
            # Проверяем, нужно ли обновить заголовки
            current_columns = sorted(all_columns)
            if set(current_columns) != set(sorted_columns):
                sorted_columns = create_excel_with_headers(all_columns, OUTPUT_FILE)
                # Перезаписываем все данные
                append_data_to_excel(all_rows, sorted_columns, OUTPUT_FILE)
            else:
                append_data_to_excel(batch_rows, sorted_columns, OUTPUT_FILE)
        except Exception as e:
            log(f"Ошибка при финальном сохранении: {str(e)}")
            save_to_csv(batch_rows, all_columns, "backup_final.csv")
        
        all_rows.extend(batch_rows)
    
    # Также сохраняем в CSV для надежности
    log("Создаю резервную копию в CSV формате...")
    save_to_csv(all_rows, all_columns, OUTPUT_CSV)
    
    # Сохраняем статистику
    save_stats(stats, total)
    
    # Итоги
    print_results(stats)
    log(f"\nФайл с данными (Excel): {OUTPUT_FILE}")
    log(f"Файл с данными (CSV): {OUTPUT_CSV}")
    log(f"Статистика: {STATS_FILE}")
    log("Парсинг завершен успешно!")

if __name__ == "__main__":
    # Исправляем кодировку для Windows
    if sys.platform == "win32":
        import io
        import locale
        
        try:
            # Пытаемся установить UTF-8
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
        except:
            pass
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("\nПарсинг прерван пользователем")
    except Exception as e:
        log(f"Критическая ошибка: {str(e)}")
        import traceback
        traceback.print_exc()
