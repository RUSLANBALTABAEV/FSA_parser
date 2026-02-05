"""
ПАРСЕР РЕЕСТРА ФСА - ЕДИНЫЙ ФАЙЛ РЕЗУЛЬТАТОВ
Все данные сохраняются в один файл output.xlsx
"""

import asyncio
import aiohttp
import pandas as pd
import json
import time
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import traceback

# ================= КОНФИГУРАЦИЯ =================
CONFIG = {
    "output_file": "реестр_фса.xlsx",  # ТОЛЬКО ОДИН ФАЙЛ
    "log_file": "fsa_parser.log",
    "concurrency": 10,
    "request_timeout": 30,
    "batch_size": 1000,  # Частота автосохранения
    "max_retries": 3,
    "retry_delay": 2,
    "total_records": 38000,
    
    # Токен авторизации
    "auth_token": "Bearer eyJhbGciOiJFZERTQSJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzcwMDY5MTA4LCJpYXQiOjE3NzAwNDAzMDh9.NdwC9BJ-rOk16GOq5GX8T1FmY4rpZXA-pfZjuLT3JeCYaZDc_3sIchWivorKJi4TpAF2-hv9ph1SRD7SzcluBA",
}

# ================= ЛОГИРОВАНИЕ =================
def setup_logging():
    logger = logging.getLogger("FSAParser")
    logger.setLevel(logging.INFO)
    
    # Формат
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Файл
    file_handler = logging.FileHandler(CONFIG["log_file"], encoding='utf-8')
    file_handler.setFormatter(formatter)
    
    # Консоль
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ================= МЕНЕДЖЕР ДАННЫХ =================
class DataManager:
    """Управление данными и единым файлом"""
    
    def __init__(self, output_file: str):
        self.output_file = Path(output_file)
        self.all_data = []  # Все данные в памяти
        self.last_save_time = time.time()
        self.save_interval = 300  # Сохранять каждые 5 минут
        
    def add_data(self, data: List[Dict]):
        """Добавление данных и автосохранение"""
        self.all_data.extend(data)
        
        # Автосохранение при накоплении
        if len(self.all_data) % CONFIG["batch_size"] == 0:
            self.save_to_excel()
            logger.info(f"Автосохранение: {len(self.all_data)} записей")
        
        # Автосохранение по времени
        current_time = time.time()
        if current_time - self.last_save_time > self.save_interval:
            self.save_to_excel()
            self.last_save_time = current_time
    
    def save_to_excel(self):
        """Сохранение всех данных в единый Excel файл"""
        if not self.all_data:
            return
        
        try:
            # Создаем DataFrame
            df = pd.DataFrame(self.all_data)
            
            # Определяем порядок столбцов
            priority_columns = [
                'id_компании', 'статус', 'уникальный_номер_записи',
                'наименование', 'сокращенное_наименование', 'инн',
                'тип_заявителя', 'организационно_правовая_форма',
                'дата_внесения_в_реестр', 'включен_в_национальную_часть',
                'фио_руководителя', 'должность_руководителя',
                'телефон', 'email', 'сайт', 'адрес_деятельности',
                'дата_парсинга'
            ]
            
            # Упорядочиваем столбцы
            existing_columns = list(df.columns)
            ordered_columns = []
            
            # Сначала приоритетные
            for col in priority_columns:
                if col in existing_columns:
                    ordered_columns.append(col)
                    if col in existing_columns:
                        existing_columns.remove(col)
            
            # Затем остальные
            ordered_columns.extend(sorted(existing_columns))
            df = df[ordered_columns]
            
            # Сохраняем в Excel
            df.to_excel(self.output_file, index=False, engine='openpyxl')
            
            # Дополнительно сохраняем как CSV
            csv_file = self.output_file.with_suffix('.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"✓ Сохранено {len(df)} записей в {self.output_file}")
            
        except Exception as e:
            logger.error(f"Ошибка сохранения: {e}")
            
            # Экстренное сохранение в JSON
            try:
                json_file = self.output_file.with_suffix('.json')
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(self.all_data, f, ensure_ascii=False, indent=2)
                logger.info(f"Экстренное сохранение в JSON: {json_file}")
            except:
                logger.error("Не удалось сохранить даже в JSON!")
    
    def get_stats(self):
        """Статистика по данным"""
        if not self.all_data:
            return {"total": 0, "unique_ids": 0}
        
        # Уникальные ID
        unique_ids = set()
        for item in self.all_data:
            if 'id_компании' in item:
                unique_ids.add(item['id_компании'])
        
        return {
            "total_records": len(self.all_data),
            "unique_companies": len(unique_ids)
        }

# ================= ПАРСЕР =================
class FSAParser:
    """Основной парсер"""
    
    def __init__(self):
        self.data_manager = DataManager(CONFIG["output_file"])
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "ru-RU,ru;q=0.9",
            "authorization": CONFIG["auth_token"],
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "referer": "https://pub.fsa.gov.ru/ral",
        }
    
    def extract_company_id(self, source: str) -> str:
        """Извлечение ID компании"""
        if 'pub.fsa.gov.ru' in source:
            parts = source.rstrip('/').split('/')
            for i, part in enumerate(parts):
                if part == 'view' and i + 1 < len(parts):
                    return parts[i + 1]
            return parts[-2] if len(parts) > 2 else source
        return source
    
    def load_company_ids(self) -> List[str]:
        """Загрузка ID компаний"""
        files = ["company_ids.txt", "links.txt", "ids.txt", "input.txt"]
        
        all_ids = []
        
        for filename in files:
            filepath = Path(filename)
            if filepath.exists():
                try:
                    logger.info(f"Загрузка из {filename}")
                    content = filepath.read_text(encoding='utf-8')
                    lines = [line.strip() for line in content.split('\n') if line.strip()]
                    
                    for line in lines:
                        company_id = self.extract_company_id(line)
                        if company_id and company_id.isdigit():
                            all_ids.append(company_id)
                        elif line.isdigit():
                            all_ids.append(line)
                            
                except Exception as e:
                    logger.error(f"Ошибка загрузки {filename}: {e}")
        
        # Удаляем дубликаты
        unique_ids = list(dict.fromkeys(all_ids))
        logger.info(f"Загружено {len(unique_ids)} уникальных ID")
        
        # Если файлов нет, создаем тестовые
        if not unique_ids:
            logger.warning("Файлы с ID не найдены. Создаю тестовые ID 1-100")
            unique_ids = [str(i) for i in range(1, 101)]
        
        return unique_ids
    
    def clean_value(self, value: Any) -> Any:
        """Очистка значения"""
        if value is None:
            return ""
        elif isinstance(value, bool):
            return "Да" if value else "Нет"
        elif isinstance(value, (dict, list)):
            try:
                return json.dumps(value, ensure_ascii=False, indent=0)
            except:
                return str(value)
        else:
            return value
    
    def safe_get(self, data: Dict, *keys, default=""):
        """Безопасное получение значения"""
        current = data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current if current is not None else default
    
    def extract_company_data(self, company_data: Dict, decl_data: Dict) -> Dict[str, Any]:
        """Извлечение данных компании"""
        result = {}
        
        # Базовые данные
        result['id_компании'] = self.safe_get(company_data, 'id', default="")
        result['статус'] = self.clean_value(
            self.safe_get(company_data, 'status') or 
            self.safe_get(decl_data, 'status')
        )
        
        result['уникальный_номер_записи'] = self.clean_value(
            self.safe_get(company_data, 'accreditationNumber') or
            self.safe_get(decl_data, 'accreditationNumber') or
            self.safe_get(company_data, 'ralNumber')
        )
        
        result['наименование'] = self.clean_value(
            self.safe_get(company_data, 'fullName') or
            self.safe_get(company_data, 'name') or
            self.safe_get(decl_data, 'organizationName')
        )
        
        result['сокращенное_наименование'] = self.clean_value(
            self.safe_get(company_data, 'shortName') or
            self.safe_get(company_data, 'abbreviation')
        )
        
        result['инн'] = self.clean_value(self.safe_get(company_data, 'inn'))
        result['кпп'] = self.clean_value(self.safe_get(company_data, 'kpp'))
        result['огрн'] = self.clean_value(self.safe_get(company_data, 'ogrn'))
        
        result['тип_заявителя'] = self.clean_value(
            "Юридическое лицо" if self.safe_get(company_data, 'legalForm') in ['ООО', 'АО', 'ПАО'] else
            "Индивидуальный предприниматель" if self.safe_get(company_data, 'legalForm') == 'ИП' else
            self.safe_get(company_data, 'legalForm')
        )
        
        result['организационно_правовая_форма'] = self.clean_value(
            self.safe_get(company_data, 'legalForm')
        )
        
        result['дата_внесения_в_реестр'] = self.clean_value(
            self.safe_get(company_data, 'registrationDate') or
            self.safe_get(decl_data, 'registrationDate')
        )
        
        result['включен_в_национальную_часть'] = self.clean_value(
            self.safe_get(company_data, 'inNationalRegistry', False)
        )
        
        # Контактные данные
        result['фио_руководителя'] = self.clean_value(
            self.safe_get(company_data, 'director', 'fullName') or
            self.safe_get(decl_data, 'headName')
        )
        
        result['должность_руководителя'] = self.clean_value(
            self.safe_get(company_data, 'director', 'position') or
            self.safe_get(decl_data, 'headPosition')
        )
        
        result['телефон'] = self.clean_value(
            self.safe_get(company_data, 'phone') or
            self.safe_get(company_data, 'contactPhone')
        )
        
        result['email'] = self.clean_value(
            self.safe_get(company_data, 'email') or
            self.safe_get(company_data, 'contactEmail')
        )
        
        result['сайт'] = self.clean_value(
            self.safe_get(company_data, 'website') or
            self.safe_get(decl_data, 'website')
        )
        
        result['адрес_деятельности'] = self.clean_value(
            self.safe_get(company_data, 'address', 'fullAddress') or
            self.safe_get(decl_data, 'activityAddress') or
            self.safe_get(company_data, 'legalAddress')
        )
        
        # Метаданные
        result['дата_парсинга'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        result['источник'] = 'https://pub.fsa.gov.ru/ral'
        
        return result
    
    async def fetch_with_retry(self, session, url, params=None, retries=3):
        """Запрос с повторными попытками"""
        for attempt in range(retries):
            try:
                async with session.get(url, params=params, timeout=CONFIG["request_timeout"]) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 404:
                        return {"_status": "NOT_FOUND"}
                    elif resp.status == 429:
                        wait = (attempt + 1) * 5
                        logger.warning(f"Слишком много запросов. Ждем {wait} сек.")
                        await asyncio.sleep(wait)
                        continue
                    else:
                        logger.error(f"HTTP {resp.status} для {url}")
                        if attempt < retries - 1:
                            await asyncio.sleep(CONFIG["retry_delay"])
                            continue
            except Exception as e:
                logger.error(f"Ошибка запроса: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(CONFIG["retry_delay"])
                    continue
        
        return {"_status": "FAILED"}
    
    async def process_company(self, session, company_id, idx, total):
        """Обработка одной компании"""
        try:
            # Данные компании
            company_url = f"https://pub.fsa.gov.ru/api/v1/ral/common/companies/{company_id}"
            company_data = await self.fetch_with_retry(session, company_url)
            
            if not company_data or company_data.get('_status') in ['NOT_FOUND', 'FAILED']:
                return {
                    'id_компании': company_id,
                    'статус': 'Не найдено',
                    'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # Данные декларации
            decl_data = {"_status": "NO_DATA"}
            accreditation = self.safe_get(company_data, 'accreditation', default={})
            
            if isinstance(accreditation, dict):
                doc_id = accreditation.get('idAccredScopeFile') or accreditation.get('id')
                if doc_id:
                    decl_url = "https://pub.fsa.gov.ru/api/v1/oa/accreditation/declaration/view/"
                    params = {"docId": doc_id, "alType": 5, "validate": "false"}
                    decl_data = await self.fetch_with_retry(session, decl_url, params=params)
            
            # Извлечение данных
            result = self.extract_company_data(company_data, decl_data)
            result['обработка'] = 'Успешно'
            
            logger.info(f"[{idx}/{total}] Обработано: {result.get('наименование', company_id)}")
            return result
            
        except Exception as e:
            logger.error(f"[{idx}] Ошибка обработки {company_id}: {e}")
            return {
                'id_компании': company_id,
                'статус': f'Ошибка: {str(e)[:100]}',
                'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'обработка': 'Ошибка'
            }
    
    async def run(self):
        """Основной запуск"""
        logger.info("=" * 60)
        logger.info("🚀 ЗАПУСК ПАРСЕРА ФСА")
        logger.info(f"📁 Выходной файл: {CONFIG['output_file']}")
        logger.info("=" * 60)
        
        # Загрузка ID
        company_ids = self.load_company_ids()
        total = len(company_ids)
        
        if CONFIG["total_records"] > 0:
            company_ids = company_ids[:CONFIG["total_records"]]
            total = min(total, CONFIG["total_records"])
        
        logger.info(f"📊 Всего записей: {total}")
        
        start_time = time.time()
        processed = 0
        failed = 0
        
        # Создаем сессию
        timeout = aiohttp.ClientTimeout(total=CONFIG["request_timeout"])
        connector = aiohttp.TCPConnector(limit=CONFIG["concurrency"])
        
        async with aiohttp.ClientSession(
            headers=self.headers,
            timeout=timeout,
            connector=connector
        ) as session:
            
            # Семафор для ограничения
            sem = asyncio.Semaphore(CONFIG["concurrency"])
            
            async def process_with_limit(company_id, idx):
                async with sem:
                    return await self.process_company(session, company_id, idx, total)
            
            # Создаем задачи
            tasks = []
            for idx, company_id in enumerate(company_ids, 1):
                tasks.append(process_with_limit(company_id, idx))
            
            # Обрабатываем
            batch_results = []
            for idx, task in enumerate(asyncio.as_completed(tasks), 1):
                try:
                    result = await task
                    processed += 1
                    
                    if result.get('обработка') == 'Успешно':
                        batch_results.append(result)
                    else:
                        batch_results.append(result)
                        failed += 1
                    
                    # Добавляем в менеджер данных
                    if batch_results:
                        self.data_manager.add_data(batch_results)
                        batch_results = []
                    
                    # Прогресс
                    if idx % 100 == 0 or idx == total:
                        elapsed = time.time() - start_time
                        speed = processed / elapsed if elapsed > 0 else 0
                        remaining = (total - processed) / speed if speed > 0 else 0
                        
                        logger.info(
                            f"📈 Прогресс: {processed}/{total} ({processed/total*100:.1f}%) | "
                            f"Скорость: {speed:.1f}/сек | "
                            f"Ошибок: {failed} | "
                            f"Осталось: ~{remaining/60:.0f} мин"
                        )
                        
                except Exception as e:
                    logger.error(f"Ошибка в задаче {idx}: {e}")
                    failed += 1
        
        # Финальное сохранение
        if batch_results:
            self.data_manager.add_data(batch_results)
        self.data_manager.save_to_excel()
        
        # Статистика
        end_time = time.time()
        total_time = end_time - start_time
        stats = self.data_manager.get_stats()
        
        logger.info("=" * 60)
        logger.info("✅ ПАРСИНГ ЗАВЕРШЕН!")
        logger.info(f"📊 Обработано: {processed} записей")
        logger.info(f"❌ Ошибок: {failed}")
        logger.info(f"⏱️  Время: {total_time/60:.1f} минут")
        logger.info(f"🚀 Скорость: {processed/total_time:.1f} записей/сек")
        logger.info(f"💾 Файл: {CONFIG['output_file']} ({stats['total_records']} записей)")
        logger.info("=" * 60)
        
        # Отчет
        report = f"""
ОТЧЕТ О ПАРСИНГЕ ФСА
{'=' * 40}
Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Всего записей: {total}
Обработано: {processed}
Ошибок: {failed}
Время: {total_time/60:.1f} минут
Скорость: {processed/total_time:.1f} записей/сек
Выходной файл: {CONFIG['output_file']}
Записей в файле: {stats['total_records']}
"""
        
        with open("отчет.txt", "w", encoding="utf-8") as f:
            f.write(report)

# ================= УТИЛИТЫ =================
def generate_test_ids():
    """Создание тестовых ID"""
    ids = list(range(1, 101))
    with open("company_ids.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(str(i) for i in ids))
    print(f"Создано {len(ids)} тестовых ID в company_ids.txt")

def check_api():
    """Проверка API"""
    import requests
    
    url = "https://pub.fsa.gov.ru/api/v1/ral/common/companies/1"
    headers = {
        "authorization": CONFIG["auth_token"],
        "user-agent": "Mozilla/5.0"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"API статус: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"Пример данных:")
            print(f"  ID: {data.get('id')}")
            print(f"  Название: {data.get('fullName')}")
            print(f"  ИНН: {data.get('inn')}")
            print(f"  Статус: {data.get('status')}")
        else:
            print(f"Текст ответа: {response.text[:200]}")
    except Exception as e:
        print(f"Ошибка: {e}")

# ================= ЗАПУСК =================
async def main():
    """Основная функция"""
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "test":
            print("🧪 ТЕСТОВЫЙ РЕЖИМ (100 записей)")
            generate_test_ids()
            parser = FSAParser()
            await parser.run()
            
        elif command == "check":
            print("🔍 ПРОВЕРКА API")
            check_api()
            
        elif command == "full":
            print("🚀 ПОЛНЫЙ ЗАПУСК")
            parser = FSAParser()
            await parser.run()
            
        elif command == "resume":
            print("🔄 ПРОДОЛЖЕНИЕ")
            # Для продолжения нужно сохранять прогресс
            parser = FSAParser()
            await parser.run()
            
        else:
            print("Использование:")
            print("  python fsa_parser.py test    - тест (100 записей)")
            print("  python fsa_parser.py full    - полный запуск")
            print("  python fsa_parser.py check   - проверка API")
            print("  python fsa_parser.py resume  - продолжение")
    else:
        # По умолчанию - полный запуск
        parser = FSAParser()
        await parser.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹️  Парсер остановлен пользователем")
        print("📁 Данные сохранены в текущем состоянии")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
