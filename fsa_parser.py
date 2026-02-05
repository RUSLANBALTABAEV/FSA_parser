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
from dataclasses import dataclass, field
import hashlib

# ================= КОНФИГУРАЦИЯ =================
@dataclass
class Config:
    """Конфигурация парсера"""
    # Пути файлов
    output_file: str = "реестр_фса.xlsx"
    log_file: str = "fsa_parser.log"
    
    # Настройки производительности
    concurrency: int = 5  # Уменьшено для стабильности
    request_timeout: int = 45
    batch_size: int = 500  # Сохранять каждые 500 записей
    max_retries: int = 2   # Уменьшено количество попыток
    retry_delay: int = 3
    
    # Ограничения
    max_records: int = 0  # 0 = все записи
    
    # Токен авторизации (важно обновить если истечет)
    auth_token: str = "eyJhbGciOiJFZERTQSJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzcwMjk3ODA3LCJpYXQiOjE3NzAyNjkwMDd9.--K03QrNpehr2-0opkxE_63AJSErHdE1g2BMinuQlNFTtSJg058RhXKgSDcJ-nl3Wb_xJTMCURPFo5J0z8bKAw"
    
    # URL API
    base_url: str = "https://pub.fsa.gov.ru"
    company_api: str = "/api/v1/ral/common/companies/{id}"
    declaration_api: str = "/api/v1/oa/accreditation/declaration/view/"
    
    # Заголовки
    headers: Dict[str, str] = field(default_factory=lambda: {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "authorization": "",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "referer": "https://pub.fsa.gov.ru/ral",
        "origin": "https://pub.fsa.gov.ru",
    })
    
    def __post_init__(self):
        """Инициализация после создания объекта"""
        self.headers["authorization"] = self.auth_token

CONFIG = Config()

# ================= ЛОГИРОВАНИЕ =================
def setup_logging():
    """Настройка логирования"""
    logger = logging.getLogger("FSAParser")
    logger.setLevel(logging.INFO)
    
    # Форматтер
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Файловый обработчик
    file_handler = logging.FileHandler(CONFIG.log_file, encoding='utf-8', mode='a')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Очищаем старые обработчики
    logger.handlers.clear()
    
    # Добавляем обработчики
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Отключаем логирование aiohttp
    aiohttp_logger = logging.getLogger('aiohttp')
    aiohttp_logger.setLevel(logging.WARNING)
    
    return logger

logger = setup_logging()

# ================= УТИЛИТЫ =================
def clean_value(value: Any) -> Any:
    """Очистка значения для сохранения"""
    if value is None:
        return ""
    elif isinstance(value, bool):
        return "Да" if value else "Нет"
    elif isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False, indent=0)
        except:
            return str(value)
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return str(value).strip()

def safe_get(data: Dict, *keys, default: Any = "") -> Any:
    """Безопасное получение значения из словаря"""
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return default
    return current if current is not None else default

def extract_company_id(source: str) -> str:
    """Извлечение ID компании из URL или строки"""
    if not source:
        return ""
    
    if 'pub.fsa.gov.ru' in source:
        parts = source.strip('/').split('/')
        for i, part in enumerate(parts):
            if part == 'view' and i + 1 < len(parts):
                return parts[i + 1]
        return parts[-2] if len(parts) > 2 else source
    
    return str(source).strip()

def generate_md5(text: str) -> str:
    """Генерация MD5 хеша для строки"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

# ================= ОБРАБОТКА ДАННЫХ =================
class DataProcessor:
    """Обработчик данных компании"""
    
    @staticmethod
    def extract_company_info(company_data: Dict) -> Dict[str, Any]:
        """Извлечение информации о компании"""
        result = {}
        
        # Базовые данные
        result['id_компании'] = clean_value(safe_get(company_data, 'id'))
        result['статус'] = clean_value(safe_get(company_data, 'status'))
        result['наименование'] = clean_value(safe_get(company_data, 'fullName'))
        result['сокращенное_наименование'] = clean_value(safe_get(company_data, 'shortName'))
        
        # Реквизиты
        result['инн'] = clean_value(safe_get(company_data, 'inn'))
        result['кпп'] = clean_value(safe_get(company_data, 'kpp'))
        result['огрн'] = clean_value(safe_get(company_data, 'ogrn'))
        result['окпо'] = clean_value(safe_get(company_data, 'okpo'))
        
        # Тип организации
        result['тип_заявителя'] = clean_value(safe_get(company_data, 'legalForm'))
        result['организационно_правовая_форма'] = clean_value(safe_get(company_data, 'legalForm'))
        result['государственное_предприятие'] = clean_value(safe_get(company_data, 'isStateOwned', False))
        result['иностранная_организация'] = clean_value(safe_get(company_data, 'isForeign', False))
        
        # Контактные данные
        result['телефон'] = clean_value(safe_get(company_data, 'phone'))
        result['email'] = clean_value(safe_get(company_data, 'email'))
        result['сайт'] = clean_value(safe_get(company_data, 'website'))
        
        # Адреса
        address_data = safe_get(company_data, 'address', default={})
        if isinstance(address_data, dict):
            result['адрес_места_нахождения'] = clean_value(safe_get(address_data, 'fullAddress'))
            result['адрес_почтовый'] = clean_value(safe_get(address_data, 'postalAddress'))
        else:
            result['адрес_места_нахождения'] = clean_value(address_data)
        
        # Руководитель
        director_data = safe_get(company_data, 'director', default={})
        if isinstance(director_data, dict):
            result['фио_руководителя'] = clean_value(safe_get(director_data, 'fullName'))
            result['должность_руководителя'] = clean_value(safe_get(director_data, 'position'))
            result['телефон_руководителя'] = clean_value(safe_get(director_data, 'phone'))
        
        # Налоговые данные
        tax_data = safe_get(company_data, 'taxAuthority', default={})
        if isinstance(tax_data, dict):
            result['налоговый_орган'] = clean_value(safe_get(tax_data, 'name'))
        else:
            result['налоговый_орган'] = clean_value(tax_data)
        
        result['дата_постановки_на_учет'] = clean_value(safe_get(company_data, 'registrationDate'))
        
        # Метаданные
        result['дата_парсинга'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        result['хеш_данных'] = generate_md5(str(company_data))
        
        return result
    
    @staticmethod
    def extract_accreditation_info(company_data: Dict, decl_data: Dict) -> Dict[str, Any]:
        """Извлечение информации об аккредитации"""
        result = {}
        
        # Данные из компании
        accreditation = safe_get(company_data, 'accreditation', default={})
        if isinstance(accreditation, dict):
            result['уникальный_номер_записи'] = clean_value(safe_get(accreditation, 'accreditationNumber'))
            result['статус_аккредитации'] = clean_value(safe_get(accreditation, 'status'))
            result['дата_аккредитации'] = clean_value(safe_get(accreditation, 'accreditationDate'))
            result['срок_действия'] = clean_value(safe_get(accreditation, 'validUntil'))
        
        # Данные из декларации
        if decl_data and isinstance(decl_data, dict) and decl_data.get('_status') not in ['NOT_FOUND', 'SERVER_ERROR']:
            result['дата_внесения_в_реестр'] = clean_value(safe_get(decl_data, 'registrationDate'))
            result['включен_в_национальную_часть'] = clean_value(safe_get(decl_data, 'inNationalRegistry', False))
            result['наименование_стандарта'] = clean_value(safe_get(decl_data, 'standard', 'name'))
            
            # Область аккредитации
            scope_data = safe_get(decl_data, 'accreditationScope', default=[])
            if isinstance(scope_data, list) and scope_data:
                scope_texts = []
                for item in scope_data[:5]:  # Берем первые 5
                    if isinstance(item, dict):
                        desc = safe_get(item, 'description') or safe_get(item, 'name')
                        if desc:
                            scope_texts.append(desc)
                result['область_аккредитации'] = clean_value(" | ".join(scope_texts))
        
        return result
    
    @staticmethod
    def process_company(company_id: str, company_data: Dict, decl_data: Dict) -> Dict[str, Any]:
        """Полная обработка данных компании"""
        result = {
            'id_компании': company_id,
            'статус_обработки': 'УСПЕШНО',
            'ошибки_декларации': 'Нет'
        }
        
        try:
            # Извлекаем информацию о компании
            company_info = DataProcessor.extract_company_info(company_data)
            result.update(company_info)
            
            # Извлекаем информацию об аккредитации
            accreditation_info = DataProcessor.extract_accreditation_info(company_data, decl_data)
            result.update(accreditation_info)
            
            # Проверяем наличие ошибок декларации
            if decl_data and decl_data.get('_status') == 'SERVER_ERROR':
                result['ошибки_декларации'] = 'Ошибка сервера (500)'
                result['статус_обработки'] = 'ЧАСТИЧНО'
            elif decl_data and decl_data.get('_status') == 'NOT_FOUND':
                result['ошибки_декларации'] = 'Декларация не найдена'
                result['статус_обработки'] = 'ЧАСТИЧНО'
            
        except Exception as e:
            result['статус_обработки'] = f'ОШИБКА: {str(e)[:100]}'
            result['ошибки_декларации'] = 'Ошибка обработки данных'
            logger.error(f"Ошибка обработки компании {company_id}: {e}")
        
        return result

# ================= API КЛИЕНТ =================
class APIClient:
    """Клиент для работы с API ФСА"""
    
    def __init__(self):
        self.base_url = CONFIG.base_url
        self.headers = CONFIG.headers.copy()
        self.timeout = aiohttp.ClientTimeout(total=CONFIG.request_timeout)
        
    async def make_request(self, session: aiohttp.ClientSession, url: str, 
                          params: Optional[Dict] = None) -> Dict:
        """Выполнение запроса с обработкой ошибок"""
        try:
            async with session.get(
                url, 
                params=params, 
                timeout=self.timeout,
                ssl=False
            ) as response:
                
                # Логируем статус
                if response.status != 200:
                    logger.debug(f"Запрос {url} вернул статус {response.status}")
                
                if response.status == 200:
                    return await response.json()
                elif response.status == 404:
                    return {"_status": "NOT_FOUND"}
                elif response.status == 500:
                    # Ошибка сервера - не повторяем запрос
                    logger.warning(f"Сервер вернул 500 для {url}")
                    return {"_status": "SERVER_ERROR", "status_code": 500}
                elif response.status == 429:
                    # Слишком много запросов
                    logger.warning(f"Слишком много запросов (429) для {url}")
                    await asyncio.sleep(10)
                    return {"_status": "TOO_MANY_REQUESTS"}
                else:
                    return {"_status": f"HTTP_{response.status}"}
                    
        except asyncio.TimeoutError:
            logger.warning(f"Таймаут запроса: {url}")
            return {"_status": "TIMEOUT"}
        except aiohttp.ClientError as e:
            logger.warning(f"Ошибка клиента для {url}: {e}")
            return {"_status": "CLIENT_ERROR", "error": str(e)}
        except Exception as e:
            logger.error(f"Неожиданная ошибка для {url}: {e}")
            return {"_status": "UNKNOWN_ERROR", "error": str(e)}
    
    async def get_company(self, session: aiohttp.ClientSession, company_id: str) -> Dict:
        """Получение данных компании"""
        url = f"{self.base_url}{CONFIG.company_api.format(id=company_id)}"
        return await self.make_request(session, url)
    
    async def get_declaration(self, session: aiohttp.ClientSession, doc_id: str) -> Dict:
        """Получение данных декларации"""
        if not doc_id:
            return {"_status": "NO_DOC_ID"}
        
        url = f"{self.base_url}{CONFIG.declaration_api}"
        params = {"docId": doc_id, "alType": 5, "validate": "false"}
        
        # Пробуем разные варианты параметров при ошибке 500
        result = await self.make_request(session, url, params)
        
        # Если серверная ошибка, пробуем без alType
        if result.get('_status') == 'SERVER_ERROR':
            alt_params = {"docId": doc_id, "validate": "false"}
            alt_result = await self.make_request(session, url, alt_params)
            if alt_result.get('_status') != 'SERVER_ERROR':
                return alt_result
        
        return result

# ================= МЕНЕДЖЕР ДАННЫХ =================
class DataManager:
    """Управление данными и файлами"""
    
    def __init__(self):
        self.output_file = Path(CONFIG.output_file)
        self.all_data = []
        self.processed_ids = set()
        
    def add_data(self, data: Dict):
        """Добавление данных"""
        company_id = data.get('id_компании')
        if company_id and company_id not in self.processed_ids:
            self.all_data.append(data)
            self.processed_ids.add(company_id)
            
            # Автосохранение
            if len(self.all_data) % CONFIG.batch_size == 0:
                self.save_to_excel()
                logger.info(f"Автосохранение: {len(self.all_data)} записей")
    
    def save_to_excel(self) -> bool:
        """Сохранение данных в Excel"""
        if not self.all_data:
            return False
        
        try:
            df = pd.DataFrame(self.all_data)
            
            # Определяем порядок столбцов
            priority_columns = [
                'id_компании', 'статус', 'наименование', 'сокращенное_наименование',
                'инн', 'кпп', 'огрн', 'тип_заявителя', 'организационно_правовая_форма',
                'уникальный_номер_записи', 'статус_аккредитации', 'дата_аккредитации',
                'телефон', 'email', 'сайт', 'адрес_места_нахождения',
                'фио_руководителя', 'должность_руководителя',
                'дата_внесения_в_реестр', 'включен_в_национальную_часть',
                'статус_обработки', 'ошибки_декларации', 'дата_парсинга'
            ]
            
            # Упорядочиваем столбцы
            existing_columns = list(df.columns)
            ordered_columns = []
            
            for col in priority_columns:
                if col in existing_columns:
                    ordered_columns.append(col)
                    if col in existing_columns:
                        existing_columns.remove(col)
            
            ordered_columns.extend(sorted(existing_columns))
            df = df[ordered_columns]
            
            # Сохраняем в Excel
            df.to_excel(self.output_file, index=False, engine='openpyxl')
            
            # Резервная копия в CSV
            csv_file = self.output_file.with_suffix('.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"✓ Сохранено {len(df)} записей в {self.output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения Excel: {e}")
            
            # Экстренное сохранение в JSON
            try:
                json_file = self.output_file.with_suffix('.json')
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(self.all_data, f, ensure_ascii=False, indent=2)
                logger.info(f"Экстренное сохранение в JSON: {json_file}")
            except:
                logger.error("Не удалось сохранить даже в JSON")
            
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """Получение статистики"""
        if not self.all_data:
            return {"total": 0, "unique": 0}
        
        status_stats = {}
        error_stats = {}
        
        for item in self.all_data:
            status = item.get('статус_обработки', 'Неизвестно')
            error = item.get('ошибки_декларации', 'Нет')
            
            status_stats[status] = status_stats.get(status, 0) + 1
            if error != 'Нет':
                error_stats[error] = error_stats.get(error, 0) + 1
        
        return {
            "total": len(self.all_data),
            "unique": len(self.processed_ids),
            "status_stats": status_stats,
            "error_stats": error_stats
        }

# ================= ОСНОВНОЙ ПАРСЕР =================
class FSAParser:
    """Основной класс парсера"""
    
    def __init__(self):
        self.data_manager = DataManager()
        self.api_client = APIClient()
        self.data_processor = DataProcessor()
        
        self.total_processed = 0
        self.total_success = 0
        self.total_server_errors = 0
        self.total_failed = 0
        
    def load_company_ids(self) -> List[str]:
        """Загрузка ID компаний из файлов"""
        possible_files = [
            "company_ids.txt",
            "links.txt",
            "ids.txt",
            "input.txt",
            "список.txt"
        ]
        
        all_ids = []
        
        for filename in possible_files:
            filepath = Path(filename)
            if filepath.exists():
                try:
                    logger.info(f"Загрузка ID из {filename}")
                    
                    content = filepath.read_text(encoding='utf-8', errors='ignore')
                    lines = [line.strip() for line in content.split('\n') if line.strip()]
                    
                    for line in lines:
                        # Пропускаем комментарии
                        if line.startswith('#') or line.startswith('//'):
                            continue
                        
                        company_id = extract_company_id(line)
                        if company_id and company_id.isdigit():
                            all_ids.append(company_id)
                        elif line.isdigit():
                            all_ids.append(line)
                    
                    logger.info(f"  Загружено {len(lines)} строк из {filename}")
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки {filename}: {e}")
        
        # Удаляем дубликаты
        seen = set()
        unique_ids = []
        for id_ in all_ids:
            if id_ not in seen:
                seen.add(id_)
                unique_ids.append(id_)
        
        logger.info(f"Всего уникальных ID: {len(unique_ids)}")
        
        # Если нет файлов - создаем тестовые
        if not unique_ids:
            logger.warning("Файлы с ID не найдены. Создаю тестовые ID 1-100")
            unique_ids = [str(i) for i in range(1, 101)]
        
        # Ограничиваем количество если нужно
        if CONFIG.max_records > 0 and len(unique_ids) > CONFIG.max_records:
            logger.info(f"Ограничение до {CONFIG.max_records} записей")
            unique_ids = unique_ids[:CONFIG.max_records]
        
        return unique_ids
    
    async def process_single_company(self, session: aiohttp.ClientSession, 
                                    company_id: str, idx: int, total: int) -> Optional[Dict]:
        """Обработка одной компании"""
        try:
            logger.debug(f"[{idx}/{total}] Запрос компании {company_id}")
            
            # 1. Получаем данные компании
            company_data = await self.api_client.get_company(session, company_id)
            
            if not company_data or company_data.get('_status') in ['NOT_FOUND', 'SERVER_ERROR']:
                self.total_failed += 1
                logger.warning(f"[{idx}] Компания {company_id} не найдена или ошибка сервера")
                return {
                    'id_компании': company_id,
                    'статус': 'Не найдено или ошибка сервера',
                    'статус_обработки': 'ОШИБКА_API',
                    'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # 2. Пытаемся получить декларацию
            decl_data = {"_status": "NO_DOC_ID"}
            accreditation = safe_get(company_data, 'accreditation', default={})
            
            if isinstance(accreditation, dict):
                doc_id = safe_get(accreditation, 'idAccredScopeFile')
                if doc_id:
                    try:
                        decl_data = await self.api_client.get_declaration(session, doc_id)
                    except Exception as e:
                        logger.warning(f"[{idx}] Ошибка при запросе декларации: {e}")
                        decl_data = {"_status": "REQUEST_ERROR", "error": str(e)}
            
            # 3. Обрабатываем данные
            result = self.data_processor.process_company(company_id, company_data, decl_data)
            
            # 4. Обновляем статистику
            self.total_processed += 1
            
            if result.get('статус_обработки') == 'УСПЕШНО':
                self.total_success += 1
            elif result.get('ошибки_декларации', 'Нет') != 'Нет':
                self.total_server_errors += 1
            else:
                self.total_failed += 1
            
            # 5. Логируем результат
            status_icon = "✅" if result.get('статус_обработки') == 'УСПЕШНО' else "⚠️"
            logger.info(f"[{idx}] {status_icon} {result.get('наименование', company_id)[:50]}...")
            
            return result
            
        except Exception as e:
            self.total_failed += 1
            logger.error(f"[{idx}] Критическая ошибка обработки {company_id}: {e}")
            logger.error(traceback.format_exc())
            
            return {
                'id_компании': company_id,
                'статус': f'Критическая ошибка: {str(e)[:100]}',
                'статус_обработки': 'КРИТИЧЕСКАЯ_ОШИБКА',
                'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    async def run(self):
        """Основной запуск парсера"""
        logger.info("=" * 70)
        logger.info("🚀 ЗАПУСК ПАРСЕРА РЕЕСТРА ФСА")
        logger.info(f"📁 Выходной файл: {CONFIG.output_file}")
        logger.info(f"🧵 Конкурентность: {CONFIG.concurrency}")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        # Загрузка ID
        company_ids = self.load_company_ids()
        total = len(company_ids)
        
        if total == 0:
            logger.error("❌ Нет ID компаний для обработки!")
            return
        
        logger.info(f"📊 Всего записей для обработки: {total}")
        
        # Создаем сессию
        connector = aiohttp.TCPConnector(
            limit=CONFIG.concurrency,
            limit_per_host=CONFIG.concurrency,
            ssl=False
        )
        
        async with aiohttp.ClientSession(
            headers=self.api_client.headers,
            connector=connector,
            timeout=self.api_client.timeout
        ) as session:
            
            # Создаем семафор для ограничения
            semaphore = asyncio.Semaphore(CONFIG.concurrency)
            
            async def process_with_limit(company_id: str, idx: int):
                async with semaphore:
                    return await self.process_single_company(session, company_id, idx, total)
            
            # Создаем задачи
            tasks = []
            for idx, company_id in enumerate(company_ids, 1):
                tasks.append(process_with_limit(company_id, idx))
            
            # Обрабатываем задачи
            completed = 0
            last_log_time = time.time()
            
            for idx, task in enumerate(asyncio.as_completed(tasks), 1):
                try:
                    result = await task
                    completed += 1
                    
                    if result:
                        self.data_manager.add_data(result)
                    
                    # Логируем прогресс каждые 10 записей или 30 секунд
                    current_time = time.time()
                    if (completed % 10 == 0) or (current_time - last_log_time > 30):
                        elapsed = current_time - start_time
                        speed = completed / elapsed if elapsed > 0 else 0
                        remaining = total - completed
                        eta = remaining / speed if speed > 0 else 0
                        
                        logger.info(
                            f"📈 Прогресс: {completed}/{total} ({completed/total*100:.1f}%) | "
                            f"Скорость: {speed:.1f}/сек | "
                            f"Успешно: {self.total_success} | "
                            f"Ошибки 500: {self.total_server_errors} | "
                            f"Сбоев: {self.total_failed} | "
                            f"Осталось: ~{eta/60:.0f} мин"
                        )
                        
                        last_log_time = current_time
                    
                except Exception as e:
                    logger.error(f"Ошибка в основной задаче {idx}: {e}")
                    self.total_failed += 1
        
        # Финальное сохранение
        self.data_manager.save_to_excel()
        
        # Статистика
        end_time = time.time()
        total_time = end_time - start_time
        stats = self.data_manager.get_stats()
        
        logger.info("=" * 70)
        logger.info("✅ ПАРСИНГ ЗАВЕРШЕН!")
        logger.info(f"📊 Обработано записей: {self.total_processed}")
        logger.info(f"✅ Успешно: {self.total_success}")
        logger.info(f"⚠️  Ошибки деклараций: {self.total_server_errors}")
        logger.info(f"❌ Сбоев: {self.total_failed}")
        logger.info(f"⏱️  Общее время: {total_time/60:.1f} минут")
        logger.info(f"🚀 Средняя скорость: {self.total_processed/total_time:.1f} записей/сек")
        logger.info(f"💾 Файл: {CONFIG.output_file}")
        logger.info("=" * 70)
        
        # Статистика по статусам
        if 'status_stats' in stats:
            logger.info("📊 СТАТИСТИКА ПО СТАТУСАМ:")
            for status, count in stats['status_stats'].items():
                logger.info(f"  {status}: {count}")
        
        # Создание отчета
        self.create_report(total_time, stats)
    
    def create_report(self, total_time: float, stats: Dict):
        """Создание отчета о парсинге"""
        report = f"""
ОТЧЕТ О ПАРСИНГЕ РЕЕСТРА ФСА
{'=' * 50}

ДАТА ВЫПОЛНЕНИЯ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

СТАТИСТИКА:
- Всего записей для обработки: {self.total_processed + self.total_failed}
- Успешно обработано: {self.total_success}
- Записей с ошибками деклараций: {self.total_server_errors}
- Записей со сбоями: {self.total_failed}
- Процент успеха: {(self.total_success/self.total_processed*100):.1f}%

ВРЕМЯ ВЫПОЛНЕНИЯ:
- Общее время: {total_time/60:.1f} минут
- Средняя скорость: {self.total_processed/total_time:.1f} записей/сек

ВЫХОДНЫЕ ФАЙЛЫ:
1. {CONFIG.output_file} - основной файл Excel
2. {CONFIG.output_file.replace('.xlsx', '.csv')} - резервный файл CSV
3. {CONFIG.log_file} - файл логов

СТАТУСЫ ОБРАБОТКИ:
"""
        
        if 'status_stats' in stats:
            for status, count in stats['status_stats'].items():
                report += f"- {status}: {count}\n"
        
        report += f"""
ПРИМЕЧАНИЯ:
1. Ошибки HTTP 500 - это проблемы на стороне сервера ФСА
2. Данные компаний сохраняются даже при ошибках деклараций
3. Для повторного запуска удалите файл {CONFIG.output_file}
4. Лог содержит подробную информацию об ошибках
"""
        
        report_file = "отчет_о_парсинге.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"📄 Отчет сохранен в: {report_file}")

# ================= КОМАНДЫ И ЗАПУСК =================
def print_banner():
    """Вывод баннера"""
    banner = """
╔══════════════════════════════════════════════════╗
║           ПАРСЕР РЕЕСТРА ФСА v3.0               ║
║     Сбор данных с pub.fsa.gov.ru/ral            ║
╚══════════════════════════════════════════════════╝
"""
    print(banner)

def check_environment():
    """Проверка окружения"""
    print("🔍 Проверка окружения...")
    
    # Проверка Python
    print(f"  Python: {sys.version.split()[0]}")
    
    # Проверка библиотек
    required = ['aiohttp', 'pandas', 'openpyxl']
    for lib in required:
        try:
            __import__(lib)
            print(f"  ✅ {lib}")
        except ImportError:
            print(f"  ❌ {lib} - требуется установка")
    
    # Проверка файлов
    input_files = ["company_ids.txt", "links.txt", "ids.txt"]
    found = False
    for file in input_files:
        if Path(file).exists():
            print(f"  ✅ Найден файл: {file}")
            found = True
    
    if not found:
        print("  ⚠️  Файлы с ID не найдены. Будет создан тестовый файл.")

async def main():
    """Основная функция"""
    print_banner()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
    else:
        command = "help"
    
    if command == "run":
        print("🚀 Запуск полного парсинга...")
        check_environment()
        parser = FSAParser()
        await parser.run()
        
    elif command == "test":
        print("🧪 Тестовый запуск (100 записей)...")
        CONFIG.max_records = 100
        CONFIG.output_file = "тест_реестр.xlsx"
        
        # Создаем тестовый файл если его нет
        if not Path("company_ids.txt").exists():
            with open("company_ids.txt", "w", encoding="utf-8") as f:
                for i in range(1, 101):
                    f.write(f"{i}\n")
            print("✓ Создан файл company_ids.txt со 100 ID")
        
        parser = FSAParser()
        await parser.run()
        
    elif command == "check":
        print("🔍 Проверка API и токена...")
        import requests
        
        test_url = f"{CONFIG.base_url}/api/v1/ral/common/companies/1"
        headers = CONFIG.headers.copy()
        
        try:
            response = requests.get(test_url, headers=headers, timeout=10)
            print(f"Статус API: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("✅ API доступен и токен действителен")
                print(f"Пример данных:")
                print(f"  ID: {data.get('id')}")
                print(f"  Название: {data.get('fullName')}")
                print(f"  ИНН: {data.get('inn')}")
                print(f"  Статус: {data.get('status')}")
                
                # Сохраняем пример
                with open("пример_ответа_api.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print("✓ Пример сохранен в 'пример_ответа_api.json'")
                
            elif response.status_code == 401:
                print("❌ Ошибка авторизации. Токен недействителен.")
                print("Обновите токен в файле CONFIG.auth_token")
            elif response.status_code == 500:
                print("⚠️  Сервер вернул 500 ошибку. Проблемы на стороне ФСА.")
            else:
                print(f"⚠️  Неожиданный статус: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Ошибка подключения: {e}")
        
    elif command == "generate":
        print("📝 Создание файла с 38000 ID...")
        try:
            with open("company_ids_full.txt", "w", encoding="utf-8") as f:
                for i in range(1, 38001):
                    f.write(f"{i}\n")
                    if i % 5000 == 0:
                        print(f"  Записано {i} ID...")
            print(f"✓ Создан файл 'company_ids_full.txt' с 38000 ID")
        except Exception as e:
            print(f"❌ Ошибка создания файла: {e}")
    
    elif command == "help":
        print("""
📖 СПРАВКА ПО ИСПОЛЬЗОВАНИЮ:

Команды:
  python fsa_parser.py run      - Полный запуск парсинга
  python fsa_parser.py test     - Тестовый запуск (100 записей)
  python fsa_parser.py check    - Проверка API и токена
  python fsa_parser.py generate - Создать файл с 38000 ID
  python fsa_parser.py help     - Эта справка

Подготовка:
  1. Создайте файл company_ids.txt с ID компаний
     (каждый ID на новой строке: 1, 2, 3, ... 38000)
  
  2. Проверьте подключение: python fsa_parser.py check
  
  3. Протестируйте: python fsa_parser.py test
  
  4. Запустите полный парсинг: python fsa_parser.py run

Файлы:
  - company_ids.txt - список ID для обработки
  - реестр_фса.xlsx - результаты парсинга
  - fsa_parser.log - подробный лог выполнения
  - отчет_о_парсинге.txt - статистика

Примечания:
  - При ошибках HTTP 500 данные компании сохраняются
  - Декларации могут отсутствовать для некоторых записей
  - Токен авторизации может потребовать обновления
""")
    
    else:
        print(f"❌ Неизвестная команда: {command}")
        print("Используйте: python fsa_parser.py help")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Парсер остановлен пользователем")
        print("📁 Данные сохранены в текущем состоянии")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
        print("Подробности в лог-файле")
        traceback.print_exc()
