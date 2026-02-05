# fsa_parser_fixed.py

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
import random
from urllib.parse import quote

# ================= КОНФИГУРАЦИЯ =================
@dataclass
class Config:
    """Конфигурация парсера"""
    # Пути файлов
    output_file: str = "реестр_фса_полный.xlsx"
    log_file: str = "fsa_parser_fixed.log"
    
    # Настройки производительности
    concurrency: int = 3  # Уменьшено для стабильности
    request_timeout: int = 60  # Увеличено
    batch_size: int = 100  # Чаще сохраняем
    max_retries: int = 2
    retry_delay: int = 5  # Увеличено
    
    # Ограничения
    max_records: int = 0  # 0 = все записи
    
    # ВАЖНО: ЗАМЕНИТЕ ЭТОТ ТОКЕН НА НОВЫЙ!
    auth_token: str = "eyJhbGciOiJFZERTQSJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzcwMjk3ODA3LCJpYXQiOjE3NzAyNjkwMDd9.--K03QrNpehr2-0opkxE_63AJSErHdE1g2BMinuQlNFTtSJg058RhXKgSDcJ-nl3Wb_xJTMCURPFo5J0z8bKAw"  # ЗАМЕНИТЕ!
    
    # URL API
    base_url: str = "https://pub.fsa.gov.ru"
    company_api: str = "/api/v1/ral/common/companies/{id}"
    declaration_api: str = "/api/v1/oa/accreditation/declaration/view/"
    
    # Заголовки
    headers: Dict[str, str] = field(default_factory=lambda: {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "authorization": "",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "referer": "https://pub.fsa.gov.ru/ral",
        "origin": "https://pub.fsa.gov.ru",
        "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
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
    
    logging.getLogger('charset_normalizer').setLevel(logging.WARNING)
    
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
        # Убираем лишние пробелы и переносы строк
        text = str(value).strip()
        # Заменяем множественные пробелы на один
        import re
        text = re.sub(r'\s+', ' ', text)
        # Убираем HTML-теги
        text = re.sub(r'<[^>]+>', '', text)
        return text

def safe_get(data: Dict, *keys, default: Any = "") -> Any:
    """Безопасное получение значения из словаря"""
    if not data:
        return default
    
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
    
    source = str(source).strip()
    
    # Если это URL
    if 'pub.fsa.gov.ru' in source:
        parts = source.strip('/').split('/')
        for i, part in enumerate(parts):
            if part == 'view' and i + 1 < len(parts):
                return parts[i + 1]
        return parts[-1] if parts else source
    
    # Убираем все нецифровые символы
    import re
    match = re.search(r'\d+', source)
    return match.group() if match else source

def generate_md5(text: str) -> str:
    """Генерация MD5 хеша для строки"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def format_phone(phone: str) -> str:
    """Форматирование телефона"""
    if not phone:
        return ""
    
    # Убираем все нецифровые символы
    import re
    digits = re.sub(r'\D', '', phone)
    
    if len(digits) == 10:
        return f"+7{digits}"
    elif len(digits) == 11:
        if digits.startswith('8'):
            return f"+7{digits[1:]}"
        elif digits.startswith('7'):
            return f"+{digits}"
    return phone

# ================= ОБРАБОТКА ДАННЫХ =================
class DataProcessor:
    """Обработчик данных компании"""
    
    @staticmethod
    def extract_company_info(company_data: Dict) -> Dict[str, Any]:
        """Извлечение информации о компании"""
        result = {}
        
        if not company_data or company_data.get('_status'):
            return result
        
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
        result['окогу'] = clean_value(safe_get(company_data, 'okogu'))
        result['окфс'] = clean_value(safe_get(company_data, 'okfs'))
        
        # Тип организации
        result['тип_заявителя'] = clean_value(safe_get(company_data, 'applicantType'))
        result['организационно_правовая_форма'] = clean_value(safe_get(company_data, 'legalForm'))
        result['государственное_предприятие'] = clean_value(safe_get(company_data, 'isStateOwned', False))
        result['иностранная_организация'] = clean_value(safe_get(company_data, 'isForeign', False))
        
        # Контактные данные
        phone = safe_get(company_data, 'phone')
        result['телефон'] = format_phone(phone)
        result['email'] = clean_value(safe_get(company_data, 'email'))
        result['сайт'] = clean_value(safe_get(company_data, 'website'))
        
        # Адреса
        address_data = safe_get(company_data, 'address', default={})
        if isinstance(address_data, dict):
            result['адрес_места_нахождения'] = clean_value(safe_get(address_data, 'fullAddress'))
            result['адрес_почтовый'] = clean_value(safe_get(address_data, 'postalAddress'))
            
            # Извлекаем компоненты адреса
            result['индекс'] = clean_value(safe_get(address_data, 'postalCode'))
            result['регион'] = clean_value(safe_get(address_data, 'region'))
            result['город'] = clean_value(safe_get(address_data, 'city'))
            result['улица'] = clean_value(safe_get(address_data, 'street'))
            result['дом'] = clean_value(safe_get(address_data, 'house'))
            result['квартира_офис'] = clean_value(safe_get(address_data, 'apartment'))
        else:
            result['адрес_места_нахождения'] = clean_value(address_data)
        
        # Руководитель
        director_data = safe_get(company_data, 'director', default={})
        if isinstance(director_data, dict):
            result['фио_руководителя'] = clean_value(safe_get(director_data, 'fullName'))
            result['должность_руководителя'] = clean_value(safe_get(director_data, 'position'))
            
            director_phone = safe_get(director_data, 'phone')
            result['телефон_руководителя'] = format_phone(director_phone)
            
            result['email_руководителя'] = clean_value(safe_get(director_data, 'email'))
        else:
            # Пробуем найти руководителя в других полях
            result['фио_руководителя'] = clean_value(safe_get(company_data, 'headName'))
            result['должность_руководителя'] = clean_value(safe_get(company_data, 'headPosition'))
        
        # Налоговые данные
        tax_data = safe_get(company_data, 'taxAuthority', default={})
        if isinstance(tax_data, dict):
            result['налоговый_орган'] = clean_value(safe_get(tax_data, 'name'))
            result['код_налогового_органа'] = clean_value(safe_get(tax_data, 'code'))
        else:
            result['налоговый_орган'] = clean_value(tax_data)
        
        result['дата_постановки_на_учет'] = clean_value(safe_get(company_data, 'registrationDate'))
        
        # Дополнительные поля
        result['основной_вид_деятельности'] = clean_value(safe_get(company_data, 'mainActivity'))
        result['дополнительные_виды_деятельности'] = clean_value(safe_get(company_data, 'additionalActivities'))
        
        # Метаданные
        result['дата_парсинга'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Генерируем хеш на основе основных данных
        hash_data = f"{result.get('инн', '')}{result.get('огрн', '')}{result.get('наименование', '')}"
        result['хеш_данных'] = generate_md5(hash_data) if hash_data else ""
        
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
            result['причина_прекращения'] = clean_value(safe_get(accreditation, 'terminationReason'))
            result['дата_прекращения'] = clean_value(safe_get(accreditation, 'terminationDate'))
            
            # ID декларации
            result['id_декларации'] = clean_value(safe_get(accreditation, 'idAccredScopeFile'))
        
        # Данные из декларации
        if decl_data and isinstance(decl_data, dict) and decl_data.get('_status') not in ['NOT_FOUND', 'SERVER_ERROR', 'ERROR']:
            result['дата_внесения_в_реестр'] = clean_value(safe_get(decl_data, 'registrationDate'))
            result['номер_реестровой_записи'] = clean_value(safe_get(decl_data, 'registryNumber'))
            result['включен_в_национальную_часть'] = clean_value(safe_get(decl_data, 'inNationalRegistry', False))
            result['наименование_стандарта'] = clean_value(safe_get(decl_data, 'standard', 'name'))
            
            # Секции декларации
            sections = safe_get(decl_data, 'sections', default=[])
            if sections:
                section_texts = []
                for section in sections[:3]:  # Берем первые 3 секции
                    section_name = safe_get(section, 'name')
                    if section_name:
                        section_texts.append(section_name)
                result['разделы_декларации'] = clean_value(" | ".join(section_texts))
            
            # Область аккредитации
            scope_data = safe_get(decl_data, 'accreditationScope', default=[])
            if isinstance(scope_data, list) and scope_data:
                scope_texts = []
                for idx, item in enumerate(scope_data[:10], 1):  # Берем первые 10
                    if isinstance(item, dict):
                        # Пробуем разные поля для описания
                        desc = (
                            safe_get(item, 'description') or 
                            safe_get(item, 'name') or 
                            safe_get(item, 'code') or
                            safe_get(item, 'scope')
                        )
                        if desc:
                            scope_texts.append(f"{idx}. {desc}")
                
                if scope_texts:
                    result['область_аккредитации'] = clean_value("\n".join(scope_texts))
                else:
                    # Пробуем получить из другого места
                    scope_text = safe_get(decl_data, 'scopeDescription')
                    if scope_text:
                        result['область_аккредитации'] = clean_value(scope_text)
        
        return result
    
    @staticmethod
    def process_company(company_id: str, company_data: Dict, decl_data: Dict) -> Dict[str, Any]:
        """Полная обработка данных компании"""
        result = {
            'id_компании': company_id,
            'статус_обработки': 'УСПЕШНО',
            'ошибки_декларации': 'Нет',
            'источник_данных': 'API ФСА'
        }
        
        try:
            # Извлекаем информацию о компании
            company_info = DataProcessor.extract_company_info(company_data)
            
            # Проверяем, есть ли хоть какие-то данные
            if not company_info.get('наименование') and not company_info.get('инн'):
                result['статус_обработки'] = 'ОШИБКА: Нет данных компании'
                result['ошибки_декларации'] = 'Нет данных компании'
                return result
            
            result.update(company_info)
            
            # Извлекаем информацию об аккредитации
            accreditation_info = DataProcessor.extract_accreditation_info(company_data, decl_data)
            result.update(accreditation_info)
            
            # Проверяем наличие ошибок декларации
            if decl_data and decl_data.get('_status') == 'SERVER_ERROR':
                result['ошибки_декларации'] = 'Ошибка сервера при получении декларации'
                result['статус_обработки'] = 'ДАННЫЕ КОМПАНИИ ПОЛНЫЕ, ДЕКЛАРАЦИЯ ОТСУТСТВУЕТ'
            elif decl_data and decl_data.get('_status') == 'NOT_FOUND':
                result['ошибки_декларации'] = 'Декларация не найдена'
                result['статус_обработки'] = 'ДАННЫЕ КОМПАНИИ ПОЛНЫЕ, ДЕКЛАРАЦИЯ НЕ НАЙДЕНА'
            elif decl_data and decl_data.get('_status') == 'ERROR':
                result['ошибки_декларации'] = f"Ошибка: {decl_data.get('error', 'Неизвестная ошибка')}"
                result['статус_обработки'] = 'ДАННЫЕ КОМПАНИИ ПОЛНЫЕ, ОШИБКА ДЕКЛАРАЦИИ'
            
        except Exception as e:
            error_msg = str(e)[:200]
            result['статус_обработки'] = f'ОШИБКА ОБРАБОТКИ: {error_msg}'
            result['ошибки_декларации'] = f'Ошибка обработки данных: {error_msg}'
            logger.error(f"Ошибка обработки компании {company_id}: {e}")
            logger.error(traceback.format_exc())
        
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
        for attempt in range(CONFIG.max_retries + 1):
            try:
                # Случайная задержка между запросами (0.5-2 секунды)
                if attempt > 0:
                    delay = CONFIG.retry_delay * (2 ** attempt) + random.uniform(0, 1)
                    await asyncio.sleep(min(delay, 10))  # Максимум 10 секунд
                
                async with session.get(
                    url, 
                    params=params, 
                    timeout=self.timeout,
                    ssl=False
                ) as response:
                    
                    # Логируем статус для отладки
                    if response.status != 200:
                        logger.debug(f"Запрос {url} вернул статус {response.status} (попытка {attempt+1})")
                    
                    if response.status == 200:
                        content_type = response.headers.get('Content-Type', '')
                        if 'application/json' in content_type:
                            data = await response.json()
                            # Проверяем, что данные не пустые
                            if data:
                                return data
                            else:
                                logger.warning(f"Пустой JSON ответ от {url}")
                                return {"_status": "EMPTY_RESPONSE"}
                        else:
                            text = await response.text()
                            logger.warning(f"Не JSON ответ от {url}: {text[:200]}")
                            return {"_status": "NOT_JSON", "text": text[:200]}
                    
                    elif response.status == 401:
                        logger.error(f"Ошибка авторизации (401) для {url}")
                        return {"_status": "UNAUTHORIZED", "status_code": 401}
                    
                    elif response.status == 404:
                        logger.debug(f"Ресурс не найден (404) для {url}")
                        return {"_status": "NOT_FOUND", "status_code": 404}
                    
                    elif response.status == 429:
                        logger.warning(f"Слишком много запросов (429) для {url}. Пауза 10 секунд...")
                        await asyncio.sleep(10)
                        continue  # Повторяем попытку
                    
                    elif response.status == 500:
                        logger.warning(f"Ошибка сервера (500) для {url} (попытка {attempt+1})")
                        if attempt == CONFIG.max_retries:
                            return {"_status": "SERVER_ERROR", "status_code": 500}
                        continue  # Повторяем попытку
                    
                    else:
                        logger.warning(f"Неожиданный статус {response.status} для {url}")
                        return {"_status": f"HTTP_{response.status}", "status_code": response.status}
                        
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут запроса: {url} (попытка {attempt+1})")
                if attempt == CONFIG.max_retries:
                    return {"_status": "TIMEOUT"}
                
            except aiohttp.ClientError as e:
                logger.warning(f"Ошибка клиента для {url}: {e} (попытка {attempt+1})")
                if attempt == CONFIG.max_retries:
                    return {"_status": "CLIENT_ERROR", "error": str(e)}
                
            except Exception as e:
                logger.error(f"Неожиданная ошибка для {url}: {e}")
                return {"_status": "UNKNOWN_ERROR", "error": str(e)}
        
        return {"_status": "MAX_RETRIES_EXCEEDED"}
    
    async def get_company(self, session: aiohttp.ClientSession, company_id: str) -> Dict:
        """Получение данных компании"""
        url = f"{self.base_url}{CONFIG.company_api.format(id=company_id)}"
        
        # Добавляем случайную задержку между запросами
        await asyncio.sleep(random.uniform(0.5, 1.5))
        
        return await self.make_request(session, url)
    
    async def get_declaration(self, session: aiohttp.ClientSession, doc_id: str) -> Dict:
        """Получение данных декларации"""
        if not doc_id:
            return {"_status": "NO_DOC_ID"}
        
        # Добавляем случайную задержку между запросами
        await asyncio.sleep(random.uniform(0.5, 2.0))
        
        url = f"{self.base_url}{CONFIG.declaration_api}"
        
        # Пробуем разные комбинации параметров
        params_variants = [
            {"docId": doc_id, "alType": 5, "validate": "false"},
            {"docId": doc_id, "alType": 1, "validate": "false"},
            {"docId": doc_id, "validate": "false"},
            {"docId": doc_id}
        ]
        
        for params in params_variants:
            result = await self.make_request(session, url, params)
            
            # Если получили валидные данные, возвращаем
            if result and not result.get('_status'):
                return result
            
            # Если ошибка авторизации или не найдено - не пробуем дальше
            if result.get('_status') in ['UNAUTHORIZED', 'NOT_FOUND']:
                return result
            
            # Для других ошибок пробуем следующий вариант
            await asyncio.sleep(1)
        
        return {"_status": "ALL_VARIANTS_FAILED"}

# ================= МЕНЕДЖЕР ДАННЫХ =================
class DataManager:
    """Управление данными и файлами"""
    
    def __init__(self):
        self.output_file = Path(CONFIG.output_file)
        self.all_data = []
        self.processed_ids = set()
        self.duplicate_count = 0
        
    def add_data(self, data: Dict):
        """Добавление данных"""
        company_id = data.get('id_компании')
        
        # Проверяем по хешу данных
        data_hash = data.get('хеш_данных')
        
        if company_id:
            if company_id not in self.processed_ids:
                self.all_data.append(data)
                self.processed_ids.add(company_id)
                
                # Автосохранение
                if len(self.all_data) % CONFIG.batch_size == 0:
                    self.save_to_excel()
                    logger.info(f"Автосохранение: {len(self.all_data)} записей")
            else:
                self.duplicate_count += 1
                logger.debug(f"Дубликат ID {company_id} пропущен")
    
    def save_to_excel(self) -> bool:
        """Сохранение данных в Excel"""
        if not self.all_data:
            logger.warning("Нет данных для сохранения")
            return False
        
        try:
            df = pd.DataFrame(self.all_data)
            
            # Определяем порядок столбцов (приоритетные сначала)
            priority_columns = [
                'id_компании', 'статус', 'наименование', 'сокращенное_наименование',
                'инн', 'кпп', 'огрн', 'окпо', 'окогу', 'окфс',
                'тип_заявителя', 'организационно_правовая_форма',
                'уникальный_номер_записи', 'статус_аккредитации', 
                'дата_аккредитации', 'срок_действия', 'номер_реестровой_записи',
                'телефон', 'email', 'сайт', 'адрес_места_нахождения',
                'адрес_почтовый', 'индекс', 'регион', 'город', 'улица', 'дом',
                'фио_руководителя', 'должность_руководителя', 'телефон_руководителя',
                'дата_внесения_в_реестр', 'включен_в_национальную_часть',
                'область_аккредитации', 'наименование_стандарта',
                'государственное_предприятие', 'иностранная_организация',
                'налоговый_орган', 'код_налогового_органа',
                'дата_постановки_на_учет', 'основной_вид_деятельности',
                'статус_обработки', 'ошибки_декларации', 'источник_данных',
                'дата_парсинга', 'хеш_данных'
            ]
            
            # Упорядочиваем столбцы
            existing_columns = list(df.columns)
            ordered_columns = []
            
            # Добавляем приоритетные колонки, которые существуют
            for col in priority_columns:
                if col in existing_columns:
                    ordered_columns.append(col)
                    existing_columns.remove(col)
            
            # Добавляем оставшиеся колонки в алфавитном порядке
            ordered_columns.extend(sorted(existing_columns))
            
            # Переупорядочиваем DataFrame
            df = df[ordered_columns]
            
            # Сохраняем в Excel
            with pd.ExcelWriter(self.output_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Реестр ФСА', index=False)
                
                # Настраиваем ширину колонок
                worksheet = writer.sheets['Реестр ФСА']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # Резервная копия в CSV
            csv_file = self.output_file.with_suffix('.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig', sep=';')
            
            # Резервная копия в JSON
            json_file = self.output_file.with_suffix('.json')
            with open(json_file, 'w', encoding='utf-8') as f:
                # Фильтруем сложные объекты для JSON
                json_data = []
                for item in self.all_data:
                    simple_item = {}
                    for key, value in item.items():
                        if isinstance(value, (str, int, float, bool, type(None))):
                            simple_item[key] = value
                        else:
                            simple_item[key] = str(value)
                    json_data.append(simple_item)
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"✓ Сохранено {len(df)} записей в {self.output_file}")
            logger.info(f"✓ CSV резервная копия: {csv_file}")
            logger.info(f"✓ JSON резервная копия: {json_file}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения Excel: {e}")
            logger.error(traceback.format_exc())
            
            # Экстренное сохранение в простой CSV
            try:
                emergency_file = self.output_file.with_name(f"emergency_{self.output_file.name}.csv")
                with open(emergency_file, 'w', encoding='utf-8') as f:
                    # Пишем заголовки
                    if self.all_data:
                        headers = self.all_data[0].keys()
                        f.write(';'.join(headers) + '\n')
                        
                        # Пишем данные
                        for item in self.all_data:
                            row = []
                            for header in headers:
                                value = item.get(header, '')
                                # Экранируем точку с запятой
                                value_str = str(value).replace(';', ',').replace('\n', ' ')
                                row.append(value_str)
                            f.write(';'.join(row) + '\n')
                
                logger.info(f"Экстренное сохранение в CSV: {emergency_file}")
            except Exception as e2:
                logger.error(f"Не удалось сохранить даже в CSV: {e2}")
            
            return False
    
    def get_stats(self) -> Dict[str, Any]:
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
            "duplicates": self.duplicate_count,
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
        self.total_unauthorized = 0
        
    def load_company_ids(self) -> List[str]:
        """Загрузка ID компаний из файлов"""
        possible_files = [
            "company_ids.txt",
            "links.txt",
            "ids.txt",
            "input.txt",
            "список.txt",
            "company_ids_full.txt"
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
                        else:
                            # Пробуем извлечь ID из URL
                            if 'pub.fsa.gov.ru' in line:
                                extracted = extract_company_id(line)
                                if extracted and extracted.isdigit():
                                    all_ids.append(extracted)
                    
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
            with open("company_ids.txt", "w", encoding="utf-8") as f:
                for id_ in unique_ids:
                    f.write(f"{id_}\n")
        
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
            
            # Проверяем ошибки авторизации
            if company_data.get('_status') == 'UNAUTHORIZED':
                self.total_unauthorized += 1
                logger.error(f"[{idx}] ОШИБКА АВТОРИЗАЦИИ для компании {company_id}. Проверьте токен!")
                return {
                    'id_компании': company_id,
                    'статус': 'ОШИБКА АВТОРИЗАЦИИ',
                    'статус_обработки': 'ОШИБКА_АВТОРИЗАЦИИ',
                    'ошибки_декларации': 'Неверный или просроченный токен авторизации',
                    'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # Проверяем другие ошибки
            if not company_data or company_data.get('_status') in ['NOT_FOUND', 'SERVER_ERROR', 'TIMEOUT']:
                self.total_failed += 1
                error_status = company_data.get('_status', 'UNKNOWN')
                logger.warning(f"[{idx}] Компания {company_id} не найдена или ошибка: {error_status}")
                return {
                    'id_компании': company_id,
                    'статус': f'Не найдено или ошибка ({error_status})',
                    'статус_обработки': 'ОШИБКА_API',
                    'ошибки_декларации': f'Ошибка API: {error_status}',
                    'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            
            # 2. Пытаемся получить декларацию
            decl_data = {"_status": "NO_DOC_ID"}
            accreditation = safe_get(company_data, 'accreditation', default={})
            
            if isinstance(accreditation, dict):
                doc_id = safe_get(accreditation, 'idAccredScopeFile')
                if doc_id and doc_id != "0":
                    try:
                        decl_data = await self.api_client.get_declaration(session, doc_id)
                    except Exception as e:
                        logger.warning(f"[{idx}] Ошибка при запросе декларации: {e}")
                        decl_data = {"_status": "REQUEST_ERROR", "error": str(e)}
            
            # 3. Обрабатываем данные
            result = self.data_processor.process_company(company_id, company_data, decl_data)
            
            # 4. Обновляем статистику
            self.total_processed += 1
            
            if result.get('статус_обработки', '').startswith('УСПЕШНО'):
                self.total_success += 1
                status_icon = "✅"
            elif result.get('статус_обработки', '').startswith('ДАННЫЕ КОМПАНИИ'):
                self.total_success += 1  # Считаем успехом, если есть данные компании
                status_icon = "⚠️"
                self.total_server_errors += 1
            else:
                self.total_failed += 1
                status_icon = "❌"
            
            # 5. Логируем результат
            company_name = result.get('наименование', company_id)
            logger.info(f"[{idx}] {status_icon} {company_name[:60]}...")
            
            # Логируем детали при ошибках
            if status_icon == "❌":
                logger.debug(f"  Ошибка: {result.get('статус_обработки', 'Неизвестно')}")
            
            return result
            
        except Exception as e:
            self.total_failed += 1
            logger.error(f"[{idx}] Критическая ошибка обработки {company_id}: {e}")
            logger.error(traceback.format_exc())
            
            return {
                'id_компании': company_id,
                'статус': f'Критическая ошибка: {str(e)[:100]}',
                'статус_обработки': 'КРИТИЧЕСКАЯ_ОШИБКА',
                'ошибки_декларации': f'Исключение: {type(e).__name__}',
                'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
    
    async def run(self):
        """Основной запуск парсера"""
        logger.info("=" * 70)
        logger.info("🚀 ЗАПУСК ПАРСЕРА РЕЕСТРА ФСА (ИСПРАВЛЕННАЯ ВЕРСИЯ)")
        logger.info(f"📁 Выходной файл: {CONFIG.output_file}")
        logger.info(f"🧵 Конкурентность: {CONFIG.concurrency}")
        logger.info(f"🔑 Токен: {'УСТАНОВЛЕН' if CONFIG.auth_token else 'ОТСУТСТВУЕТ'}")
        logger.info("=" * 70)
        
        start_time = time.time()
        
        # Проверка токена
        if not CONFIG.auth_token or CONFIG.auth_token == "Bearer ВАШ_НОВЫЙ_ТОКЕН_ЗДЕСЬ":
            logger.error("❌ ТОКЕН НЕ НАСТРОЕН! Замените токен в классе Config.")
            logger.error("Инструкция по получению токена:")
            logger.error("1. Откройте https://pub.fsa.gov.ru/ral в браузере")
            logger.error("2. Нажмите F12 -> вкладка Network")
            logger.error("3. Обновите страницу (F5)")
            logger.error("4. Найдите любой запрос к API (/api/)")
            logger.error("5. Скопируйте заголовок Authorization")
            logger.error("6. Вставьте в переменную auth_token в классе Config")
            return
        
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
            ssl=False,
            force_close=True
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
                    # Добавляем случайную задержку между задачами
                    await asyncio.sleep(random.uniform(0.1, 0.5))
                    return await self.process_single_company(session, company_id, idx, total)
            
            # Создаем задачи
            tasks = []
            for idx, company_id in enumerate(company_ids, 1):
                tasks.append(process_with_limit(company_id, idx))
            
            # Обрабатываем задачи
            completed = 0
            last_log_time = time.time()
            last_save_time = time.time()
            
            for future in asyncio.as_completed(tasks):
                try:
                    result = await future
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
                            f"Скорость: {speed:.2f}/сек | "
                            f"Успешно: {self.total_success} | "
                            f"Ошибки авторизации: {self.total_unauthorized} | "
                            f"Ошибки деклараций: {self.total_server_errors} | "
                            f"Сбоев: {self.total_failed} | "
                            f"Осталось: ~{eta/60:.1f} мин"
                        )
                        
                        last_log_time = current_time
                    
                    # Автосохранение каждые 5 минут
                    if current_time - last_save_time > 300:  # 5 минут
                        self.data_manager.save_to_excel()
                        last_save_time = current_time
                        logger.info("💾 Автосохранение по таймеру")
                    
                except Exception as e:
                    logger.error(f"Ошибка в основной задаче: {e}")
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
        logger.info(f"🔐 Ошибки авторизации: {self.total_unauthorized}")
        logger.info(f"⚠️  Ошибки деклараций: {self.total_server_errors}")
        logger.info(f"❌ Сбоев: {self.total_failed}")
        logger.info(f"⏱️  Общее время: {total_time/60:.1f} минут")
        logger.info(f"🚀 Средняя скорость: {self.total_processed/total_time:.2f} записей/сек")
        logger.info(f"💾 Основной файл: {CONFIG.output_file}")
        logger.info(f"📄 CSV резервный: {CONFIG.output_file.replace('.xlsx', '.csv')}")
        logger.info(f"📋 JSON резервный: {CONFIG.output_file.replace('.xlsx', '.json')}")
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
ВЕРСИЯ ПАРСЕРА: Исправленная версия 4.0

СТАТИСТИКА:
- Всего записей для обработки: {self.total_processed + self.total_failed}
- Успешно обработано: {self.total_success}
- Записей с ошибками авторизации: {self.total_unauthorized}
- Записей с ошибками деклараций: {self.total_server_errors}
- Записей со сбоями: {self.total_failed}
- Процент успеха: {(self.total_success/self.total_processed*100):.1f}% (если > 0)

ВРЕМЯ ВЫПОЛНЕНИЯ:
- Общее время: {total_time/60:.1f} минут
- Средняя скорость: {self.total_processed/total_time:.2f} записей/сек

ВЫХОДНЫЕ ФАЙЛЫ:
1. {CONFIG.output_file} - основной файл Excel
2. {CONFIG.output_file.replace('.xlsx', '.csv')} - резервный файл CSV
3. {CONFIG.output_file.replace('.xlsx', '.json')} - резервный файл JSON
4. {CONFIG.log_file} - файл логов

СТАТУСЫ ОБРАБОТКИ:
"""
        
        if 'status_stats' in stats:
            for status, count in stats['status_stats'].items():
                report += f"- {status}: {count}\n"
        
        report += f"""
ПРИМЕЧАНИЯ:
1. Ошибки авторизации - необходимо обновить токен в коде
2. Ошибки HTTP 500 - это проблемы на стороне сервера ФСА
3. Данные компаний сохраняются даже при ошибках деклараций
4. Для повторного запуска удалите файл {CONFIG.output_file}
5. Лог содержит подробную информацию об ошибках

ИНСТРУКЦИЯ ПО ОБНОВЛЕНИЮ ТОКЕНА:
1. Откройте https://pub.fsa.gov.ru/ral в браузере
2. Нажмите F12 -> вкладка Network
3. Обновите страницу (F5)
4. Найдите любой запрос к API (фильтр: /api/)
5. Скопируйте заголовок Authorization
6. Вставьте в переменную auth_token в классе Config
7. Запустите: python fsa_parser_fixed.py check
"""
        
        report_file = "отчет_о_парсинге.txt"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"📄 Отчет сохранен в: {report_file}")

# ================= КОМАНДЫ И ЗАПУСК =================
def print_banner():
    """Вывод баннера"""
    banner = """
╔══════════════════════════════════════════════════════╗
║           ПАРСЕР РЕЕСТРА ФСА v4.0 (ИСПРАВЛЕННЫЙ)    ║
║     Сбор данных с pub.fsa.gov.ru/ral                ║
║     Требуется обновление токена!                    ║
╚══════════════════════════════════════════════════════╝
"""
    print(banner)

def check_environment():
    """Проверка окружения"""
    print("🔍 Проверка окружения...")
    
    # Проверка Python
    print(f"  Python: {sys.version.split()[0]}")
    
    # Проверка библиотек
    required = ['aiohttp', 'pandas', 'openpyxl']
    missing = []
    for lib in required:
        try:
            __import__(lib)
            print(f"  ✅ {lib}")
        except ImportError:
            print(f"  ❌ {lib} - требуется установка")
            missing.append(lib)
    
    if missing:
        print(f"\n📦 Установите недостающие библиотеки:")
        print(f"  pip install {' '.join(missing)}")
        return False
    
    # Проверка токена
    if CONFIG.auth_token == "Bearer ВАШ_НОВЫЙ_ТОКЕН_ЗДЕСЬ":
        print("  ❌ Токен не настроен! Замените токен в коде.")
        return False
    
    return True

async def check_api_and_token():
    """Проверка API и токена"""
    print("🔍 Проверка API и токена...")
    
    # Проверка в синхронном режиме для простоты
    import requests
    
    test_url = f"{CONFIG.base_url}/api/v1/ral/common/companies/50"  # ID 50 для теста
    headers = CONFIG.headers.copy()
    
    print(f"  URL: {test_url}")
    print(f"  Токен: {CONFIG.auth_token[:50]}...")
    
    try:
        response = requests.get(test_url, headers=headers, timeout=30, verify=False)
        print(f"  Статус API: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ API доступен и токен действителен")
            print(f"Пример данных для компании ID 50:")
            print(f"  Название: {data.get('fullName', 'Нет данных')}")
            print(f"  ИНН: {data.get('inn', 'Нет данных')}")
            print(f"  КПП: {data.get('kpp', 'Нет данных')}")
            print(f"  ОГРН: {data.get('ogrn', 'Нет данных')}")
            print(f"  Статус: {data.get('status', 'Нет данных')}")
            
            # Проверяем наличие адреса
            address = data.get('address', {})
            if address:
                print(f"  Адрес: {address.get('fullAddress', 'Нет данных')}")
            
            # Сохраняем пример
            with open("пример_ответа_api.json", "w", encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print("✓ Пример сохранен в 'пример_ответа_api.json'")
            
            return True
            
        elif response.status_code == 401:
            print("❌ Ошибка авторизации. Токен недействителен.")
            print("Обновите токен в файле CONFIG.auth_token")
            return False
            
        elif response.status_code == 500:
            print("⚠️  Сервер вернул 500 ошибку. Проблемы на стороне ФСА.")
            return False
            
        else:
            print(f"⚠️  Неожиданный статус: {response.status_code}")
            print(f"  Ответ: {response.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return False

async def main():
    """Основная функция"""
    print_banner()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
    else:
        command = "help"
    
    if command == "run":
        print("🚀 Запуск полного парсинга...")
        if not check_environment():
            return
        
        # Проверяем токен
        if not await check_api_and_token():
            print("\n❌ Не удалось проверить токен. Исправьте токен и попробуйте снова.")
            return
        
        parser = FSAParser()
        await parser.run()
        
    elif command == "test":
        print("🧪 Тестовый запуск (50 записей)...")
        CONFIG.max_records = 50
        CONFIG.output_file = "тест_реестр_исправленный.xlsx"
        
        if not check_environment():
            return
        
        # Проверяем токен
        if not await check_api_and_token():
            print("\n❌ Не удалось проверить токен. Исправьте токен и попробуйте снова.")
            return
        
        # Создаем тестовый файл если его нет
        if not Path("company_ids.txt").exists():
            with open("company_ids.txt", "w", encoding="utf-8") as f:
                for i in range(1, 51):
                    f.write(f"{i}\n")
            print("✓ Создан файл company_ids.txt с 50 ID")
        
        parser = FSAParser()
        await parser.run()
        
    elif command == "check":
        print("🔍 Проверка API и токена...")
        await check_api_and_token()
        
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
  python fsa_parser_fixed.py run      - Полный запуск парсинга
  python fsa_parser_fixed.py test     - Тестовый запуск (50 записей)
  python fsa_parser_fixed.py check    - Проверка API и токена
  python fsa_parser_fixed.py generate - Создать файл с 38000 ID
  python fsa_parser_fixed.py help     - Эта справка

ВАЖНО! Перед запуском:
  1. ОБНОВИТЕ ТОКЕН в классе Config (строка auth_token)
  2. Проверьте токен: python fsa_parser_fixed.py check
  3. Протестируйте: python fsa_parser_fixed.py test

Как получить токен:
  1. Откройте https://pub.fsa.gov.ru/ral в браузере
  2. Нажмите F12 -> вкладка Network
  3. Обновите страницу (F5)
  4. Найдите любой запрос к API (фильтр: /api/)
  5. Скопируйте заголовок Authorization
  6. Вставьте в переменную auth_token в классе Config

Файлы:
  - company_ids.txt - список ID для обработки
  - реестр_фса_полный.xlsx - результаты парсинга
  - fsa_parser_fixed.log - подробный лог
  - отчет_о_парсинге.txt - статистика

Пример обновления токена:
  В файле fsa_parser_fixed.py найдите строку:
      auth_token: str = "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
  Замените на свой токен.

Примечания:
  - При ошибках авторизации проверьте токен
  - При ошибках 500 - проблемы на сервере ФСА
  - Данные сохраняются каждые 100 записей
  - Есть резервные копии в CSV и JSON формате
""")
    
    else:
        print(f"❌ Неизвестная команда: {command}")
        print("Используйте: python fsa_parser_fixed.py help")

if __name__ == "__main__":
    try:
        # Настройка asyncio для Windows
        if sys.platform == 'win32':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Парсер остановлен пользователем")
        print("📁 Данные сохранены в текущем состоянии")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
        print("Подробности в лог-файле")
        traceback.print_exc()
