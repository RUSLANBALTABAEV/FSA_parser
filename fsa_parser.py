"""
ПАРСЕР РЕЕСТРА АККРЕДИТОВАННЫХ ЛИЦ ФСА
Автоматический сбор данных с pub.fsa.gov.ru/ral
Версия 2.0 - Полная обработка всех разделов
"""

import asyncio
import aiohttp
import pandas as pd
import json
import time
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import traceback
import aiofiles
import sys
import requests
from bs4 import BeautifulSoup

# ================= КОНФИГУРАЦИЯ =================
CONFIG = {
    "log_file": "fsa_parser_full.log",
    "output_excel": "реестр_фса_полный.xlsx",
    "output_csv": "реестр_фса_полный.csv",
    "output_json": "реестр_фса_полный.json",
    "batch_size": 500,          # Сохранять каждые 500 записей
    "concurrency": 10,          # Одновременных запросов
    "request_timeout": 45,      # Таймаут запроса
    "retry_attempts": 3,        # Попытки повтора
    "retry_delay": 3,           # Задержка между повторами
    "max_records": 38000,       # Максимум записей (0 = все)
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    
    # Токен (может потребоваться обновление)
    "auth_token": "Bearer eyJhbGciOiJFZERTQSJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzcwMDY5MTA4LCJpYXQiOjE3NzAwNDAzMDh9.NdwC9BJ-rOk16GOq5GX8T1FmY4rpZXA-pfZjuLT3JeCYaZDc_3sIchWivorKJi4TpAF2-hv9ph1SRD7SzcluBA",
}

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
    file_handler = logging.FileHandler(
        CONFIG["log_file"], 
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Консольный обработчик
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Добавляем обработчики
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ================= КЛАСС ДЛЯ РАБОТЫ С API =================
class FSAApiClient:
    """Клиент для работы с API ФСА"""
    
    def __init__(self):
        self.base_url = "https://pub.fsa.gov.ru"
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "authorization": CONFIG["auth_token"],
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "user-agent": CONFIG["user_agent"],
            "referer": f"{self.base_url}/ral",
            "origin": self.base_url,
        }
        self.session = None
        
    async def __aenter__(self):
        """Асинхронный вход в контекст"""
        timeout = aiohttp.ClientTimeout(total=CONFIG["request_timeout"])
        connector = aiohttp.TCPConnector(
            limit=CONFIG["concurrency"],
            limit_per_host=CONFIG["concurrency"],
            force_close=True,
            enable_cleanup_closed=True
        )
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            timeout=timeout,
            connector=connector,
            trust_env=True
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронный выход из контекста"""
        if self.session:
            await self.session.close()
    
    async def fetch_json(self, url: str, params: dict = None) -> Optional[Dict]:
        """Получение JSON с повторными попытками"""
        for attempt in range(CONFIG["retry_attempts"]):
            try:
                async with self.session.get(
                    url, 
                    params=params,
                    ssl=False
                ) as response:
                    
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 404:
                        logger.debug(f"404 для {url}")
                        return {"_status": "NOT_FOUND"}
                    elif response.status == 429:
                        wait = (attempt + 1) * 10
                        logger.warning(f"429 Слишком много запросов. Ждем {wait} сек.")
                        await asyncio.sleep(wait)
                        continue
                    elif response.status >= 500:
                        logger.warning(f"Ошибка сервера {response.status}")
                        await asyncio.sleep(CONFIG["retry_delay"])
                        continue
                    else:
                        logger.error(f"HTTP {response.status} для {url}")
                        if attempt < CONFIG["retry_attempts"] - 1:
                            await asyncio.sleep(CONFIG["retry_delay"])
                            continue
                        return {"_status": f"ERROR_{response.status}"}
                        
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут. Попытка {attempt + 1}/{CONFIG['retry_attempts']}")
                await asyncio.sleep(CONFIG["retry_delay"])
            except Exception as e:
                logger.error(f"Ошибка запроса: {str(e)[:100]}")
                if attempt < CONFIG["retry_attempts"] - 1:
                    await asyncio.sleep(CONFIG["retry_delay"])
                    continue
        
        logger.error(f"Не удалось выполнить запрос: {url}")
        return {"_status": "FAILED"}
    
    async def get_company_data(self, company_id: str) -> Optional[Dict]:
        """Получение данных компании"""
        url = f"{self.base_url}/api/v1/ral/common/companies/{company_id}"
        return await self.fetch_json(url)
    
    async def get_declaration_data(self, doc_id: str) -> Optional[Dict]:
        """Получение данных декларации"""
        url = f"{self.base_url}/api/v1/oa/accreditation/declaration/view/"
        params = {"docId": doc_id, "alType": 5, "validate": "false"}
        return await self.fetch_json(url, params)

# ================= КЛАСС ДЛЯ ИЗВЛЕЧЕНИЯ ДАННЫХ =================
class DataExtractor:
    """Извлечение и обработка данных из API"""
    
    @staticmethod
    def safe_get(data: Dict, path: str, default: Any = ""):
        """Безопасное получение значения по пути"""
        if not data or not isinstance(data, dict):
            return default
        
        parts = path.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
        
        return current if current is not None else default
    
    @staticmethod
    def clean_value(value: Any) -> str:
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
    
    @classmethod
    def extract_accredited_person(cls, company_data: Dict, decl_data: Dict) -> Dict[str, Any]:
        """Извлечение данных раздела 'Аккредитовано лицо'"""
        result = {}
        
        # Базовые данные
        result['статус'] = cls.clean_value(
            cls.safe_get(company_data, 'status') or 
            cls.safe_get(decl_data, 'status')
        )
        
        result['дата_внесения_в_реестр'] = cls.clean_value(
            cls.safe_get(company_data, 'registrationDate') or
            cls.safe_get(decl_data, 'registrationDate')
        )
        
        result['включен_в_национальную_часть'] = cls.clean_value(
            cls.safe_get(company_data, 'inNationalRegistry', False)
        )
        
        result['наименование_стандарта'] = cls.clean_value(
            cls.safe_get(decl_data, 'standard.name') or
            cls.safe_get(decl_data, 'accreditationStandard')
        )
        
        result['уникальный_номер_записи'] = cls.clean_value(
            cls.safe_get(company_data, 'accreditationNumber') or
            cls.safe_get(decl_data, 'accreditationNumber') or
            cls.safe_get(company_data, 'ralNumber')
        )
        
        result['наименование'] = cls.clean_value(
            cls.safe_get(company_data, 'fullName') or
            cls.safe_get(company_data, 'name') or
            cls.safe_get(decl_data, 'organizationName')
        )
        
        result['сокращенное_наименование'] = cls.clean_value(
            cls.safe_get(company_data, 'shortName') or
            cls.safe_get(company_data, 'abbreviation')
        )
        
        # Руководство
        result['фио_руководителя'] = cls.clean_value(
            cls.safe_get(company_data, 'director.fullName') or
            cls.safe_get(decl_data, 'headName')
        )
        
        result['должность_руководителя'] = cls.clean_value(
            cls.safe_get(company_data, 'director.position') or
            cls.safe_get(decl_data, 'headPosition')
        )
        
        result['телефон'] = cls.clean_value(
            cls.safe_get(company_data, 'phone') or
            cls.safe_get(company_data, 'contactPhone')
        )
        
        result['телефон_руководителя'] = cls.clean_value(
            cls.safe_get(company_data, 'director.phone') or
            cls.safe_get(decl_data, 'headPhone')
        )
        
        result['email'] = cls.clean_value(
            cls.safe_get(company_data, 'email') or
            cls.safe_get(company_data, 'contactEmail')
        )
        
        result['сайт'] = cls.clean_value(
            cls.safe_get(company_data, 'website') or
            cls.safe_get(decl_data, 'website')
        )
        
        # Адреса
        addresses = []
        for addr_path in ['address.fullAddress', 'legalAddress', 'activityAddress']:
            addr = cls.safe_get(company_data, addr_path) or cls.safe_get(decl_data, addr_path)
            if addr and addr not in addresses:
                addresses.append(addr)
        
        result['адрес_деятельности'] = cls.clean_value("; ".join(addresses))
        
        # Государственные услуги (из PDF)
        result['номер_гос_услуги'] = cls.clean_value(
            cls.safe_get(decl_data, 'stateServiceNumber')
        )
        
        result['дата_гос_услуги'] = cls.clean_value(
            cls.safe_get(decl_data, 'stateServiceDate')
        )
        
        result['номер_решения'] = cls.clean_value(
            cls.safe_get(decl_data, 'decisionNumber')
        )
        
        result['дата_решения'] = cls.clean_value(
            cls.safe_get(decl_data, 'decisionDate')
        )
        
        # Область аккредитации
        accreditation_scope = cls.safe_get(decl_data, 'accreditationScope', [])
        if isinstance(accreditation_scope, list):
            scope_texts = []
            for scope in accreditation_scope:
                if isinstance(scope, dict):
                    desc = scope.get('description') or scope.get('name') or str(scope)
                    if desc and desc not in scope_texts:
                        scope_texts.append(desc)
            result['описание_области_аккредитации'] = cls.clean_value(" | ".join(scope_texts))
        
        return result
    
    @classmethod
    def extract_applicant_data(cls, company_data: Dict) -> Dict[str, Any]:
        """Извлечение данных раздела 'Заявитель'"""
        result = {}
        
        # Тип и форма
        legal_form = cls.safe_get(company_data, 'legalForm')
        result['тип_заявителя'] = cls.clean_value(
            "Юридическое лицо" if legal_form in ['ООО', 'ЗАО', 'ОАО', 'АО', 'ПАО'] else 
            "Индивидуальный предприниматель" if legal_form in ['ИП'] else
            legal_form or "Юридическое лицо"
        )
        
        result['организационно_правовая_форма'] = cls.clean_value(legal_form)
        
        result['полное_наименование'] = cls.clean_value(
            cls.safe_get(company_data, 'fullName') or
            cls.safe_get(company_data, 'legalName')
        )
        
        result['сокращенное_наименование_заявителя'] = cls.clean_value(
            cls.safe_get(company_data, 'shortName') or
            cls.safe_get(company_data, 'abbreviation')
        )
        
        result['государственное_предприятие'] = cls.clean_value(
            cls.safe_get(company_data, 'isStateOwned', False)
        )
        
        result['иностранная_организация'] = cls.clean_value(
            cls.safe_get(company_data, 'isForeign', False)
        )
        
        # Реквизиты
        result['инн'] = cls.clean_value(cls.safe_get(company_data, 'inn'))
        result['кпп'] = cls.clean_value(cls.safe_get(company_data, 'kpp'))
        result['огрн'] = cls.clean_value(cls.safe_get(company_data, 'ogrn'))
        result['окпо'] = cls.clean_value(cls.safe_get(company_data, 'okpo'))
        result['окогу'] = cls.clean_value(cls.safe_get(company_data, 'okogu'))
        result['окфс'] = cls.clean_value(cls.safe_get(company_data, 'okfs'))
        
        # Адреса
        result['адрес_места_нахождения'] = cls.clean_value(
            cls.safe_get(company_data, 'legalAddress.fullAddress') or
            cls.safe_get(company_data, 'legalAddress')
        )
        
        result['адрес_почтовый'] = cls.clean_value(
            cls.safe_get(company_data, 'postalAddress')
        )
        
        # Налоговые данные
        result['налоговый_орган'] = cls.clean_value(
            cls.safe_get(company_data, 'taxAuthority.name') or
            cls.safe_get(company_data, 'taxAuthority')
        )
        
        result['дата_постановки_на_учет'] = cls.clean_value(
            cls.safe_get(company_data, 'registrationDate')
        )
        
        # Руководитель заявителя
        result['фио_руководителя_заявителя'] = cls.clean_value(
            cls.safe_get(company_data, 'director.fullName') or
            cls.safe_get(company_data, 'head.fullName') or
            cls.safe_get(company_data, 'generalDirector')
        )
        
        result['должность_руководителя_заявителя'] = cls.clean_value(
            cls.safe_get(company_data, 'director.position')
        )
        
        result['телефон_заявителя'] = cls.clean_value(
            cls.safe_get(company_data, 'contactPhone') or
            cls.safe_get(company_data, 'phone')
        )
        
        result['email_заявителя'] = cls.clean_value(
            cls.safe_get(company_data, 'contactEmail') or
            cls.safe_get(company_data, 'email')
        )
        
        # Дополнительные поля из PDF
        result['номер_постановления_актуализации'] = cls.clean_value(
            cls.safe_get(company_data, 'updateDecreeNumber')
        )
        
        result['дата_постановления_актуализации'] = cls.clean_value(
            cls.safe_get(company_data, 'updateDecreeDate')
        )
        
        return result
    
    @classmethod
    def extract_additional_data(cls, company_data: Dict, decl_data: Dict) -> Dict[str, Any]:
        """Извлечение дополнительных данных"""
        result = {}
        
        # Коды деятельности
        result['оквэд'] = cls.clean_value(cls.safe_get(company_data, 'okved'))
        result['окпд2'] = cls.clean_value(cls.safe_get(company_data, 'okpd2'))
        
        # Свидетельства
        result['номер_свидетельства'] = cls.clean_value(
            cls.safe_get(company_data, 'certificateNumber')
        )
        
        result['дата_выдачи_свидетельства'] = cls.clean_value(
            cls.safe_get(company_data, 'certificateIssueDate')
        )
        
        result['срок_действия_свидетельства'] = cls.clean_value(
            cls.safe_get(company_data, 'certificateValidUntil')
        )
        
        # Лабораторные данные
        result['тип_лаборатории'] = cls.clean_value(
            cls.safe_get(decl_data, 'laboratoryType')
        )
        
        result['область_аккредитации_коды'] = cls.clean_value(
            cls.safe_get(decl_data, 'accreditationCodes')
        )
        
        # Файлы и документы
        files = cls.safe_get(decl_data, 'files', [])
        if isinstance(files, list):
            file_list = []
            for file_item in files:
                if isinstance(file_item, dict):
                    name = file_item.get('name', '')
                    url = file_item.get('url', '')
                    if url:
                        if not url.startswith('http'):
                            url = f"https://pub.fsa.gov.ru{url}"
                        file_list.append(f"{name}: {url}")
            result['прикрепленные_файлы'] = cls.clean_value(" | ".join(file_list))
        
        return result
    
    @classmethod
    def process_company(cls, company_id: str, company_data: Dict, decl_data: Dict) -> Dict[str, Any]:
        """Полная обработка данных компании"""
        result = {
            'id_компании': company_id,
            'источник_данных': 'https://pub.fsa.gov.ru/ral',
            'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'статус_парсинга': 'УСПЕШНО'
        }
        
        try:
            # Извлекаем данные из всех разделов
            accredited_data = cls.extract_accredited_person(company_data, decl_data)
            applicant_data = cls.extract_applicant_data(company_data)
            additional_data = cls.extract_additional_data(company_data, decl_data)
            
            # Объединяем все данные
            result.update(accredited_data)
            result.update(applicant_data)
            result.update(additional_data)
            
            # Проверяем наличие аккредитации
            if not result.get('уникальный_номер_записи'):
                result['статус'] = 'Нет действующей аккредитации'
                result['статус_парсинга'] = 'НЕТ_АККРЕДИТАЦИИ'
            
            logger.debug(f"Обработана компания {company_id}: {result.get('наименование', 'Без названия')}")
            
        except Exception as e:
            result['статус_парсинга'] = f'ОШИБКА: {str(e)[:100]}'
            logger.error(f"Ошибка обработки компании {company_id}: {e}")
        
        return result

# ================= КЛАСС ДЛЯ УПРАВЛЕНИЯ ДАННЫМИ =================
class DataManager:
    """Управление загрузкой и сохранением данных"""
    
    @staticmethod
    def extract_company_id(source: str) -> str:
        """Извлечение ID компании из URL или строки"""
        if not source:
            return ""
        
        if 'pub.fsa.gov.ru' in source:
            # Обработка URL
            parts = source.strip('/').split('/')
            for i, part in enumerate(parts):
                if part == 'view' and i + 1 < len(parts):
                    return parts[i + 1]
            # Если не нашли через view, берем предпоследнюю часть
            return parts[-2] if len(parts) > 2 else ""
        
        # Если это уже ID
        return str(source).strip()
    
    @classmethod
    def load_company_ids(cls) -> List[str]:
        """Загрузка списка ID компаний из файлов"""
        possible_files = [
            "company_ids.txt",   # Только ID
            "links.txt",         # Ссылки
            "ids.txt",           # Простой список
            "input.txt",         # Общий файл
            "список.txt",        # Русское название
        ]
        
        all_ids = []
        
        for filename in possible_files:
            filepath = Path(filename)
            if filepath.exists():
                try:
                    logger.info(f"Загрузка ID из {filename}")
                    
                    content = filepath.read_text(encoding='utf-8').strip()
                    lines = [line.strip() for line in content.split('\n') if line.strip()]
                    
                    for line in lines:
                        # Пропускаем комментарии
                        if line.startswith('#') or line.startswith('//'):
                            continue
                        
                        company_id = cls.extract_company_id(line)
                        if company_id and company_id.isdigit():
                            all_ids.append(company_id)
                        elif line.isdigit():
                            all_ids.append(line)
                            
                    logger.info(f"  Загружено {len(lines)} строк из {filename}")
                    
                except Exception as e:
                    logger.error(f"Ошибка загрузки {filename}: {e}")
        
        # Удаляем дубликаты, сохраняя порядок
        seen = set()
        unique_ids = []
        for id_ in all_ids:
            if id_ not in seen:
                seen.add(id_)
                unique_ids.append(id_)
        
        logger.info(f"Всего уникальных ID: {len(unique_ids)}")
        
        # Если нет файлов, генерируем тестовые ID
        if not unique_ids:
            logger.warning("Файлы с ID не найдены. Генерация тестовых ID 1-1000")
            unique_ids = [str(i) for i in range(1, 1001)]
        
        # Ограничение количества записей
        if CONFIG["max_records"] > 0:
            unique_ids = unique_ids[:CONFIG["max_records"]]
            logger.info(f"Ограничение до {CONFIG['max_records']} записей")
        
        return unique_ids
    
    @staticmethod
    def save_results(data: List[Dict], filename: str, format_type: str = 'excel'):
        """Сохранение результатов в файл"""
        try:
            if not data:
                logger.warning("Нет данных для сохранения")
                return False
            
            df = pd.DataFrame(data)
            
            # Упорядочиваем столбцы
            priority_columns = [
                'id_компании', 'статус', 'уникальный_номер_записи',
                'наименование', 'сокращенное_наименование', 'инн',
                'тип_заявителя', 'организационно_правовая_форма',
                'дата_внесения_в_реестр', 'дата_парсинга'
            ]
            
            existing_columns = list(df.columns)
            ordered_columns = []
            
            # Сначала приоритетные
            for col in priority_columns:
                if col in existing_columns:
                    ordered_columns.append(col)
                    existing_columns.remove(col)
            
            # Затем остальные в алфавитном порядке
            ordered_columns.extend(sorted(existing_columns))
            df = df[ordered_columns]
            
            if format_type == 'excel':
                # Excel
                excel_file = filename if filename.endswith('.xlsx') else f"{filename}.xlsx"
                df.to_excel(excel_file, index=False, engine='openpyxl')
                logger.info(f"Сохранено {len(df)} записей в Excel: {excel_file}")
                
                # Дополнительно сохраняем в CSV
                csv_file = excel_file.replace('.xlsx', '.csv')
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                logger.info(f"Резервная копия в CSV: {csv_file}")
                
            elif format_type == 'csv':
                # Только CSV
                csv_file = filename if filename.endswith('.csv') else f"{filename}.csv"
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                logger.info(f"Сохранено {len(df)} записей в CSV: {csv_file}")
            
            elif format_type == 'json':
                # JSON
                json_file = filename if filename.endswith('.json') else f"{filename}.json"
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                logger.info(f"Сохранено {len(data)} записей в JSON: {json_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения файла {filename}: {e}")
            logger.error(traceback.format_exc())
            return False

# ================= КЛАСС ДЛЯ ПОИСКА ID В РЕЕСТРЕ =================
class IDFinder:
    """Поиск ID компаний в реестре"""
    
    @staticmethod
    def find_ids_from_api() -> List[str]:
        """Поиск ID через API (если доступно)"""
        logger.info("Попытка найти ID через API...")
        
        try:
            # Этот URL может потребовать настройки
            search_url = "https://pub.fsa.gov.ru/api/v1/ral/common/search"
            
            headers = {
                "authorization": CONFIG["auth_token"],
                "user-agent": CONFIG["user_agent"],
            }
            
            # Параметры поиска (может потребоваться настройка)
            params = {
                "page": 0,
                "size": 100,  # Количество на странице
                "sort": "id,asc"
            }
            
            response = requests.get(search_url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                ids = []
                
                # Извлекаем ID из ответа (структура может отличаться)
                if isinstance(data, dict):
                    content = data.get('content', [])
                    if isinstance(content, list):
                        for item in content:
                            if isinstance(item, dict):
                                company_id = item.get('id')
                                if company_id:
                                    ids.append(str(company_id))
                
                logger.info(f"Найдено {len(ids)} ID через API")
                return ids
            else:
                logger.warning(f"API поиска вернул {response.status_code}")
                
        except Exception as e:
            logger.error(f"Ошибка поиска через API: {e}")
        
        return []
    
    @staticmethod
    def generate_id_range(start: int = 1, end: int = 38000) -> List[str]:
        """Генерация диапазона ID"""
        logger.info(f"Генерация ID с {start} по {end}")
        return [str(i) for i in range(start, end + 1)]

# ================= ОСНОВНОЙ КЛАСС ПАРСЕРА =================
class FSAParser:
    """Основной класс парсера"""
    
    def __init__(self):
        self.data_manager = DataManager()
        self.data_extractor = DataExtractor()
        self.id_finder = IDFinder()
        self.processed_count = 0
        self.failed_count = 0
        self.start_time = None
        
    async def process_single_company(self, api_client: FSAApiClient, company_id: str) -> Optional[Dict]:
        """Обработка одной компании"""
        try:
            logger.debug(f"Запрос данных компании {company_id}")
            
            # Получаем данные компании
            company_data = await api_client.get_company_data(company_id)
            
            if not company_data or company_data.get('_status') in ['NOT_FOUND', 'FAILED']:
                logger.warning(f"Компания {company_id} не найдена")
                return {
                    'id_компании': company_id,
                    'статус': 'Не найдено в реестре',
                    'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'статус_парсинга': 'НЕ_НАЙДЕНО'
                }
            
            # Ищем данные декларации
            decl_data = {"_status": "NO_DATA"}
            accreditation = DataExtractor.safe_get(company_data, 'accreditation')
            
            if isinstance(accreditation, dict):
                doc_id = accreditation.get('idAccredScopeFile') or accreditation.get('id')
                
                if doc_id:
                    logger.debug(f"Запрос декларации {doc_id}")
                    decl_data = await api_client.get_declaration_data(doc_id)
                    
                    if decl_data.get('_status') == 'NOT_FOUND':
                        logger.debug(f"Декларация для {company_id} не найдена")
                    else:
                        logger.debug(f"Получена декларация для {company_id}")
            
            # Обрабатываем данные
            result = self.data_extractor.process_company(company_id, company_data, decl_data)
            
            self.processed_count += 1
            return result
            
        except Exception as e:
            self.failed_count += 1
            logger.error(f"Ошибка обработки компании {company_id}: {e}")
            logger.error(traceback.format_exc())
            
            return {
                'id_компании': company_id,
                'статус': f'Ошибка обработки: {str(e)[:100]}',
                'дата_парсинга': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'статус_парсинга': 'ОШИБКА'
            }
    
    async def run(self):
        """Основной метод запуска парсера"""
        logger.info("=" * 70)
        logger.info("🚀 ЗАПУСК ПАРСЕРА РЕЕСТРА ФСА")
        logger.info("=" * 70)
        
        self.start_time = time.time()
        
        # Загружаем ID компаний
        company_ids = self.data_manager.load_company_ids()
        
        if not company_ids:
            logger.error("❌ Нет ID компаний для обработки!")
            return
        
        total = len(company_ids)
        logger.info(f"📊 Всего записей для обработки: {total}")
        
        # Создаем список для результатов
        all_results = []
        batch_results = []
        
        # Создаем семафор для ограничения одновременных запросов
        sem = asyncio.Semaphore(CONFIG["concurrency"])
        
        async def process_with_limit(company_id: str, idx: int):
            """Обработка с ограничением одновременных запросов"""
            async with sem:
                # Пропускаем, если достигли лимита
                if CONFIG["max_records"] > 0 and idx > CONFIG["max_records"]:
                    return None
                
                return await self.process_single_company(api_client, company_id)
        
        # Создаем API клиент
        async with FSAApiClient() as api_client:
            # Создаем задачи
            tasks = []
            for idx, company_id in enumerate(company_ids, 1):
                task = process_with_limit(company_id, idx)
                tasks.append(task)
            
            # Обрабатываем задачи по мере готовности
            current_batch = 0
            
            for idx, task in enumerate(asyncio.as_completed(tasks), 1):
                try:
                    result = await task
                    
                    if result:
                        batch_results.append(result)
                        all_results.append(result)
                        
                        # Промежуточное сохранение
                        if len(batch_results) >= CONFIG["batch_size"]:
                            self.data_manager.save_results(
                                batch_results, 
                                f"часть_{current_batch + 1}_{CONFIG['output_excel']}",
                                'excel'
                            )
                            
                            # Сохраняем также в общий файл
                            self.data_manager.save_results(
                                all_results,
                                CONFIG["output_excel"],
                                'excel'
                            )
                            
                            batch_results = []
                            current_batch += 1
                    
                    # Вывод прогресса
                    if idx % 100 == 0 or idx == total:
                        elapsed = time.time() - self.start_time
                        processed = self.processed_count + self.failed_count
                        
                        if elapsed > 0:
                            speed = processed / elapsed
                            remaining = (total - processed) / speed if speed > 0 else 0
                            
                            logger.info(
                                f"📈 Прогресс: {processed}/{total} ({processed/total*100:.1f}%) | "
                                f"Скорость: {speed:.1f}/сек | "
                                f"Ошибок: {self.failed_count} | "
                                f"Осталось: ~{remaining/60:.0f} мин"
                            )
                
                except Exception as e:
                    logger.error(f"Ошибка в основной задаче {idx}: {e}")
                    self.failed_count += 1
        
        # Сохраняем оставшиеся данные
        if batch_results:
            self.data_manager.save_results(
                batch_results,
                f"часть_{current_batch + 1}_{CONFIG['output_excel']}",
                'excel'
            )
        
        # Финальное сохранение
        self.data_manager.save_results(all_results, CONFIG["output_excel"], 'excel')
        self.data_manager.save_results(all_results, CONFIG["output_csv"], 'csv')
        self.data_manager.save_results(all_results, CONFIG["output_json"], 'json')
        
        # Генерация отчета
        self.generate_report(total, all_results)
    
    def generate_report(self, total: int, results: List[Dict]):
        """Генерация итогового отчета"""
        end_time = time.time()
        total_time = end_time - self.start_time
        
        # Статистика по статусам
        status_stats = {}
        for result in results:
            status = result.get('статус', 'Неизвестно')
            status_stats[status] = status_stats.get(status, 0) + 1
        
        logger.info("=" * 70)
        logger.info("✅ ПАРСИНГ ЗАВЕРШЕН!")
        logger.info("=" * 70)
        logger.info(f"📊 ОБЩАЯ СТАТИСТИКА:")
        logger.info(f"   Всего записей: {total}")
        logger.info(f"   Успешно обработано: {self.processed_count}")
        logger.info(f"   Записей с ошибками: {self.failed_count}")
        logger.info(f"   Процент успеха: {(self.processed_count/(self.processed_count + self.failed_count))*100:.1f}%")
        logger.info(f"⏱️  ВРЕМЯ ВЫПОЛНЕНИЯ:")
        logger.info(f"   Общее время: {total_time/60:.1f} минут")
        logger.info(f"   Средняя скорость: {self.processed_count/total_time:.2f} записей/сек")
        logger.info(f"💾 ВЫХОДНЫЕ ФАЙЛЫ:")
        logger.info(f"   Основной Excel: {CONFIG['output_excel']}")
        logger.info(f"   Резервный CSV: {CONFIG['output_csv']}")
        logger.info(f"   Резервный JSON: {CONFIG['output_json']}")
        logger.info(f"   Лог файл: {CONFIG['log_file']}")
        logger.info(f"📈 СТАТИСТИКА ПО СТАТУСАМ:")
        for status, count in sorted(status_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"   {status}: {count}")
        logger.info("=" * 70)
        
        # Сохраняем отчет в файл
        report = f"""
ОТЧЕТ О ПАРСИНГЕ РЕЕСТРА ФСА
{'=' * 50}

ДАТА ВЫПОЛНЕНИЯ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

СТАТИСТИКА:
- Всего записей для обработки: {total}
- Успешно обработано: {self.processed_count}
- Записей с ошибками: {self.failed_count}
- Процент успеха: {(self.processed_count/(self.processed_count + self.failed_count))*100:.1f}%

ВРЕМЯ ВЫПОЛНЕНИЯ:
- Общее время: {total_time/60:.1f} минут
- Средняя скорость: {self.processed_count/total_time:.2f} записей/сек

ВЫХОДНЫЕ ФАЙЛЫ:
1. {CONFIG['output_excel']} - основной файл Excel
2. {CONFIG['output_csv']} - резервный файл CSV
3. {CONFIG['output_json']} - резервный файл JSON
4. {CONFIG['log_file']} - файл логов

РАСПРЕДЕЛЕНИЕ ПО СТАТУСАМ:
"""
        
        for status, count in sorted(status_stats.items(), key=lambda x: x[1], reverse=True):
            report += f"- {status}: {count}\n"
        
        report_path = "отчет_о_парсинге.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"📄 Отчет сохранен в: {report_path}")

# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================
def check_environment():
    """Проверка окружения и зависимостей"""
    print("🔍 ПРОВЕРКА ОКРУЖЕНИЯ...")
    
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
    
    # Проверка API
    print("\n🔍 ПРОВЕРКА ДОСТУПНОСТИ API...")
    
    test_url = "https://pub.fsa.gov.ru/api/v1/ral/common/companies/1"
    headers = {
        "authorization": CONFIG["auth_token"],
        "user-agent": CONFIG["user_agent"],
    }
    
    try:
        response = requests.get(test_url, headers=headers, timeout=10)
        print(f"  Статус API: {response.status_code}")
        
        if response.status_code == 200:
            print("  ✅ API доступен")
            data = response.json()
            print(f"  Пример данных:")
            print(f"    ID: {data.get('id')}")
            print(f"    Название: {data.get('fullName')}")
            print(f"    ИНН: {data.get('inn')}")
            
            # Сохраняем пример
            with open("пример_api.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print("  Пример сохранен в 'пример_api.json'")
            
        elif response.status_code == 401:
            print("  ❌ Ошибка авторизации. Проверьте токен в CONFIG['auth_token']")
        else:
            print(f"  ⚠️ API вернул код: {response.status_code}")
            
    except Exception as e:
        print(f"  ❌ Ошибка подключения: {e}")

def create_sample_files():
    """Создание примеров файлов"""
    print("\n📁 СОЗДАНИЕ ПРИМЕРНЫХ ФАЙЛОВ...")
    
    # 1. Файл с ID
    sample_ids = [str(i) for i in range(1, 101)]  # 1-100
    with open("пример_company_ids.txt", "w", encoding="utf-8") as f:
        f.write("# Пример файла с ID компаний\n")
        f.write("# Каждая строка - один ID\n\n")
        f.write("\n".join(sample_ids))
    print("  Создан: пример_company_ids.txt (100 ID)")
    
    # 2. Файл со ссылками
    sample_links = [
        "https://pub.fsa.gov.ru/ral/view/1/current-aa",
        "https://pub.fsa.gov.ru/ral/view/2/current-aa",
        "https://pub.fsa.gov.ru/ral/view/3/current-aa",
    ]
    with open("пример_links.txt", "w", encoding="utf-8") as f:
        f.write("# Пример файла со ссылками\n")
        f.write("# Парсер извлечет ID автоматически\n\n")
        f.write("\n".join(sample_links))
    print("  Создан: пример_links.txt (3 ссылки)")
    
    # 3. Конфигурационный файл
    config_sample = {
        "инструкция": "Это пример конфигурации",
        "токен": "Ваш_токен_здесь",
        "лимит_записей": 1000,
        "сохранять_каждые": 100
    }
    with open("пример_config.json", "w", encoding="utf-8") as f:
        json.dump(config_sample, f, ensure_ascii=False, indent=2)
    print("  Создан: пример_config.json")

def print_help():
    """Вывод справки"""
    help_text = """
ПАРСЕР РЕЕСТРА ФСА - СПРАВКА

ИСПОЛЬЗОВАНИЕ:
  python fsa_parser.py [команда]

КОМАНДЫ:
  run      - Запуск полного парсинга
  test     - Тестовый запуск (10 записей)
  check    - Проверка окружения и API
  sample   - Создание примеров файлов
  help     - Эта справка

ПОДГОТОВКА:
  1. Проверьте окружение: python fsa_parser.py check
  2. Создайте файл с ID компаний (company_ids.txt)
  3. Запустите тест: python fsa_parser.py test
  4. Запустите полный парсер: python fsa_parser.py run

ФАЙЛЫ:
  - company_ids.txt - список ID (по одному на строку)
  - links.txt - список ссылок на страницы компаний
  - ids.txt - альтернативное название файла с ID

ВЫХОДНЫЕ ФАЙЛЫ:
  - реестр_фса_полный.xlsx - основной файл Excel
  - реестр_фса_полный.csv - резервный CSV
  - реестр_фса_полный.json - резервный JSON
  - fsa_parser_full.log - файл логов
  - отчет_о_парсинге.txt - статистика

ПРИМЕЧАНИЯ:
  - Для 38000 записей потребуется ~2-3 часа
  - Токен авторизации может потребовать обновления
  - Парсер автоматически сохраняет промежуточные результаты
    """
    print(help_text)

# ================= ТОЧКА ВХОДА =================
async def main():
    """Основная функция"""
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
    else:
        command = "help"
    
    if command == "run":
        # Полный запуск
        parser = FSAParser()
        await parser.run()
        
    elif command == "test":
        # Тестовый запуск
        print("🧪 ТЕСТОВЫЙ ЗАПУСК (10 записей)")
        CONFIG["max_records"] = 10
        CONFIG["output_excel"] = "тест_реестр.xlsx"
        
        # Создаем тестовые ID
        test_ids = [str(i) for i in range(1, 11)]
        with open("company_ids.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(test_ids))
        print("Создан company_ids.txt с 10 ID")
        
        parser = FSAParser()
        await parser.run()
        
    elif command == "check":
        # Проверка окружения
        check_environment()
        
    elif command == "sample":
        # Создание примеров файлов
        create_sample_files()
        
    elif command == "help":
        # Справка
        print_help()
        
    else:
        print(f"Неизвестная команда: {command}")
        print("Используйте: python fsa_parser.py help")

if __name__ == "__main__":
    # Запуск асинхронного кода
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Парсер остановлен пользователем")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
        print("Подробности в лог-файле")
