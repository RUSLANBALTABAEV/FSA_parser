"""
КОНФИГУРАЦИЯ ПАРСЕРА FSA С ПРОКСИ
"""

# Базовые настройки
BASE_URL = "https://pub.fsa.gov.ru"
API_BASE = "https://pub.fsa.gov.ru/api/v1"

# Диапазон ID для генерации ссылок
ID_RANGE = {
    "start": 38450,   # Начальный ID
    "end": 40000,     # Конечный ID
    "step": 1         # Шаг
}

# Настройки парсинга
MAX_CONCURRENT_REQUESTS = 10    # Одновременных запросов
REQUEST_TIMEOUT = 15           # Таймаут в секундах
RETRY_ATTEMPTS = 3             # Попыток при ошибке
SAVE_EVERY = 50               # Сохранять каждые N записей

# Настройки вывода
OUTPUT_FORMATS = ["excel", "csv"]  # Форматы вывода

# ================================
# НАСТРОЙКИ ПРОКСИ
# ================================
# Формат: "http://login:password@ip:port" или "http://ip:port"
PROXY = "http://gZYKDm:VaRKoM@193.31.102.226:9435"

# ИЛИ использовать несколько прокси (работает ротация)
PROXY_LIST = [
    "http://gZYKDm:VaRKoM@193.31.102.226:9435",
    # Или попробуйте формат без http://
    "193.31.102.226:9435",
    # Или с HTTPS
    "https://gZYKDm:VaRKoM@193.31.102.226:9435",
]

# Режим работы с прокси:
# "single" - использовать один прокси
# "rotate" - ротировать несколько прокси
# "none" - не использовать прокси
PROXY_MODE = "rotate"

# Проверять ли SSL сертификаты (False для многих прокси)
VERIFY_SSL = True

REQUEST_TIMEOUT = 30
MAX_CONCURRENT_REQUESTS = 3  # Уменьшите для стабильности

# Имена файлов
LINKS_FILE = "all_links.txt"
EXCEL_FILE = "fsa_companies_data.xlsx"
CSV_FILE = "fsa_companies_data.csv"
JSON_FILE = "fsa_companies_data.json"
STATS_FILE = "parsing_stats.json"
LOG_FILE = "parser.log"

# Заголовки для запросов (с прокси могут понадобиться другие)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}
