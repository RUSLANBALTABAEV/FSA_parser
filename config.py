"""
КОНФИГУРАЦИЯ ПАРСЕРА
Здесь хранятся настройки и токен
"""

# ТОКЕН для доступа к API FSA
# Как получить новый токен:
# 1. Откройте https://pub.fsa.gov.ru/ral в браузере
# 2. Нажмите F12 -> Network (Сеть)
# 3. Обновите страницу (F5)
# 4. Найдите любой запрос к API
# 5. Скопируйте значение заголовка Authorization
# 6. Вставьте сюда (весь заголовок, включая "Bearer ")

TOKEN = "Bearer eyJhbGciOiJFZERTQSJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzcwMTI4MjgwLCJpYXQiOjE3NzAwOTk0ODB9.YKxvd9y7WcpONvijWRpoOVr__Qjw-wUiKEscvqIEdyjTRrRxE__U-W_TFEW9a7eLHcxV0XgON-AP5T1pXomjAQ"

# Настройки
BASE_URL = "https://pub.fsa.gov.ru"
API_BASE = "https://pub.fsa.gov.ru/api/v1"
