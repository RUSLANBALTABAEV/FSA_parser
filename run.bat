@echo off
chcp 65001 >nul
cls

echo ============================================
echo    ПАРСЕР ДАННЫХ FSA
echo ============================================
echo.

echo Шаг 1: Проверка Python...
python --version
if errorlevel 1 (
    echo ОШИБКА: Python не установлен!
    echo Скачайте с python.org
    pause
    exit /b 1
)

echo.
echo Шаг 2: Установка библиотек...
pip install aiohttp beautifulsoup4 lxml openpyxl --quiet
if errorlevel 1 (
    echo Пробую установить по одной...
    pip install aiohttp --quiet
    pip install beautifulsoup4 --quiet
    pip install lxml --quiet
    pip install openpyxl --quiet
)
echo Библиотеки установлены!

echo.
echo Шаг 3: Настройка диапазона ID...
echo Текущие настройки в config.py:
echo   Начальный ID: 38450
echo   Конечный ID: 40000
echo   Шаг: 1
echo.
set /p choice="Изменить настройки? (y/n): "
if /i "%choice%"=="y" (
    set /p start_id="Начальный ID: "
    set /p end_id="Конечный ID: "
    set /p step="Шаг: "
    
    echo Обновляю config.py...
    python -c "
import re
with open('config.py', 'r', encoding='utf-8') as f:
    content = f.read()
content = re.sub(r'\"start\": \d+', f'\"start\": {start_id}', content)
content = re.sub(r'\"end\": \d+', f'\"end\": {end_id}', content)
content = re.sub(r'\"step\": \d+', f'\"step\": {step}', content)
with open('config.py', 'w', encoding='utf-8') as f:
    f.write(content)
print('Настройки обновлены!')
"
)

echo.
echo Шаг 4: Генерация ссылок...
echo Будет создан файл all_links.txt
echo.
python generate_links.py
if errorlevel 1 (
    echo ОШИБКА при генерации ссылок!
    pause
    exit /b 1
)

echo.
echo Шаг 5: Проверка файла со ссылками...
if not exist "all_links.txt" (
    echo Файл all_links.txt не найден!
    pause
    exit /b 1
)

for /f %%i in ('type "all_links.txt" 2^>nul ^| find /c /v ""') do set count=%%i
echo Найдено ссылок: %count%

if "%count%"=="0" (
    echo Файл пустой!
    pause
    exit /b 1
)

echo.
echo Шаг 6: Настройка парсера...
echo Параллельных запросов: 10
echo Таймаут: 15 секунд
echo Сохранение каждые: 50 записей
echo.
set /p choice="Изменить настройки парсера? (y/n): "
if /i "%choice%"=="y" (
    set /p concurrent="Параллельных запросов (1-50): "
    set /p timeout="Таймаут в секундах (5-60): "
    set /p save_every="Сохранять каждые (10-1000): "
    
    echo Обновляю настройки...
    python -c "
import re
with open('config.py', 'r', encoding='utf-8') as f:
    content = f.read()
content = re.sub(r'MAX_CONCURRENT_REQUESTS = \d+', f'MAX_CONCURRENT_REQUESTS = {concurrent}', content)
content = re.sub(r'REQUEST_TIMEOUT = \d+', f'REQUEST_TIMEOUT = {timeout}', content)
content = re.sub(r'SAVE_EVERY = \d+', f'SAVE_EVERY = {save_every}', content)
with open('config.py', 'w', encoding='utf-8') as f:
    f.write(content)
print('Настройки обновлены!')
"
)

echo.
echo Шаг 7: Создание тестового набора...
echo Для теста можно обработать только часть ссылок
echo.
set /p choice="Создать тестовый файл? (y/n): "
if /i "%choice%"=="y" (
    set /p test_count="Количество ссылок для теста (10-1000): "
    echo Создаю тестовый файл...
    python -c "
with open('all_links.txt', 'r', encoding='utf-8') as f:
    links = [line.strip() for line in f if line.strip()]
test_links = links[:int('$test_count%')]
with open('test_links.txt', 'w', encoding='utf-8') as f:
    for link in test_links:
        f.write(link + '\n')
print(f'Создан тестовый файл с {len(test_links)} ссылками')
"
    
    echo.
    set /p use_test="Использовать тестовый файл? (y/n): "
    if /i "%use_test%"=="y" (
        copy test_links.txt all_links.txt >nul
        echo Используется тестовый файл с %test_count% ссылками
    )
)

echo.
echo Шаг 8: Запуск парсера...
echo ВНИМАНИЕ: Обработка %count% компаний займет время!
echo Примерное время:
echo   - 100 компаний: 5-10 минут
echo   - 1000 компаний: 30-60 минут
echo   - 10000 компаний: 4-8 часов
echo.
echo Рекомендуется запускать на ночь!
echo.
set /p choice="Запустить парсер? (y/n): "
if /i "%choice%"=="y" (
    echo.
    echo Запуск основного парсера...
    echo Результаты будут сохранены в:
    echo   - fsa_companies_data.xlsx (Excel)
    echo   - fsa_companies_data.csv (CSV)
    echo   - fsa_companies_data.json (JSON)
    echo   - parsing_stats.json (статистика)
    echo   - parser.log (лог работы)
    echo.
    echo Для остановки нажмите Ctrl+C
    echo.
    python parser.py
)

echo.
echo ============================================
echo    РЕЗУЛЬТАТЫ
echo ============================================
echo.
echo Созданные файлы:
dir *.xlsx 2>nul && echo.
dir *.csv 2>nul && echo.
dir *.json 2>nul && echo.

if exist "parsing_stats.json" (
    echo.
    echo Статистика парсинга:
    type parsing_stats.json
)

echo.
echo Лог работы:
if exist "parser.log" (
    echo Файл: parser.log
    echo Последние 10 строк:
    python -c "
try:
    with open('parser.log', 'r', encoding='utf-8') as f:
        lines = f.readlines()
    for line in lines[-10:]:
        print(line.strip())
except:
    print('Не удалось прочитать лог')
"
)

echo.
echo ============================================
echo    ДАЛЬНЕЙШИЕ ДЕЙСТВИЯ
echo ============================================
echo.
echo 1. Откройте fsa_companies_data.xlsx в Excel
echo 2. Используйте фильтры для анализа данных
echo 3. Проверьте parser.log для отладки ошибок
echo.
echo Для увеличения объема данных:
echo 1. Измените ID_RANGE в config.py
echo 2. Запустите generate_links.py
echo 3. Запустите parser.py
echo.
pause
