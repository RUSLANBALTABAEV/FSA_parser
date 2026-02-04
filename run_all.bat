@echo off
chcp 65001 >nul
cls

echo ========================================
echo    ПАРСЕР ВСЕХ 38000+ РЕЕСТРОВ FSA
echo ========================================
echo.

echo 1. Проверка Python...
python --version
if errorlevel 1 (
    echo ОШИБКА: Python не установлен!
    echo Скачайте Python с python.org
    pause
    exit /b 1
)

echo.
echo 2. Проверка config.py...
if not exist "config.py" (
    echo Файл config.py не найден!
    echo.
    echo Создайте файл config.py со следующим содержимым:
    echo.
    echo TOKEN = "Bearer ВАШ_ТОКЕН"
    echo BASE_URL = "https://pub.fsa.gov.ru"
    echo API_BASE = "https://pub.fsa.gov.ru/api/v1"
    echo.
    echo Получить токен:
    echo 1. Откройте https://pub.fsa.gov.ru/ral
    echo 2. Нажмите F12 -> Network
    echo 3. Обновите страницу (F5)
    echo 4. Найдите запрос к API
    echo 5. Скопируйте заголовок Authorization
    echo 6. Вставьте в config.py
    pause
    exit /b 1
)

echo.
echo 3. Установка библиотек...
pip install aiohttp openpyxl aiofiles --quiet
echo Библиотеки установлены

echo.
echo 4. Сбор всех ссылок (38000+ компаний)...
echo Это займет 10-20 минут...
python collect_links.py

if not exist "all_links.txt" (
    echo ОШИБКА: Файл со ссылками не создан!
    echo Проверьте токен в config.py
    pause
    exit /b 1
)

echo.
echo 5. Проверка количества ссылок...
for /f %%i in ('type "all_links.txt" 2^>nul ^| find /c /v ""') do set count=%%i
echo Найдено ссылок: %count%

echo.
echo 6. Запуск основного парсера...
echo ВНИМАНИЕ: Обработка %count% компаний займет 4-8 часов!
echo Рекомендуется запускать на ночь.
echo.
set /p choice="Запустить полный парсинг? (y/n): "
if /i "%choice%" neq "y" (
    echo.
    echo Создаю тестовый файл на 50 компаний...
    powershell -Command "Get-Content 'all_links.txt' | Select-Object -First 50 > test_50_links.txt"
    copy test_50_links.txt all_links.txt >nul
    echo Будут обработаны первые 50 компаний
    set /p choice2="Запустить тестовый парсинг? (y/n): "
    if /i "%choice2%" neq "y" (
        echo Отменено
        pause
        exit /b 0
    )
)

echo.
echo Запуск парсинга...
echo Ожидайте создания файла FSA_реестры_все_*.xlsx
echo.
python main.py

echo.
echo ========================================
echo    РЕЗУЛЬТАТЫ:
echo ========================================
echo.
echo Созданные файлы:
dir *.xlsx
echo.
echo Статистика:
type parsing_stats.json 2>nul || echo Файл статистики не найден
echo.
echo Примерное время обработки:
echo - 50 компаний: 2-5 минут
echo - 1000 компаний: 30-60 минут
echo - 38000 компаний: 4-8 часов
echo.
echo Готово! Основной файл: FSA_реестры_все_*.xlsx
echo.
pause
