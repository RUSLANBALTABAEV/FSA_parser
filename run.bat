@echo off
chcp 65001 >nul
cls

echo ========================================
echo    ПАРСЕР FSA - АВТОМАТИЧЕСКИЙ ЗАПУСК
echo ========================================
echo.

echo Шаг 1: Проверка Python...
python --version
if errorlevel 1 (
    echo ОШИБКА: Python не установлен!
    echo Скачайте Python с python.org
    pause
    exit /b 1
)

echo.
echo Шаг 2: Проверка config.py...
if not exist "config.py" (
    echo Создаю файл config.py...
    echo # Токен для FSA API> config.py
    echo TOKEN = "ВСТАВЬТЕ_СЮДА_ВАШ_ТОКЕН">> config.py
    echo.
    echo ВАЖНО: Откройте файл config.py и вставьте токен!
    echo Как получить токен:
    echo 1. Откройте https://pub.fsa.gov.ru/ral
    echo 2. Нажмите F12 -> Network (Сеть)
    echo 3. Обновите страницу (F5)
    echo 4. Найдите запрос к API
    echo 5. Скопируйте заголовок Authorization
    echo 6. Вставьте в config.py
    pause
    exit /b 1
)

echo.
echo Шаг 3: Установка библиотек...
pip install aiohttp openpyxl --quiet
echo Библиотеки установлены.

echo.
echo Шаг 4: Сбор ссылок на компании...
echo Это может занять несколько минут...
python collect_links.py

if not exist "all_links.txt" (
    echo ОШИБКА: Файл со ссылками не создан!
    echo Проверьте токен в config.py
    pause
    exit /b 1
)

echo.
echo Шаг 5: Проверка количества ссылок...
for /f %%i in ('type "all_links.txt" 2^>nul ^| find /c /v ""') do set count=%%i
echo Найдено ссылок: %count%

echo.
set /p choice="Запустить парсинг всех компаний (%count% шт)? (y/n): "
if /i "%choice%" neq "y" (
    echo Создаю тестовый файл на 10 компаний...
    powershell -Command "Get-Content 'all_links.txt' | Select-Object -First 10 > test_10.txt"
    copy test_10.txt all_links.txt >nul
    echo Будут обработаны первые 10 компаний.
    set count=10
)

echo.
echo Шаг 6: Запуск основного парсера...
echo Обработка %count% компаний...
echo Это займет примерно %count% секунд...
echo.
python main.py

echo.
echo ========================================
echo    РЕЗУЛЬТАТЫ:
echo ========================================
echo.
echo Созданные файлы:
dir *.xlsx
dir *.json
echo.
echo Основной файл с данными: fsa_data_*.xlsx
echo.
echo Нажмите любую клавишу для выхода...
pause >nul
