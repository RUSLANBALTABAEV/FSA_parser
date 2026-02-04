@echo off
chcp 65001 >nul
cls

echo ============================================
echo    ПАРСЕР ВСЕХ КОМПАНИЙ FSA
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
pip install aiohttp openpyxl aiofiles --quiet
echo Библиотеки установлены!

echo.
echo Шаг 3: Получение токена...
echo Сначала нужно получить токен для доступа к API
echo Браузер откроется автоматически...
echo.
pause
python get_token.py

if not exist "config.py" (
    echo Токен не получен! Повторите попытку.
    pause
    exit /b 1
)

echo.
echo Шаг 4: Проверка токена...
python -c "from config import TOKEN; print('Токен найден!' if TOKEN and 'Bearer' in TOKEN else 'Токен не найден!')"

echo.
echo Шаг 5: Сбор всех ссылок на компании...
echo Это займет 10-30 минут...
python collect_links.py

if not exist "all_links.txt" (
    echo Не удалось собрать ссылки!
    pause
    exit /b 1
)

echo.
echo Шаг 6: Проверка количества ссылок...
for /f %%i in ('type "all_links.txt" ^| find /c /v ""') do set count=%%i
echo Найдено ссылок: %count%

echo.
echo Шаг 7: Парсинг данных всех компаний...
echo ВНИМАНИЕ: Обработка %count% компаний займет несколько часов!
echo Рекомендуется запускать на ночь.
echo.
set /p choice="Запустить полный парсинг? (y/n): "
if /i not "%choice%"=="y" (
    echo Создаю тестовый файл на 10 компаний...
    python -c "with open('all_links.txt', 'r', encoding='utf-8') as f: links = [line.strip() for line in f if line.strip()]; open('test_10.txt', 'w', encoding='utf-8').write('\n'.join(links[:10]))"
    move /y test_10.txt all_links.txt >nul
    echo Будут обработаны первые 10 компаний.
)

echo.
echo Запуск основного парсера...
echo Результаты будут сохранены в файлы FSA_all_companies_*.xlsx и *.csv
echo.
python main.py

echo.
echo ============================================
echo    РЕЗУЛЬТАТЫ
echo ============================================
echo.
dir *.xlsx
dir *.csv
echo.
echo Готово! Основной файл: FSA_all_companies_*.xlsx
echo.
pause
