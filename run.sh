#!/bin/bash

echo "============================================"
echo "   ПАРСЕР ВСЕХ КОМПАНИЙ FSA"
echo "============================================"
echo

echo "Шаг 1: Проверка Python..."
python3 --version || { echo "ОШИБКА: Python не установлен!"; exit 1; }

echo
echo "Шаг 2: Установка библиотек..."
pip3 install aiohttp openpyxl aiofiles --quiet
echo "Библиотеки установлены!"

echo
echo "Шаг 3: Получение токена..."
echo "Сначала нужно получить токен для доступа к API"
echo "Браузер откроется автоматически..."
read -p "Нажмите Enter для продолжения..."
python3 get_token.py

if [ ! -f "config.py" ]; then
    echo "Токен не получен! Повторите попытку."
    exit 1
fi

echo
echo "Шаг 4: Проверка токена..."
python3 -c "from config import TOKEN; print('✓ Токен найден!' if TOKEN and 'Bearer' in TOKEN else '✗ Токен не найден!')"

echo
echo "Шаг 5: Сбор всех ссылок на компании..."
echo "Это займет 10-30 минут..."
python3 collect_links.py

if [ ! -f "all_links.txt" ]; then
    echo "✗ Не удалось собрать ссылки!"
    exit 1
fi

echo
echo "Шаг 6: Проверка количества ссылок..."
count=$(wc -l < "all_links.txt")
echo "Найдено ссылок: $count"

echo
echo "Шаг 7: Парсинг данных всех компаний..."
echo "ВНИМАНИЕ: Обработка $count компаний займет несколько часов!"
read -p "Запустить полный парсинг? (y/n): " choice

if [[ ! "$choice" =~ ^[Yy]$ ]]; then
    echo "Создаю тестовый файл на 10 компаний..."
    head -10 all_links.txt > test_10.txt
    mv test_10.txt all_links.txt
    echo "Будут обработаны первые 10 компаний."
fi

echo
echo "Запуск основного парсера..."
echo "Результаты будут сохранены в файлы FSA_all_companies_*.xlsx и *.csv"
echo
python3 main.py

echo
echo "============================================"
echo "   РЕЗУЛЬТАТЫ"
echo "============================================"
echo
ls -la *.xlsx *.csv
echo
echo "Готово! Основной файл: FSA_all_companies_*.xlsx"
