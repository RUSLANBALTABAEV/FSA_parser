#!/bin/bash

echo "============================================"
echo "ПАРСЕР ДАННЫХ FSA"
echo "============================================"
echo

echo "Шаг 1: Проверка Python..."
python3 --version || { echo "ОШИБКА: Python не установлен!"; exit 1; }

echo
echo "Шаг 2: Установка библиотек..."
pip3 install aiohttp beautifulsoup4 lxml openpyxl --quiet
if [ $? -ne 0 ]; then
    echo "Пробую установить по одной..."
    pip3 install aiohttp --quiet
    pip3 install beautifulsoup4 --quiet
    pip3 install lxml --quiet
    pip3 install openpyxl --quiet
fi
echo "Библиотеки установлены!"

echo
echo "Шаг 3: Генерация ссылок..."
echo "Будет создан файл all_links.txt"
echo
python3 generate_links.py
if [ $? -ne 0 ]; then
    echo "ОШИБКА при генерации ссылок!"
    exit 1
fi

echo
echo "Шаг 4: Проверка файла со ссылками..."
if [ ! -f "all_links.txt" ]; then
    echo "Файл all_links.txt не найден!"
    exit 1
fi

count=$(wc -l < "all_links.txt")
echo "Найдено ссылок: $count"

if [ "$count" -eq 0 ]; then
    echo "Файл пустой!"
    exit 1
fi

echo
echo "Шаг 5: Запуск парсера..."
echo "ВНИМАНИЕ: Обработка $count компаний займет время!"
echo "Примерное время:"
echo "  - 100 компаний: 5-10 минут"
echo "  - 1000 компаний: 30-60 минут"
echo "  - 10000 компаний: 4-8 часов"
echo
read -p "Запустить парсер? (y/n): " choice
if [[ "$choice" =~ ^[Yy]$ ]]; then
    echo
    echo "Запуск основного парсера..."
    echo "Результаты будут сохранены в:"
    echo "  - fsa_companies_data.xlsx (Excel)"
    echo "  - fsa_companies_data.csv (CSV)"
    echo "  - fsa_companies_data.json (JSON)"
    echo "  - parsing_stats.json (статистика)"
    echo "  - parser.log (лог работы)"
    echo
    echo "Для остановки нажмите Ctrl+C"
    echo
    python3 parser.py
fi

echo
echo "============================================"
echo "РЕЗУЛЬТАТЫ"
echo "============================================"
echo
echo "Созданные файлы:"
ls -la *.xlsx *.csv *.json 2>/dev/null | head -10

if [ -f "parsing_stats.json" ]; then
    echo
    echo "Статистика парсинга:"
    cat parsing_stats.json | python3 -m json.tool 2>/dev/null || cat parsing_stats.json
fi

echo
echo "Лог работы:"
if [ -f "parser.log" ]; then
    echo "Файл: parser.log"
    echo "Последние 10 строк:"
    tail -10 parser.log 2>/dev/null || echo "Не удалось прочитать лог"
fi

echo
echo "============================================"
echo "ДАЛЬНЕЙШИЕ ДЕЙСТВИЯ"
echo "============================================"
echo
echo "1. Откройте fsa_companies_data.xlsx в Excel"
echo "2. Используйте фильтры для анализа данных"
echo "3. Проверьте parser.log для отладки ошибок"
echo
echo "Для увеличения объема данных:"
echo "1. Измените ID_RANGE в config.py"
echo "2. Запустите generate_links.py"
echo "3. Запустите parser.py"
echo
