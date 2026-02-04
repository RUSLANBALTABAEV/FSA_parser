#!/bin/bash

echo "========================================"
echo "ПАРСЕР ДАННЫХ FSA"
echo "========================================"
echo ""

echo "Шаг 1: Установка зависимостей..."
pip3 install -r requirements.txt

echo ""
echo "Шаг 2: Сбор всех ссылок на компании..."
echo "Это может занять несколько минут..."
python3 collect_links.py

echo ""
echo "Шаг 3: Проверка собранных ссылок..."
if [ -f "all_links.txt" ]; then
    count=$(wc -l < "all_links.txt")
    echo "Найдено ссылок: $count"
    
    echo ""
    echo "Создаю тестовый файл с первыми 10 ссылками..."
    head -10 all_links.txt > test_10_links.txt
    
    echo ""
    echo "Шаг 4: Запуск основного парсера..."
    echo "Это займет время в зависимости от количества компаний..."
    echo ""
    
    read -p "Хотите протестировать на 10 компаниях? (y/n): " choice
    if [[ $choice == "y" || $choice == "Y" ]]; then
        echo "Запуск тестового парсинга..."
        cp test_10_links.txt all_links.txt
    else
        echo "Использую все собранные ссылки..."
    fi
    
    python3 main.py
    
else
    echo "Файл со ссылками не найден!"
    exit 1
fi

echo ""
echo "========================================"
echo "РЕЗУЛЬТАТЫ:"
echo "========================================"
echo ""
echo "Созданные файлы:"
echo "1. all_links.txt - все ссылки на компании"
echo "2. test_10_links.txt - первые 10 ссылок для теста"
echo "3. fsa_companies_*.xlsx - основной файл с данными"
echo "4. parsing_stats.json - статистика парсинга"
echo "5. parser.log - лог работы"
echo "6. links_stats.json - статистика сбора ссылок"
echo ""
echo "Для просмотра результатов откройте Excel файл."
echo ""
