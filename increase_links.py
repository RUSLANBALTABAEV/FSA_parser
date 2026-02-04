# increase_links.py
"""
Скрипт для увеличения количества ссылок в all_links.txt
"""
import requests
import time
import json
import random
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

def get_random_user_agent():
    """Возвращает случайный User-Agent"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(user_agents)

def check_company_id(company_id):
    """Проверяет, существует ли компания с указанным ID"""
    url = f"https://pub.fsa.gov.ru/ral/view/{company_id}/current-aa"
    
    try:
        headers = {
            "User-Agent": get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        
        # Используем HEAD запрос для экономии трафика
        response = requests.head(url, headers=headers, timeout=5, allow_redirects=True)
        
        # Если страница существует (статус 200) и это не ошибка 404
        if response.status_code == 200:
            # Дополнительная проверка - делаем GET запрос для уверенности
            try:
                response_get = requests.get(url, headers=headers, timeout=5)
                if "реестр аккредитованных лиц" in response_get.text.lower():
                    return url
            except:
                return url if response.status_code == 200 else None
        
        return None
    except Exception as e:
        return None

def load_existing_links():
    """Загружает существующие ссылки"""
    try:
        with open("all_links.txt", "r", encoding="utf-8") as f:
            links = [line.strip() for line in f if line.strip()]
        print(f"Загружено {len(links)} существующих ссылок")
        return set(links)
    except FileNotFoundError:
        print("Файл all_links.txt не найден, создаем новый")
        return set()

def save_new_links(new_links, all_links):
    """Сохраняет новые ссылки"""
    if not new_links:
        return
    
    # Открываем файл в режиме добавления
    with open("all_links.txt", "a", encoding="utf-8") as f:
        for link in new_links:
            f.write(f"{link}\n")
    
    print(f"Добавлено {len(new_links)} новых ссылок")
    print(f"Всего ссылок: {len(all_links) + len(new_links)}")
    
    # Сохраняем статистику
    stats = {
        "total_links": len(all_links) + len(new_links),
        "new_links_added": len(new_links),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "method": "ID scanning"
    }
    
    with open("increase_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def scan_id_range(start_id, end_id, batch_size=1000):
    """Сканирует диапазон ID"""
    print(f"Сканирую ID от {start_id:,} до {end_id:,}")
    
    all_links = load_existing_links()
    new_links = []
    
    # Разбиваем диапазон на батчи
    batches = []
    current = start_id
    while current <= end_id:
        batch_end = min(current + batch_size - 1, end_id)
        batches.append((current, batch_end))
        current = batch_end + 1
    
    print(f"Создано {len(batches)} батчей по {batch_size} ID")
    
    for batch_start, batch_end in tqdm(batches, desc="Сканирование батчей"):
        batch_links = []
        
        # Используем многопоточность для ускорения
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Создаем задачи для проверки ID в батче
            futures = []
            for company_id in range(batch_start, batch_end + 1):
                future = executor.submit(check_company_id, company_id)
                futures.append(future)
            
            # Собираем результаты
            for future in futures:
                result = future.result()
                if result and result not in all_links:
                    batch_links.append(result)
        
        # Добавляем найденные ссылки
        if batch_links:
            new_links.extend(batch_links)
            # Сохраняем каждые 100 найденных ссылок
            if len(new_links) >= 100:
                save_new_links(new_links, all_links)
                all_links.update(new_links)
                new_links = []
        
        # Пауза между батчами
        time.sleep(1)
    
    # Сохраняем оставшиеся ссылки
    if new_links:
        save_new_links(new_links, all_links)
    
    return new_links

def smart_scan():
    """Умное сканирование"""
    print("=" * 70)
    print("УМНОЕ СКАНИРОВАНИЕ ID КОМПАНИЙ")
    print("=" * 70)
    
    # Определяем диапазон на основе уже найденных ссылок
    existing_links = load_existing_links()
    
    if existing_links:
        # Извлекаем ID из существующих ссылок
        ids = []
        for link in existing_links:
            try:
                parts = link.split('/')
                company_id = int(parts[-2]) if parts[-1] == 'current-aa' else int(parts[-1])
                ids.append(company_id)
            except:
                continue
        
        if ids:
            min_id = min(ids)
            max_id = max(ids)
            
            print(f"На основе существующих ссылок:")
            print(f"Минимальный ID: {min_id:,}")
            print(f"Максимальный ID: {max_id:,}")
            print(f"Количество найденных ID: {len(ids):,}")
            
            # Расширяем диапазон в обе стороны
            start_id = max(1, min_id - 50000)
            end_id = max_id + 200000
            
            print(f"\nСканирую расширенный диапазон:")
            print(f"От: {start_id:,}")
            print(f"До: {end_id:,}")
            print(f"Всего ID для проверки: {end_id - start_id + 1:,}")
            
            choice = input("\nНачать сканирование? (y/n): ")
            if choice.lower() == 'y':
                return scan_id_range(start_id, end_id, batch_size=5000)
    else:
        print("Существующих ссылок не найдено. Начинаю с базового диапазона.")
        
        # Базовый диапазон для начала
        start_id = 1
        end_id = 100000
        
        print(f"Сканирую диапазон: {start_id:,} - {end_id:,}")
        
        choice = input("Начать сканирование? (y/n): ")
        if choice.lower() == 'y':
            return scan_id_range(start_id, end_id, batch_size=5000)
    
    return []

def main():
    """Основная функция"""
    print("СКРИПТ ДЛЯ УВЕЛИЧЕНИЯ КОЛИЧЕСТВА ССЫЛОК")
    print("=" * 50)
    
    print("\nДоступные методы:")
    print("1. Умное сканирование (рекомендуется)")
    print("2. Сканирование указанного диапазона")
    print("3. Быстрое сканирование (первые 10000 ID)")
    
    choice = input("\nВыберите метод (1-3): ")
    
    if choice == "1":
        new_links = smart_scan()
    elif choice == "2":
        start_id = int(input("Введите начальный ID: "))
        end_id = int(input("Введите конечный ID: "))
        new_links = scan_id_range(start_id, end_id)
    elif choice == "3":
        new_links = scan_id_range(1, 10000, batch_size=1000)
    else:
        print("Неверный выбор")
        return
    
    if new_links:
        print(f"\nСканирование завершено!")
        print(f"Добавлено новых ссылок: {len(new_links):,}")
        
        # Загружаем финальное количество
        all_links = load_existing_links()
        print(f"Всего ссылок в all_links.txt: {len(all_links):,}")
        
        # Показываем примеры новых ссылок
        print("\nПримеры новых ссылок:")
        for i, link in enumerate(new_links[:5], 1):
            print(f"  {i}. {link}")
    else:
        print("Новых ссылок не найдено")

if __name__ == "__main__":
    main()
