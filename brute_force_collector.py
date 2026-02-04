# brute_force_collector.py
import requests
from concurrent.futures import ThreadPoolExecutor
import time

def brute_force_collect():
    """Грубый перебор ID компаний"""
    
    # Создаем сессию для ускорения запросов
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    
    found_links = []
    
    # Пробуем разные диапазоны
    ranges = [
        (1, 100000),     # Первые 100к
        (100000, 200000),
        (200000, 300000),
        (300000, 400000),
        (400000, 500000),
        (500000, 600000),
        (600000, 700000),
        (700000, 800000),
        (800000, 900000),
        (900000, 1000000)
    ]
    
    def check_range(start, end, step=1):
        """Проверяет диапазон ID"""
        local_found = []
        
        for company_id in range(start, end + 1, step):
            url = f"https://pub.fsa.gov.ru/ral/view/{company_id}/current-aa"
            
            try:
                response = session.head(url, timeout=3)
                if response.status_code == 200:
                    local_found.append(url)
                    print(f"✓ Найдена компания ID={company_id}")
            except:
                continue
        
        return local_found
    
    print("Начинаю грубый перебор ID...")
    print("Это может занять много времени!")
    
    # Проверяем диапазоны
    for start, end in ranges:
        print(f"\nПроверяю диапазон {start:,} - {end:,}")
        links = check_range(start, end, step=100)  # Шаг 100 для скорости
        found_links.extend(links)
        
        # Сохраняем промежуточные результаты
        if links:
            with open("found_links.txt", "a", encoding="utf-8") as f:
                for link in links:
                    f.write(f"{link}\n")
            
            print(f"Найдено в этом диапазоне: {len(links)}")
            print(f"Всего найдено: {len(found_links)}")
        
        time.sleep(1)
    
    return found_links

if __name__ == "__main__":
    links = brute_force_collect()
    print(f"\nИтого найдено: {len(links)} ссылок")
