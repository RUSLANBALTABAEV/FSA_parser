# collect_all_companies.py
import requests
import time
import json
from tqdm import tqdm
import sys

def get_companies_by_ranges():
    """Собирает компании по диапазонам ID"""
    
    # Попробуем разные диапазоны ID
    id_ranges = [
        (1, 10000),
        (10001, 20000),
        (20001, 30000),
        (30001, 40000),
        (40001, 50000),
        (50001, 60000),
        (60001, 70000),
        (70001, 80000)
    ]
    
    # Или попробуем шаговый перебор
    id_steps = [
        (1, 100000, 100),  # от 1 до 100000 с шагом 100
    ]
    
    all_links = []
    
    print("Собираю ссылки методом перебора ID...")
    
    for start_id, end_id, step in id_steps:
        for company_id in tqdm(range(start_id, end_id + 1, step)):
            url = f"https://pub.fsa.gov.ru/ral/view/{company_id}/current-aa"
            
            # Проверяем, доступна ли страница
            try:
                response = requests.head(url, timeout=5)
                if response.status_code == 200:
                    all_links.append(url)
                    print(f"Найдена компания ID={company_id}")
                    
                    # Сохраняем каждые 100 найденных компаний
                    if len(all_links) % 100 == 0:
                        save_links(all_links)
            except:
                continue
    
    return all_links

def save_links(links):
    """Сохраняет ссылки в файл"""
    with open("all_links.txt", "w", encoding="utf-8") as f:
        for link in links:
            f.write(f"{link}\n")
    
    print(f"Сохранено {len(links)} ссылок")
    
    # Также сохраняем статистику
    stats = {
        "total_links": len(links),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "links_sample": links[:5] if links else []
    }
    
    with open("links_stats.json", "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

def get_links_from_sitemap():
    """Попробуем получить ссылки из карты сайта"""
    import re
    
    sitemap_urls = [
        "https://pub.fsa.gov.ru/sitemap.xml",
        "https://pub.fsa.gov.ru/ral/sitemap",
    ]
    
    all_links = []
    
    for sitemap_url in sitemap_urls:
        try:
            response = requests.get(sitemap_url, timeout=10)
            if response.status_code == 200:
                # Ищем ссылки на компании в тексте
                pattern = r'/ral/view/(\d+)/current-aa'
                found_links = re.findall(pattern, response.text)
                
                for company_id in found_links:
                    link = f"https://pub.fsa.gov.ru/ral/view/{company_id}/current-aa"
                    all_links.append(link)
                
                print(f"Из {sitemap_url} найдено {len(found_links)} ссылок")
        except Exception as e:
            print(f"Ошибка при парсинге {sitemap_url}: {str(e)}")
    
    return all_links

def main():
    """Основная функция"""
    print("=" * 70)
    print("АЛЬТЕРНАТИВНЫЕ МЕТОДЫ СБОРА ССЫЛОК")
    print("=" * 70)
    
    print("\n1. Попробуем найти sitemap...")
    links = get_links_from_sitemap()
    
    if not links:
        print("Не удалось найти ссылки через sitemap")
        print("\n2. Пробуем перебор ID...")
        links = get_companies_by_ranges()
    
    if links:
        save_links(links)
        print(f"\n✓ Успешно собрано {len(links)} ссылок")
        print(f"✓ Файл: all_links.txt")
        
        # Создаем файл с первыми 1000 ссылок для быстрого теста
        if len(links) > 1000:
            with open("test_1000_links.txt", "w", encoding="utf-8") as f:
                for link in links[:1000]:
                    f.write(f"{link}\n")
            print(f"✓ Тестовый файл: test_1000_links.txt")
    else:
        print("\n✗ Не удалось собрать ссылки")

if __name__ == "__main__":
    main()
