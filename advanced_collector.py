# advanced_collector.py
import requests
import json
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys

class FSALinkCollector:
    def __init__(self):
        self.all_links = []
        self.base_url = "https://pub.fsa.gov.ru"
        
        # Попробуем разные User-Agent
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/537.36"
        ]
    
    def get_headers(self):
        """Возвращает случайные заголовки"""
        return {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    
    def check_company_exists(self, company_id):
        """Проверяет существование компании по ID"""
        url = f"{self.base_url}/ral/view/{company_id}/current-aa"
        
        try:
            headers = self.get_headers()
            response = requests.get(url, headers=headers, timeout=5)
            
            # Если страница загружается успешно и не является ошибкой 404
            if response.status_code == 200:
                # Дополнительная проверка - ищем ключевые слова на странице
                if "реестр аккредитованных лиц" in response.text.lower():
                    return True
            return False
        except:
            return False
    
    def search_by_inn(self):
        """Поиск компаний по ИНН (генерирует ИНН и проверяет)"""
        print("Поиск компаний по ИНН...")
        
        # Генерируем ИНН для тестирования
        test_inns = []
        
        # Формируем ИНН для юридических лиц (10 цифр)
        for i in range(1000000000, 1000001000):
            test_inns.append(str(i))
        
        found_links = []
        
        for inn in tqdm(test_inns[:100]):  # Ограничим для теста
            search_url = f"{self.base_url}/search?query={inn}"
            
            try:
                response = requests.get(search_url, headers=self.get_headers(), timeout=5)
                if response.status_code == 200:
                    # Парсим ссылки на компании из результатов поиска
                    if "/ral/view/" in response.text:
                        # Упрощенный парсинг
                        import re
                        company_ids = re.findall(r'/ral/view/(\d+)/', response.text)
                        for company_id in set(company_ids):
                            link = f"{self.base_url}/ral/view/{company_id}/current-aa"
                            if link not in found_links:
                                found_links.append(link)
            except:
                continue
        
        return found_links
    
    def smart_id_scan(self):
        """Умное сканирование ID компаний"""
        print("Умное сканирование ID...")
        
        # Определяем диапазон на основе известных ID
        known_ids = [38453, 38454, 38455]
        if known_ids:
            min_id = min(known_ids)
            max_id = max(known_ids)
            
            # Расширяем диапазон
            start_id = max(1, min_id - 10000)
            end_id = max_id + 100000
            
            print(f"Сканирую ID с {start_id} до {end_id}")
            
            found_links = []
            
            # Используем многопоточность для ускорения
            def check_id(company_id):
                if self.check_company_exists(company_id):
                    return f"{self.base_url}/ral/view/{company_id}/current-aa"
                return None
            
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = {}
                for company_id in range(start_id, end_id + 1, 10):  # Шаг 10 для скорости
                    future = executor.submit(check_id, company_id)
                    futures[future] = company_id
                
                for future in tqdm(as_completed(futures), total=len(futures)):
                    result = future.result()
                    if result:
                        found_links.append(result)
            
            return found_links
        
        return []
    
    def collect_from_api_alternative(self):
        """Альтернативный подход к API"""
        print("Пробуем альтернативные API эндпоинты...")
        
        api_endpoints = [
            f"{self.base_url}/api/v1/ral/common/companies?size=1000",
            f"{self.base_url}/api/v1/ral/companies",
            f"{self.base_url}/api/companies",
            f"{self.base_url}/ral/api/companies",
        ]
        
        all_links = []
        
        for endpoint in api_endpoints:
            print(f"Пробую: {endpoint}")
            
            try:
                response = requests.get(endpoint, headers=self.get_headers(), timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    
                    # Пробуем разные форматы ответа
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict) and 'id' in item:
                                link = f"{self.base_url}/ral/view/{item['id']}/current-aa"
                                all_links.append(link)
                    elif isinstance(data, dict):
                        if 'content' in data:
                            for item in data['content']:
                                if 'id' in item:
                                    link = f"{self.base_url}/ral/view/{item['id']}/current-aa"
                                    all_links.append(link)
                        elif 'data' in data:
                            for item in data['data']:
                                if 'id' in item:
                                    link = f"{self.base_url}/ral/view/{item['id']}/current-aa"
                                    all_links.append(link)
                    
                    print(f"Найдено {len(all_links)} ссылок")
            except Exception as e:
                print(f"Ошибка: {str(e)}")
                continue
        
        return all_links
    
    def run(self):
        """Запускает все методы сбора"""
        print("=" * 70)
        print("ПРОДВИНУТЫЙ СБОРЩИК ССЫЛОК FSA")
        print("=" * 70)
        
        methods = [
            ("Альтернативные API", self.collect_from_api_alternative),
            ("Умное сканирование ID", self.smart_id_scan),
            ("Поиск по ИНН", self.search_by_inn),
        ]
        
        all_unique_links = []
        
        for method_name, method_func in methods:
            print(f"\n▶ Метод: {method_name}")
            links = method_func()
            
            # Добавляем только уникальные ссылки
            for link in links:
                if link not in all_unique_links:
                    all_unique_links.append(link)
            
            print(f"Найдено новых ссылок: {len(links)}")
            print(f"Всего уникальных ссылок: {len(all_unique_links)}")
            
            # Сохраняем промежуточные результаты
            if all_unique_links:
                self.save_results(all_unique_links)
            
            # Пауза между методами
            time.sleep(2)
        
        return all_unique_links
    
    def save_results(self, links):
        """Сохраняет результаты"""
        output_files = [
            "all_links.txt",
            f"links_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        ]
        
        for filename in output_files:
            with open(filename, "w", encoding="utf-8") as f:
                for link in links:
                    f.write(f"{link}\n")
        
        print(f"✓ Сохранено {len(links)} ссылок в {output_files[0]}")
        
        # Сохраняем статистику
        stats = {
            "total_links": len(links),
            "collection_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "first_10_links": links[:10],
            "last_10_links": links[-10:] if len(links) > 10 else links
        }
        
        with open("collection_stats.json", "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

def main():
    """Основная функция"""
    collector = FSALinkCollector()
    links = collector.run()
    
    print("\n" + "=" * 70)
    print("ИТОГИ СБОРА:")
    print("=" * 70)
    print(f"Всего собрано ссылок: {len(links)}")
    
    if links:
        print(f"\nПримеры найденных ссылок:")
        for i, link in enumerate(links[:5], 1):
            print(f"  {i}. {link}")
        
        print(f"\n✓ Результаты сохранены в all_links.txt")
        print(f"✓ Статистика сохранена в collection_stats.json")
    else:
        print("✗ Не удалось собрать ссылки")

if __name__ == "__main__":
    main()
