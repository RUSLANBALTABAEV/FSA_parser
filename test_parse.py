# test_parse.py
import requests
from bs4 import BeautifulSoup
import re

def test_parse_one():
    """Тест парсинга одной страницы"""
    test_urls = [
        "https://pub.fsa.gov.ru/rds/declaration/view/38450/application",
        "https://pub.fsa.gov.ru/rds/declaration/view/38500/application",
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    for url in test_urls:
        print(f"\nТестирую: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=15, verify=False)
            print(f"Статус: {response.status_code}")
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Проверяем наличие данных
                text = soup.get_text()
                
                # Ищем ИНН
                inn_match = re.search(r'\b\d{10}\b|\b\d{12}\b', text)
                if inn_match:
                    print(f"Найден ИНН: {inn_match.group()}")
                else:
                    print("ИНН не найден")
                
                # Проверяем название
                title = soup.find('title')
                if title:
                    print(f"Заголовок: {title.get_text(strip=True)[:100]}...")
                
                print(f"Размер страницы: {len(response.text) / 1024:.1f} КБ")
            else:
                print(f"Ошибка HTTP: {response.status_code}")
                
        except Exception as e:
            print(f"Ошибка: {e}")

if __name__ == "__main__":
    test_parse_one()
