"""
ТЕСТОВЫЙ СКРИПТ ДЛЯ ПРОВЕРКИ API FSA
"""
import aiohttp
import asyncio
import json
import sys
from config import TOKEN, API_BASE, BASE_URL

async def test_api():
    """Тестирование API"""
    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": TOKEN,
    }
    
    # Проверяем базовый доступ
    print("=" * 70)
    print("ТЕСТИРОВАНИЕ API FSA")
    print("=" * 70)
    print(f"Токен: {TOKEN[:50]}...")
    print(f"Базовая ссылка: {BASE_URL}")
    print(f"API: {API_BASE}")
    print("=" * 70)
    
    async with aiohttp.ClientSession(headers=headers) as session:
        # Тест 1: Проверка доступности API
        url = f"{API_BASE}/ral/common/companies?page=0&size=1"
        print(f"\n1. Запрос: {url}")
        
        try:
            async with session.get(url) as response:
                print(f"   Статус: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    print(f"   Всего компаний: {data.get('totalElements', 0):,}")
                    print(f"   Страниц: {data.get('totalPages', 0)}")
                    print(f"   Размер страницы: {data.get('size', 0)}")
                    
                    # Выводим первую компанию для примера
                    if 'content' in data and data['content']:
                        company = data['content'][0]
                        print(f"\n   Пример компании:")
                        print(f"   - ID: {company.get('id')}")
                        print(f"   - Название: {company.get('fullName', 'Нет')}")
                        print(f"   - ИНН: {company.get('inn', 'Нет')}")
                elif response.status == 401:
                    print("   ОШИБКА 401: Неверный токен!")
                elif response.status == 403:
                    print("   ОШИБКА 403: Доступ запрещен!")
                else:
                    text = await response.text()
                    print(f"   Ответ: {text[:200]}")
                    
        except Exception as e:
            print(f"   Ошибка соединения: {str(e)}")
        
        # Тест 2: Получение нескольких компаний
        print(f"\n2. Запрос 10 компаний:")
        url = f"{API_BASE}/ral/common/companies?page=0&size=10"
        
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    companies = data.get('content', [])
                    print(f"   Получено компаний: {len(companies)}")
                    
                    for i, company in enumerate(companies[:5], 1):
                        print(f"   {i}. ID: {company.get('id')}, Название: {company.get('fullName', 'Нет')[:50]}")
                else:
                    print(f"   Ошибка: {response.status}")
        except Exception as e:
            print(f"   Ошибка: {str(e)}")

async def test_individual_company():
    """Тест получения данных отдельной компании"""
    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": TOKEN,
    }
    
    # ID компании для теста (возьмите из вашего файла all_links.txt)
    test_ids = [38453, 38454, 38455]
    
    async with aiohttp.ClientSession(headers=headers) as session:
        for company_id in test_ids:
            url = f"{API_BASE}/ral/common/companies/{company_id}"
            print(f"\nЗапрос компании ID={company_id}:")
            print(f"URL: {url}")
            
            try:
                async with session.get(url) as response:
                    print(f"   Статус: {response.status}")
                    
                    if response.status == 200:
                        data = await response.json()
                        print(f"   Название: {data.get('fullName', 'Нет')}")
                        print(f"   ИНН: {data.get('inn', 'Нет')}")
                        print(f"   Статус: {data.get('status', {}).get('name', 'Нет')}")
                    else:
                        text = await response.text()
                        print(f"   Ошибка: {text[:200]}")
            except Exception as e:
                print(f"   Ошибка: {str(e)}")

if __name__ == "__main__":
    # Проверяем конфигурацию
    if TOKEN == "Bearer ВАШ_ТОКЕН" or "ВСТАВЬТЕ_СЮДА_ВАШ_ТОКЕН" in TOKEN:
        print("ОШИБКА: Установите правильный токен в config.py!")
        sys.exit(1)
    
    asyncio.run(test_api())
    # asyncio.run(test_individual_company())
