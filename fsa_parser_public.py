# fsa_parser_public.py - Парсер без токена
import asyncio
import aiohttp
import pandas as pd
import json
import time
import re
from datetime import datetime

async def parse_without_token():
    """Парсер, который не требует токена"""
    
    print("🚀 Запуск парсера без токена...")
    
    # Пробуем разные подходы
    
    # 1. Прямой доступ к данным через веб-страницу
    urls = [
        "https://pub.fsa.gov.ru/ral/registry/accredited-persons",
        "https://pub.fsa.gov.ru/api/v1/ral/public/companies",
        "https://pub.fsa.gov.ru/ral/api/companies"
    ]
    
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        for url in urls:
            print(f"📡 Пробуем URL: {url}")
            try:
                async with session.get(url, timeout=30) as response:
                    if response.status == 200:
                        content = await response.text()
                        print(f"✅ Успешно! Статус: {response.status}")
                        
                        # Пробуем найти JSON в ответе
                        if "application/json" in response.headers.get('Content-Type', ''):
                            data = await response.json()
                            print(f"📊 Найден JSON с {len(data) if isinstance(data, list) else '?'} записями")
                            return data
                        else:
                            # Ищем JavaScript данные на странице
                            print("🔍 Ищу данные на странице...")
                            # Ищем JSON в JavaScript
                            json_patterns = [
                                r'JSON\.parse\(\'([^\']+)\'\)',
                                r'var data = (\{.*?\});',
                                r'window\.__INITIAL_STATE__ = (\{.*?\});'
                            ]
                            for pattern in json_patterns:
                                matches = re.findall(pattern, content, re.DOTALL)
                                if matches:
                                    print(f"🎯 Найден паттерн: {pattern[:30]}...")
                                    try:
                                        data = json.loads(matches[0])
                                        return data
                                    except:
                                        pass
            except Exception as e:
                print(f"❌ Ошибка: {e}")
    
    print("⚠️ Не удалось получить данные")
    return None

async def main():
    data = await parse_without_token()
    if data:
        print(f"✅ Данные получены!")
        # Сохраняем
        with open("data_from_website.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print("📁 Сохранено в data_from_website.json")
    else:
        print("❌ Не удалось получить данные")

if __name__ == "__main__":
    asyncio.run(main())
