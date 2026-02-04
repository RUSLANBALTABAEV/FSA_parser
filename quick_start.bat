@echo off
chcp 65001 >nul
cls

echo ========================================
echo    ПАРСЕР FSA - БЫСТРЫЙ СТАРТ
echo ========================================
echo.

echo 1. Создаем необходимые файлы...

echo # Получение токена для FSA> get_token.py
echo import webbrowser>> get_token.py
echo import time>> get_token.py
echo import json>> get_token.py
echo import os>> get_token.py
echo.>> get_token.py
echo def get_token_manually():>> get_token.py
echo     print("=" * 60)>> get_token.py
echo     print("ИНСТРУКЦИЯ ПО ПОЛУЧЕНИЮ ТОКЕНА:")>> get_token.py
echo     print("=" * 60)>> get_token.py
echo     print("\n1. Браузер откроется автоматически...")>> get_token.py
echo     webbrowser.open("https://pub.fsa.gov.ru/ral")>> get_token.py
echo     print("2. Нажмите F12 (инструменты разработчика)")>> get_token.py
echo     print("3. Вкладка 'Network' (Сеть)")>> get_token.py
echo     print("4. Нажмите F5 (обновить страницу)")>> get_token.py
echo     print("5. Найдите запрос к API (содержит /api/v1/)")>> get_token.py
echo     print("6. Кликните на запрос")>> get_token.py
echo     print("7. Во вкладке 'Headers' найдите 'Authorization'")>> get_token.py
echo     print("8. Скопируйте весь токен (начинается с 'Bearer ...')")>> get_token.py
echo     print("9. Вставьте его ниже\n")>> get_token.py
echo     token = input("Вставьте ваш токен (с 'Bearer '): ").strip()>> get_token.py
echo     if token.startswith("Bearer "):>> get_token.py
echo         with open("config.py", "w", encoding="utf-8") as f:>> get_token.py
echo             f.write(f'TOKEN = "{token}"\n')>> get_token.py
echo         print(f"\n✅ Токен сохранен в config.py")>> get_token.py
echo     else:>> get_token.py
echo         print("\n❌ Ошибка: Токен должен начинаться с 'Bearer '")>> get_token.py
echo         get_token_manually()>> get_token.py
echo.>> get_token.py
echo if __name__ == "__main__":>> get_token.py
echo     get_token_manually()>> get_token.py

echo # Парсер ссылок> collect_links_simple.py
echo import asyncio>> collect_links_simple.py
echo import aiohttp>> collect_links_simple.py
echo import time>> collect_links_simple.py
echo.>> collect_links_simple.py
echo def get_token():>> collect_links_simple.py
echo     try:>> collect_links_simple.py
echo         from config import TOKEN>> collect_links_simple.py
echo         return TOKEN>> collect_links_simple.py
echo     except:>> collect_links_simple.py
echo         print("ОШИБКА: Создайте config.py с токеном!")>> collect_links_simple.py
echo         return None>> collect_links_simple.py
echo.>> collect_links_simple.py
echo async def main():>> collect_links_simple.py
echo     token = get_token()>> collect_links_simple.py
echo     if not token:>> collect_links_simple.py
echo         return>> collect_links_simple.py
echo.>> collect_links_simple.py
echo     headers = {"Authorization": token}>> collect_links_simple.py
echo     async with aiohttp.ClientSession(headers=headers) as session:>> collect_links_simple.py
echo         url = "https://pub.fsa.gov.ru/api/v1/ral/common/companies?page=0&size=100">> collect_links_simple.py
echo         async with session.get(url) as response:>> collect_links_simple.py
echo             print(f"Статус: {response.status}")>> collect_links_simple.py
echo             if response.status == 200:>> collect_links_simple.py
echo                 data = await response.json()>> collect_links_simple.py
echo                 print(f"Найдено компаний: {data.get('totalElements', 0)}")>> collect_links_simple.py
echo.>> collect_links_simple.py
echo if __name__ == "__main__":>> collect_links_simple.py
echo     asyncio.run(main())>> collect_links_simple.py

echo.
echo 2. Запускаем получение токена...
python get_token.py

echo.
echo 3. Проверяем токен...
python collect_links_simple.py

echo.
if exist "config.py" (
    echo 4. Запускаем сбор всех ссылок...
    echo # Основной сборщик ссылок> collect_all.py
    echo import asyncio>> collect_all.py
    echo import aiohttp>> collect_all.py
    echo import json>> collect_all.py
    echo import time>> collect_all.py
    echo from config import TOKEN>> collect_all.py
    echo.>> collect_all.py
    echo HEADERS = {"Authorization": TOKEN}>> collect_all.py
    echo.>> collect_all.py
    echo async def get_total():>> collect_all.py
    echo     async with aiohttp.ClientSession(headers=HEADERS) as session:>> collect_all.py
    echo         url = "https://pub.fsa.gov.ru/api/v1/ral/common/companies?page=0&size=1">> collect_all.py
    echo         async with session.get(url) as resp:>> collect_all.py
    echo             if resp.status == 200:>> collect_all.py
    echo                 data = await resp.json()>> collect_all.py
    echo                 return data.get("totalElements", 0)>> collect_all.py
    echo             return 0>> collect_all.py
    echo.>> collect_all.py
    echo async def get_page(page, size=100):>> collect_all.py
    echo     async with aiohttp.ClientSession(headers=HEADERS) as session:>> collect_all.py
    echo         url = f"https://pub.fsa.gov.ru/api/v1/ral/common/companies?page={page}&size={size}">> collect_all.py
    echo         async with session.get(url) as resp:>> collect_all.py
    echo             if resp.status == 200:>> collect_all.py
    echo                 return await resp.json()>> collect_all.py
    echo             return None>> collect_all.py
    echo.>> collect_all.py
    echo async def main():>> collect_all.py
    echo     print("Сбор ссылок...")>> collect_all.py
    echo     total = await get_total()>> collect_all.py
    echo     print(f"Всего компаний: {total}")>> collect_all.py
    echo.>> collect_all.py
    echo     pages = (total + 99) // 100>> collect_all.py
    echo     all_links = []>> collect_all.py
    echo.>> collect_all.py
    echo     for page in range(pages):>> collect_all.py
    echo         print(f"Страница {page+1}/{pages}")>> collect_all.py
    echo         data = await get_page(page)>> collect_all.py
    echo         if data and "content" in data:>> collect_all.py
    echo             for company in data["content"]:>> collect_all.py
    echo                 company_id = company.get("id")>> collect_all.py
    echo                 if company_id:>> collect_all.py
    echo                     link = f"https://pub.fsa.gov.ru/ral/view/{company_id}/current-aa">> collect_all.py
    echo                     all_links.append(link)>> collect_all.py
    echo.>> collect_all.py
    echo     with open("all_links.txt", "w", encoding="utf-8") as f:>> collect_all.py
    echo         for link in all_links:>> collect_all.py
    echo             f.write(f"{link}\n")>> collect_all.py
    echo.>> collect_all.py
    echo     print(f"Сохранено ссылок: {len(all_links)}")>> collect_all.py
    echo.>> collect_all.py
    echo if __name__ == "__main__":>> collect_all.py
    echo     asyncio.run(main())>> collect_all.py
    
    python collect_all.py
)

echo.
echo Готово!
echo Проверьте файлы:
echo - config.py - ваш токен
echo - all_links.txt - ссылки на компании
echo.
pause
