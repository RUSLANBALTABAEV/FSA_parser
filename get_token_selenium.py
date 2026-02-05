# get_token_selenium.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json

def get_token_with_selenium():
    """Получение токена через Selenium (автоматизация браузера)"""
    
    print("🌐 Запуск браузера через Selenium...")
    
    options = Options()
    options.add_argument("--headless")  # Без графического интерфейса
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        # Открываем сайт
        driver.get("https://pub.fsa.gov.ru/ral")
        print("✅ Страница загружена")
        
        # Ждем загрузки
        time.sleep(5)
        
        # Получаем куки
        cookies = driver.get_cookies()
        print(f"🍪 Получено {len(cookies)} куки")
        
        # Получаем localStorage
        local_storage = driver.execute_script("return window.localStorage;")
        print(f"💾 LocalStorage: {len(local_storage)} записей")
        
        # Ищем токен в localStorage
        for key, value in local_storage.items():
            if "token" in key.lower() or "auth" in key.lower():
                print(f"🔑 Найден токен в localStorage: {key} = {value[:50]}...")
                if value.startswith("Bearer "):
                    return value
                else:
                    return f"Bearer {value}"
        
        # Ищем в cookies
        for cookie in cookies:
            if "token" in cookie['name'].lower() or "auth" in cookie['name'].lower():
                print(f"🍪 Найден токен в куки: {cookie['name']} = {cookie['value'][:50]}...")
                return f"Bearer {cookie['value']}"
        
        # Делаем скриншот для отладки
        driver.save_screenshot("debug_screenshot.png")
        print("📸 Скриншот сохранен: debug_screenshot.png")
        
        # Получаем исходный код
        html = driver.page_source
        
        # Ищем токен в HTML
        import re
        patterns = [
            r'access_token["\']?\s*:\s*["\']([^"\']+)["\']',
            r'"token"\s*:\s*"([^"]+)"',
            r'Bearer\s+([^\s"\']+)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, html)
            if matches:
                print(f"🎯 Найден токен по паттерну: {pattern}")
                token = matches[0]
                if token.startswith("Bearer "):
                    return token
                else:
                    return f"Bearer {token}"
        
        print("❌ Токен не найден")
        return None
        
    except Exception as e:
        print(f"❌ Ошибка Selenium: {e}")
        return None
        
    finally:
        driver.quit()

if __name__ == "__main__":
    token = get_token_with_selenium()
    if token:
        print(f"\n✅ Токен: {token[:80]}...")
    else:
        print("\n❌ Токен не получен")
