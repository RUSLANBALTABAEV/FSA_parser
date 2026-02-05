# get_token.py
import requests
import json
from urllib.parse import quote

def get_fsa_token():
    """Автоматическое получение токена ФСА"""
    
    # URL для входа (если требуется)
    login_url = "https://pub.fsa.gov.ru/login"
    
    # Параметры запроса для получения токена
    params = {
        "client_id": "ral-public",
        "redirect_uri": "https://pub.fsa.gov.ru/ral",
        "response_type": "token",
        "scope": "openid",
        "state": "12345"
    }
    
    # Заголовки как в браузере
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0"
    }
    
    try:
        # Делаем запрос к главной странице
        print("📡 Запрос к главной странице...")
        session = requests.Session()
        response = session.get("https://pub.fsa.gov.ru/ral", headers=headers, timeout=30, verify=False)
        
        if response.status_code == 200:
            print(f"✅ Главная страница загружена")
            
            # Ищем токен в JavaScript на странице
            if "access_token" in response.text:
                print("🎯 Токен найден в ответе")
                # Извлекаем токен из текста
                import re
                token_match = re.search(r'access_token["\']?\s*:\s*["\']([^"\']+)["\']', response.text)
                if token_match:
                    token = token_match.group(1)
                    if token.startswith("Bearer "):
                        return token
                    else:
                        return f"Bearer {token}"
            
            # Ищем в заголовках
            for header in response.headers:
                if "token" in header.lower() or "auth" in header.lower():
                    print(f"🔍 Найден заголовок: {header}: {response.headers[header][:50]}...")
        
        # Пробуем получить через OAuth
        print("\n🔐 Пробуем получить токен через OAuth...")
        oauth_url = "https://pub.fsa.gov.ru/oauth/authorize"
        response = session.get(oauth_url, params=params, headers=headers, timeout=30, verify=False)
        
        if response.status_code == 200:
            print("✅ OAuth страница доступна")
            # Проверяем редирект URL на наличие токена
            final_url = str(response.url)
            if "#access_token=" in final_url:
                token = final_url.split("#access_token=")[1].split("&")[0]
                return f"Bearer {token}"
        
        print("\n❌ Не удалось получить токен автоматически")
        print("📋 Попробуйте вручную:")
        print("1. Откройте https://pub.fsa.gov.ru/ral")
        print("2. F12 → Network → Ищите запросы к /api/")
        print("3. Скопируйте заголовок Authorization")
        
        return None
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

if __name__ == "__main__":
    token = get_fsa_token()
    if token:
        print(f"\n✅ Получен токен:")
        print(f"{token[:80]}...")
        print(f"\n📝 Скопируйте его в файл fsa_parser_fixed.py:")
        print(f"Замените строку: auth_token: str = \"Bearer ...\"")
        print(f"На: auth_token: str = \"{token[:50]}...\"")
    else:
        print("\n❌ Токен не получен")
