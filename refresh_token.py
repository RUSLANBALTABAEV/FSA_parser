# refresh_token.py
import requests
import webbrowser
import json
import time

def get_new_token():
    """Получает новый токен автоматически"""
    print("Открываю страницу для получения токена...")
    webbrowser.open("https://pub.fsa.gov.ru/ral")
    
    print("\n1. Дождитесь полной загрузки страницы")
    print("2. Нажмите F12 -> Network (Сеть)")
    print("3. Нажмите F5 для обновления страницы")
    print("4. В списке запросов найдите любой к API")
    print("5. Кликните на него и скопируйте значение 'Authorization' из Headers")
    print("\nВставьте токен ниже (должен начинаться с 'Bearer '):")
    
    token = input().strip()
    
    if not token.startswith("Bearer "):
        print("ОШИБКА: Токен должен начинаться с 'Bearer '")
        return None
    
    # Проверяем токен
    headers = {"Authorization": token}
    test_url = "https://pub.fsa.gov.ru/api/v1/ral/common/companies?page=0&size=10"
    
    try:
        response = requests.get(test_url, headers=headers, timeout=10)
        print(f"Статус проверки: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            total = data.get("totalElements", 0)
            print(f"✓ Токен рабочий! Найдено компаний: {total:,}")
            
            # Сохраняем токен
            with open("config.py", "w", encoding="utf-8") as f:
                f.write(f'TOKEN = "{token}"\n')
                f.write('BASE_URL = "https://pub.fsa.gov.ru"\n')
                f.write('API_BASE = "https://pub.fsa.gov.ru/api/v1"\n')
            
            return token
        else:
            print(f"✗ Токен недействителен. Статус: {response.status_code}")
            return None
    except Exception as e:
        print(f"Ошибка при проверке: {str(e)}")
        return None

if __name__ == "__main__":
    token = get_new_token()
    if token:
        print(f"\nНовый токен сохранен в config.py")
        print(f"Токен (первые 50 символов): {token[:50]}...")
