import webbrowser
import time
import json
import os

def get_token_manually():
    """Инструкция для получения токена"""
    print("=" * 60)
    print("ИНСТРУКЦИЯ ПО ПОЛУЧЕНИЮ ТОКЕНА:")
    print("=" * 60)
    print("\n1. Браузер откроется автоматически...")
    
    # Открываем сайт
    webbrowser.open("https://pub.fsa.gov.ru/ral")
    
    print("2. Нажмите F12 (откроются инструменты разработчика)")
    print("3. Перейдите на вкладку 'Network' (Сеть)")
    print("4. Нажмите F5 (обновить страницу)")
    print("5. В списке запросов найдите любой к API (содержит /api/v1/)")
    print("6. Кликните на запрос")
    print("7. Во вкладке 'Headers' найдите 'Authorization'")
    print("8. Скопируйте весь токен (начинается с 'Bearer ...')")
    print("9. Вставьте его ниже\n")
    
    token = input("Вставьте ваш токен (с 'Bearer '): ").strip()
    
    if token.startswith("Bearer "):
        # Сохраняем в config.py
        with open("config.py", "w", encoding="utf-8") as f:
            f.write(f'TOKEN = "{token}"\n')
            f.write('BASE_URL = "https://pub.fsa.gov.ru"\n')
            f.write('API_BASE = "https://pub.fsa.gov.ru/api/v1"\n')
        
        print(f"\nТокен сохранен в config.py")
        
        # Проверяем токен
        check_token(token)
    else:
        print("\nОшибка: Токен должен начинаться с 'Bearer '")
        print("Пример правильного токена:")
        print("Bearer eyJhbGciOiJFZERTQSJ9...")
        get_token_manually()

def check_token(token):
    """Проверка валидности токена"""
    import requests
    
    headers = {
        "Authorization": token,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    print("\nПроверяю токен...")
    
    try:
        # Пробный запрос
        url = "https://pub.fsa.gov.ru/api/v1/ral/common/companies?page=0&size=1"
        response = requests.get(url, headers=headers, timeout=10)
        
        print(f"Статус ответа: {response.status_code}")
        
        if response.status_code == 200:
            print("Токен рабочий!")
            data = response.json()
            print(f"Всего компаний в реестре: {data.get('totalElements', '?')}")
        elif response.status_code == 401:
            print("Токен недействителен (ошибка 401)")
            print("Возможно, токен устарел или неверный")
            retry = input("Попробовать снова? (y/n): ")
            if retry.lower() == 'y':
                get_token_manually()
        else:
            print(f"Неожиданный статус: {response.status_code}")
            print(f"Ответ: {response.text[:200]}")
    
    except Exception as e:
        print(f"Ошибка при проверке: {str(e)}")

def main():
    """Основная функция"""
    print("ПОЛУЧЕНИЕ ТОКЕНА ДЛЯ FSA API")
    print("-" * 40)
    
    # Если config.py уже существует
    if os.path.exists("config.py"):
        print("config.py уже существует")
        choice = input("Заменить токен? (y/n): ")
        if choice.lower() != 'y':
            return
    
    get_token_manually()

if __name__ == "__main__":
    main()
