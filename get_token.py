"""
СКРИПТ ДЛЯ ПОЛУЧЕНИЯ ТОКЕНА ВРУЧНУЮ
Запустите этот файл первым, чтобы получить токен
"""
import webbrowser
import time
import requests
import json

def get_token_manually():
    """Пошаговая инструкция для получения токена"""
    print("=" * 70)
    print("ПОЛУЧЕНИЕ ТОКЕНА ДЛЯ FSA API")
    print("=" * 70)
    
    print("\n1. Сейчас откроется браузер...")
    time.sleep(2)
    webbrowser.open("https://pub.fsa.gov.ru/ral")
    
    print("2. После загрузки страницы нажмите F12 (инструменты разработчика)")
    print("3. Перейдите на вкладку 'Network' (Сеть)")
    print("4. Нажмите F5 (обновить страницу)")
    print("5. В списке запросов найдите любой запрос к API (содержит '/api/v1/')")
    print("6. Кликните на этот запрос")
    print("7. Во вкладке 'Headers' найдите заголовок 'Authorization'")
    print("8. Скопируйте ВЕСЬ текст (начинается с 'Bearer ey...')")
    print("\n" + "=" * 70)
    
    token = input("Вставьте скопированный токен: ").strip()
    
    if not token.startswith("Bearer "):
        print("ОШИБКА: Токен должен начинаться с 'Bearer '!")
        return None
    
    # Проверяем токен
    print("\nПроверяю токен...")
    headers = {"Authorization": token}
    
    try:
        response = requests.get(
            "https://pub.fsa.gov.ru/api/v1/ral/common/companies?page=0&size=1",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            total_companies = data.get("totalElements", 0)
            print(f"✓ Токен рабочий!")
            print(f"✓ Найдено компаний в реестре: {total_companies:,}")
            
            # Сохраняем токен
            with open("config.py", "w", encoding="utf-8") as f:
                f.write(f'TOKEN = "{token}"\n')
                f.write('BASE_URL = "https://pub.fsa.gov.ru"\n')
                f.write('API_BASE = "https://pub.fsa.gov.ru/api/v1"\n')
            
            print(f"\n✓ Токен сохранен в config.py")
            return token
            
        elif response.status_code == 401:
            print("✗ Токен недействителен (ошибка 401)")
            return None
        else:
            print(f"✗ Ошибка: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Ошибка соединения: {str(e)}")
        return None

def main():
    """Основная функция"""
    print("Этот скрипт поможет получить токен для доступа к API FSA")
    print("Без токена основной парсер не будет работать!")
    print("\nНажмите Enter для продолжения...")
    input()
    
    token = get_token_manually()
    
    if token:
        print("\n" + "=" * 70)
        print("ТОКЕН УСПЕШНО ПОЛУЧЕН!")
        print("Теперь запустите main.py для парсинга данных")
        print("=" * 70)
    else:
        print("\nНе удалось получить токен. Попробуйте снова.")

if __name__ == "__main__":
    main()
