"""
ПРОВЕРКА РАБОТОСПОСОБНОСТИ ПРОКСИ
"""
import requests
import time

PROXY = "http://gZYKDm:VaRKoM@193.31.102.226:9435"
TEST_URL = "https://pub.fsa.gov.ru"

def test_proxy():
    """Тестирует прокси"""
    proxies = {
        "http": PROXY,
        "https": PROXY,
    }
    
    print(f"Тестирую прокси: {PROXY[:50]}...")
    print(f"Тестовый URL: {TEST_URL}")
    print("-" * 50)
    
    try:
        # Отключаем проверку SSL для теста
        start_time = time.time()
        
        response = requests.get(
            TEST_URL,
            proxies=proxies,
            timeout=10,
            verify=False
        )
        
        elapsed = time.time() - start_time
        
        print(f"✓ Прокси РАБОТАЕТ!")
        print(f"  Статус: {response.status_code}")
        print(f"  Время ответа: {elapsed:.2f} сек")
        print(f"  Размер ответа: {len(response.text) / 1024:.1f} КБ")
        
        # Проверяем, что это действительно сайт FSA
        if "fsa" in response.text.lower():
            print("  ✓ Контент похож на FSA")
        else:
            print("  ⚠ Контент не похож на FSA")
        
        return True
    
    except requests.exceptions.ProxyError as e:
        print(f"✗ Ошибка прокси: {str(e)}")
        print("  Проверьте:")
        print("  1. Правильность IP и порта")
        print("  2. Правильность логина и пароля")
        print("  3. Работает ли прокси-сервер")
        return False
    
    except requests.exceptions.ConnectTimeout:
        print("✗ Таймаут подключения")
        print("  Прокси не отвечает или заблокирован")
        return False
    
    except requests.exceptions.SSLError:
        print("✗ Ошибка SSL")
        print("  Попробуйте отключить проверку SSL")
        return False
    
    except Exception as e:
        print(f"✗ Неизвестная ошибка: {str(e)}")
        return False

def test_without_proxy():
    """Тестирует без прокси для сравнения"""
    print("\n" + "-" * 50)
    print("Тест БЕЗ прокси для сравнения:")
    
    try:
        start_time = time.time()
        response = requests.get(TEST_URL, timeout=10, verify=False)
        elapsed = time.time() - start_time
        
        print(f"  Статус: {response.status_code}")
        print(f"  Время ответа: {elapsed:.2f} сек")
        print(f"  Размер ответа: {len(response.text) / 1024:.1f} КБ")
        
        if response.status_code == 200:
            print("  ✓ Сайт доступен без прокси")
        else:
            print(f"  ⚠ Сайт отвечает с кодом {response.status_code}")
    
    except Exception as e:
        print(f"  ✗ Ошибка без прокси: {str(e)}")
        print("  Сайт может быть заблокирован в вашем регионе")

if __name__ == "__main__":
    print("=" * 50)
    print("ТЕСТ ПРОКСИ ДЛЯ FSA ПАРСЕРА")
    print("=" * 50)
    
    success = test_proxy()
    test_without_proxy()
    
    print("\n" + "=" * 50)
    if success:
        print("✓ Прокси готов к использованию!")
        print("Запускайте парсер с настройками прокси")
    else:
        print("✗ Прокси не работает")
        print("Проверьте настройки или используйте другой прокси")
