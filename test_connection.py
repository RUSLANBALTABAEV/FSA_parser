"""
ПРОВЕРКА ДОСТУПНОСТИ САЙТА FSA
Проверяет различные методы доступа к сайту
"""
import socket
import requests
import time
import json
from datetime import datetime
from config import BASE_URL

def check_dns():
    """Проверяет разрешение DNS"""
    print("Проверка DNS...")
    
    try:
        ip_address = socket.gethostbyname('pub.fsa.gov.ru')
        print(f"✓ DNS разрешен: {ip_address}")
        return True
    except socket.gaierror as e:
        print(f"✗ DNS ошибка: {e}")
        return False

def check_ping():
    """Проверяет пинг"""
    print("\nПроверка ping...")
    
    try:
        import subprocess
        import platform
        
        # Параметр ping зависит от ОС
        param = '-n' if platform.system().lower() == 'windows' else '-c'
        
        # Выполняем ping
        command = ['ping', param, '3', 'pub.fsa.gov.ru']
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✓ Ping успешен")
            print(result.stdout)
            return True
        else:
            print("✗ Ping не удался")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"✗ Ошибка ping: {e}")
        return False

def check_ports():
    """Проверяет открытые порты"""
    print("\nПроверка портов...")
    
    ports = [80, 443, 8080, 8443]
    open_ports = []
    
    for port in ports:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        
        try:
            result = sock.connect_ex(('pub.fsa.gov.ru', port))
            if result == 0:
                print(f"✓ Порт {port} открыт")
                open_ports.append(port)
            else:
                print(f"✗ Порт {port} закрыт")
        except Exception as e:
            print(f"✗ Ошибка проверки порта {port}: {e}")
        finally:
            sock.close()
    
    return open_ports

def check_http():
    """Проверяет HTTP/HTTPS доступность"""
    print("\nПроверка HTTP/HTTPS...")
    
    protocols = [
        ("HTTP", "http://pub.fsa.gov.ru"),
        ("HTTPS", "https://pub.fsa.gov.ru"),
        ("API", "https://pub.fsa.gov.ru/api/v1/ral/common/companies?size=1"),
    ]
    
    available = []
    
    for name, url in protocols:
        try:
            start_time = time.time()
            response = requests.head(url, timeout=5, allow_redirects=True)
            elapsed = time.time() - start_time
            
            print(f"{name}:")
            print(f"  Статус: {response.status_code}")
            print(f"  Время: {elapsed:.2f} сек")
            
            if response.status_code == 200:
                print(f"  ✓ Доступен")
                available.append((name, url, response.status_code, elapsed))
            else:
                print(f"  ✗ Недоступен")
                
        except requests.exceptions.Timeout:
            print(f"{name}:")
            print(f"  ✗ Таймаут (5 секунд)")
        except requests.exceptions.ConnectionError as e:
            print(f"{name}:")
            print(f"  ✗ Ошибка соединения: {e}")
        except Exception as e:
            print(f"{name}:")
            print(f"  ✗ Ошибка: {e}")
    
    return available

def check_with_proxies():
    """Проверяет доступность через прокси"""
    print("\nПроверка через прокси...")
    
    # Список публичных прокси (обновляемый)
    proxies = [
        {"http": "http://138.197.157.32:8080", "https": "http://138.197.157.32:8080"},
        {"http": "http://209.141.55.228:80", "https": "http://209.141.55.228:80"},
        {"http": "http://64.225.8.82:9999", "https": "http://64.225.8.82:9999"},
    ]
    
    working_proxies = []
    
    for proxy in proxies:
        try:
            print(f"Пробую прокси: {list(proxy.values())[0]}")
            
            start_time = time.time()
            response = requests.get(
                "https://pub.fsa.gov.ru",
                proxies=proxy,
                timeout=10
            )
            elapsed = time.time() - start_time
            
            if response.status_code == 200:
                print(f"  ✓ Работает ({elapsed:.2f} сек)")
                working_proxies.append(proxy)
            else:
                print(f"  ✗ Ошибка: {response.status_code}")
                
        except Exception as e:
            print(f"  ✗ Не работает: {str(e)}")
    
    return working_proxies

def check_vpn_services():
    """Проверяет доступные VPN сервисы"""
    print("\nДоступные VPN сервисы (бесплатные):")
    
    vpn_services = [
        {
            "name": "Windscribe",
            "free_tier": True,
            "russian_servers": True,
            "link": "https://windscribe.com",
            "notes": "10 ГБ/мес бесплатно, есть серверы в России"
        },
        {
            "name": "ProtonVPN",
            "free_tier": True,
            "russian_servers": False,
            "link": "https://protonvpn.com",
            "notes": "Бесплатно, но нет российских серверов"
        },
        {
            "name": "Hide.me",
            "free_tier": True,
            "russian_servers": True,
            "link": "https://hide.me",
            "notes": "10 ГБ/мес бесплатно"
        },
        {
            "name": "TunnelBear",
            "free_tier": True,
            "russian_servers": False,
            "link": "https://tunnelbear.com",
            "notes": "500 МБ/мес бесплатно"
        },
        {
            "name": "Hotspot Shield",
            "free_tier": True,
            "russian_servers": False,
            "link": "https://hotspotshield.com",
            "notes": "Базовый бесплатный план"
        },
    ]
    
    for vpn in vpn_services:
        status = "✓" if vpn["russian_servers"] else "⚠"
        print(f"{status} {vpn['name']}: {vpn['notes']}")

def generate_alternative_urls():
    """Генерирует альтернативные URL для проверки"""
    print("\nАльтернативные URL для проверки:")
    
    base_urls = [
        "http://pub.fsa.gov.ru",        # HTTP вместо HTTPS
        "https://www.fsa.gov.ru",       # www версия
        "http://fsa.gov.ru",            # Без pub
        "https://reestr.fsa.gov.ru",    # Возможное зеркало
        "http://195.208.109.51",        # Прямой IP (пример)
    ]
    
    for url in base_urls:
        print(f"  {url}")

def save_test_results(results):
    """Сохраняет результаты тестирования"""
    filename = f"connection_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\nРезультаты сохранены в: {filename}")
    return filename

def main():
    """Основная функция проверки"""
    print("=" * 70)
    print("ТЕСТИРОВАНИЕ ДОСТУПНОСТИ FSA.GOV.RU")
    print("=" * 70)
    
    results = {
        "test_date": datetime.now().isoformat(),
        "target_url": BASE_URL,
        "checks": {}
    }
    
    # Выполняем все проверки
    results["checks"]["dns"] = check_dns()
    results["checks"]["ping"] = check_ping()
    results["checks"]["ports"] = check_ports()
    results["checks"]["http"] = check_http()
    results["checks"]["proxies"] = check_with_proxies()
    
    # Дополнительная информация
    check_vpn_services()
    generate_alternative_urls()
    
    # Сохранение результатов
    results_file = save_test_results(results)
    
    # Рекомендации
    print("\n" + "=" * 70)
    print("РЕКОМЕНДАЦИИ:")
    print("=" * 70)
    
    if not results["checks"]["dns"]:
        print("1. Проблема с DNS:")
        print("   - Измените DNS на Google (8.8.8.8) или Cloudflare (1.1.1.1)")
        print("   - Используйте VPN для обхода DNS блокировки")
    
    if not results["checks"]["http"]:
        print("\n2. Сайт недоступен по HTTP/HTTPS:")
        print("   - Используйте VPN с российскими серверами")
        print("   - Попробуйте публичные прокси из списка выше")
        print("   - Проверьте, не заблокирован ли сайт вашим провайдером")
    
    if results["checks"]["proxies"]:
        print(f"\n3. Найдено рабочих прокси: {len(results['checks']['proxies'])}")
        print("   Используйте их в настройках парсера (файл config.py)")
    
    print("\n" + "=" * 70)
    print("БЫСТРОЕ РЕШЕНИЕ:")
    print("=" * 70)
    print("1. Установите Windscribe VPN (бесплатно)")
    print("2. Подключитесь к серверу в России")
    print("3. Запустите generate_links.py и затем parser.py")
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nТестирование прервано пользователем")
    except Exception as e:
        print(f"\n\nОшибка тестирования: {str(e)}")
        import traceback
        traceback.print_exc()
