import aiohttp
import asyncio
import time

async def test_connection():
    url = "https://pub.fsa.gov.ru"
    
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, ssl=False) as response:
                print(f"Статус: {response.status}")
                print(f"Заголовки: {response.headers}")
                
                # Читаем небольшой фрагмент
                content = await response.text(encoding='utf-8')
                print(f"Первые 500 символов: {content[:500]}")
                
                return True
    except Exception as e:
        print(f"Ошибка: {type(e).__name__}: {e}")
        return False

async def main():
    print("Тестирую подключение к FSA...")
    success = await test_connection()
    
    if success:
        print("\n✓ Подключение успешно!")
    else:
        print("\n✗ Не удалось подключиться")

if __name__ == "__main__":
    asyncio.run(main())
