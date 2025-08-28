"""
Пример использования HistoryPgSQL с зашифрованной конфигурацией.

Демонстрирует различные способы создания экземпляра HistoryPgSQL:
1. Из файла зашифрованной конфигурации
2. Из зашифрованной конфигурации в виде строки
3. С обновлением конфигурации на лету
"""

import asyncio
import logging
import sys
from pathlib import Path

# Добавляем путь к модулю uapg
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from uapg.history_pgsql import HistoryPgSQL
from uapg.db_manager import DatabaseManager


async def demo_from_config_file():
    """Демонстрация создания HistoryPgSQL из файла конфигурации."""
    print("=== Демонстрация создания из файла конфигурации ===\n")
    
    try:
        # Создание экземпляра из файла конфигурации
        history = HistoryPgSQL.from_config_file(
            config_file="db_config.enc",
            master_password="my_secure_password",
            min_size=2,
            max_size=20
        )
        
        print("✓ HistoryPgSQL создан из файла конфигурации")
        print(f"Параметры подключения: {history.get_connection_info()}")
        
        # Инициализация
        await history.init()
        print("✓ История инициализирована успешно")
        
        # Остановка
        await history.stop()
        print("✓ История остановлена\n")
        
    except Exception as e:
        print(f"✗ Ошибка: {e}\n")


async def demo_from_encrypted_string():
    """Демонстрация создания HistoryPgSQL из зашифрованной строки."""
    print("=== Демонстрация создания из зашифрованной строки ===\n")
    
    try:
        # Создание DatabaseManager для получения зашифрованной конфигурации
        db_manager = DatabaseManager("my_secure_password")
        
        # Получение зашифрованной конфигурации в виде строки
        if db_manager.config:
            encrypted_config = db_manager._encrypt_config(db_manager.config)
            encrypted_string = encrypted_config.decode()
            
            print("✓ Зашифрованная конфигурация получена")
            
            # Создание HistoryPgSQL из зашифрованной строки
            history = HistoryPgSQL.from_encrypted_config(
                encrypted_config=encrypted_string,
                master_password="my_secure_password"
            )
            
            print("✓ HistoryPgSQL создан из зашифрованной строки")
            print(f"Параметры подключения: {history.get_connection_info()}")
            
            # Инициализация
            await history.init()
            print("✓ История инициализирована успешно")
            
            # Остановка
            await history.stop()
            print("✓ История остановлена\n")
            
        else:
            print("✗ Конфигурация не найдена\n")
            
    except Exception as e:
        print(f"✗ Ошибка: {e}\n")


async def demo_update_config():
    """Демонстрация обновления конфигурации на лету."""
    print("=== Демонстрация обновления конфигурации ===\n")
    
    try:
        # Создание экземпляра с базовыми параметрами
        history = HistoryPgSQL(
            user="postgres",
            password="postmaster",
            database="opcua",
            host="localhost",
            port=5432
        )
        
        print("✓ HistoryPgSQL создан с базовыми параметрами")
        print(f"Параметры подключения: {history.get_connection_info()}")
        
        # Обновление конфигурации из файла
        success = history.update_config(
            config_file="db_config.enc",
            master_password="my_secure_password"
        )
        
        if success:
            print("✓ Конфигурация обновлена из файла")
            print(f"Новые параметры: {history.get_connection_info()}")
            
            # Инициализация с новой конфигурацией
            await history.init()
            print("✓ История инициализирована с новой конфигурацией")
            
            # Остановка
            await history.stop()
            print("✓ История остановлена\n")
        else:
            print("✗ Не удалось обновить конфигурацию\n")
            
    except Exception as e:
        print(f"✗ Ошибка: {e}\n")


async def demo_mixed_usage():
    """Демонстрация смешанного использования."""
    print("=== Демонстрация смешанного использования ===\n")
    
    try:
        # Создание экземпляра с приоритетом зашифрованной конфигурации
        history = HistoryPgSQL(
            user="fallback_user",
            password="fallback_password",
            database="fallback_db",
            host="localhost",
            port=5432,
            config_file="db_config.enc",
            master_password="my_secure_password"
        )
        
        print("✓ HistoryPgSQL создан с приоритетом зашифрованной конфигурации")
        print(f"Параметры подключения: {history.get_connection_info()}")
        
        # Инициализация
        await history.init()
        print("✓ История инициализирована успешно")
        
        # Остановка
        await history.stop()
        print("✓ История остановлена\n")
        
    except Exception as e:
        print(f"✗ Ошибка: {e}\n")


async def main():
    """Основная функция демонстрации."""
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Демонстрация HistoryPgSQL с зашифрованной конфигурацией\n")
    
    # Проверяем наличие файла конфигурации
    if not Path("db_config.enc").exists():
        print("⚠️  Файл db_config.enc не найден. Создайте его с помощью DatabaseManager.")
        print("Пример создания:\n")
        print("from uapg.db_manager import DatabaseManager")
        print("db_manager = DatabaseManager('my_secure_password')")
        print("await db_manager.create_database(...)")
        print("\n")
        return
    
    # Запуск демонстраций
    await demo_from_config_file()
    await demo_from_encrypted_string()
    await demo_update_config()
    await demo_mixed_usage()
    
    print("=== Демонстрация завершена ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nДемонстрация прервана пользователем")
    except Exception as e:
        print(f"\n\nОшибка во время демонстрации: {e}")
        logging.exception("Детали ошибки:")
