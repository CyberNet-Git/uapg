"""
Пример использования DatabaseManager для управления базой данных OPC UA History.

Этот скрипт демонстрирует все основные возможности модуля DatabaseManager:
1. Создание базы данных
2. Управление конфигурацией
3. Миграция схемы
4. Резервное копирование
5. Очистка данных
"""

import asyncio
import logging
import sys
from pathlib import Path

# Добавляем путь к модулю uapg
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from uapg.db_manager import DatabaseManager, create_database_standalone, backup_database_standalone


async def main():
    """Основная функция демонстрации."""
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== Демонстрация DatabaseManager ===\n")
    
    # 1. Создание экземпляра DatabaseManager
    print("1. Создание DatabaseManager...")
    master_password = "my_secure_master_password_123"
    db_manager = DatabaseManager(master_password)
    print("✓ DatabaseManager создан\n")
    
    # 2. Создание базы данных
    print("2. Создание базы данных...")
    success = await db_manager.create_database(
        user="opcua_user",
        password="opcua_password_123",
        database="opcua_history",
        host="localhost",
        port=5432,
        superuser="postgres",
        superuser_password=None,  # Для локальной установки без пароля
        enable_timescaledb=True
    )
    
    if success:
        print("✓ База данных создана успешно\n")
    else:
        print("✗ Ошибка создания базы данных\n")
        return
    
    # 3. Получение информации о базе данных
    print("3. Информация о базе данных...")
    db_info = await db_manager.get_database_info()
    print(f"   Имя БД: {db_info.get('database_name')}")
    print(f"   Хост: {db_info.get('host')}:{db_info.get('port')}")
    print(f"   Пользователь: {db_info.get('user')}")
    print(f"   Версия схемы: {db_info.get('schema_version')}")
    print(f"   Размер БД: {db_info.get('database_size')}")
    print(f"   Таблицы переменных: {db_info.get('variable_tables')}")
    print(f"   Таблицы событий: {db_info.get('event_tables')}\n")
    
    # 4. Создание резервной копии
    print("4. Создание резервной копии...")
    backup_path = await db_manager.backup_database(
        backup_path="opcua_history_backup.backup",
        backup_format="custom",
        compression=True
    )
    
    if backup_path:
        print(f"✓ Резервная копия создана: {backup_path}\n")
    else:
        print("✗ Ошибка создания резервной копии\n")
    
    # 5. Демонстрация миграции схемы
    print("5. Демонстрация миграции схемы...")
    
    # Пример скриптов миграции
    migration_scripts = [
        {
            'version': '1.1',
            'description': 'Добавление индекса для оптимизации запросов по времени',
            'sql': '''
                CREATE INDEX IF NOT EXISTS idx_variable_metadata_created_at 
                ON variable_metadata(created_at);
                
                CREATE INDEX IF NOT EXISTS idx_event_type_metadata_created_at 
                ON event_type_metadata(created_at);
            '''
        },
        {
            'version': '1.2',
            'description': 'Добавление поля для комментариев',
            'sql': '''
                ALTER TABLE variable_metadata 
                ADD COLUMN IF NOT EXISTS description TEXT;
                
                ALTER TABLE event_type_metadata 
                ADD COLUMN IF NOT EXISTS description TEXT;
            '''
        }
    ]
    
    success = await db_manager.migrate_schema('1.2', migration_scripts)
    if success:
        print("✓ Миграция схемы выполнена успешно\n")
    else:
        print("✗ Ошибка миграции схемы\n")
    
    # 6. Демонстрация очистки данных
    print("6. Демонстрация очистки данных...")
    
    # Очистка данных старше 30 дней (для демонстрации)
    success = await db_manager.cleanup_old_data(
        retention_days=30,
        node_ids=None,  # Все узлы
        event_types=None  # Все типы событий
    )
    
    if success:
        print("✓ Очистка данных выполнена успешно\n")
    else:
        print("✗ Ошибка очистки данных\n")
    
    # 7. Экспорт конфигурации
    print("7. Экспорт конфигурации...")
    success = db_manager.export_config("db_config_export.json")
    if success:
        print("✓ Конфигурация экспортирована в db_config_export.json\n")
    else:
        print("✗ Ошибка экспорта конфигурации\n")
    
    # 8. Демонстрация изменения главного пароля
    print("8. Изменение главного пароля...")
    new_master_password = "new_secure_master_password_456"
    success = db_manager.change_master_password(new_master_password)
    if success:
        print("✓ Главный пароль изменен успешно\n")
        # Обновляем пароль в экземпляре
        db_manager.master_password = new_master_password
    else:
        print("✗ Ошибка изменения главного пароля\n")
    
    # 9. Демонстрация standalone функций
    print("9. Демонстрация standalone функций...")
    
    # Создание бэкапа без экземпляра класса
    backup_path_standalone = await backup_database_standalone(
        user="opcua_user",
        password="opcua_password_123",
        database="opcua_history",
        host="localhost",
        port=5432,
        backup_path="standalone_backup.backup"
    )
    
    if backup_path_standalone:
        print(f"✓ Standalone бэкап создан: {backup_path_standalone}\n")
    else:
        print("✗ Ошибка создания standalone бэкапа\n")
    
    # 10. Получение обновленной информации
    print("10. Обновленная информация о базе данных...")
    db_info_updated = await db_manager.get_database_info()
    print(f"   Версия схемы: {db_info_updated.get('schema_version')}")
    print(f"   Размер БД: {db_info_updated.get('database_size')}\n")
    
    print("=== Демонстрация завершена ===")


async def demo_advanced_features():
    """Демонстрация дополнительных возможностей."""
    print("\n=== Демонстрация дополнительных возможностей ===\n")
    
    master_password = "demo_master_password"
    db_manager = DatabaseManager(master_password)
    
    # Установка конфигурации для демонстрации
    db_manager.config = {
        'user': 'opcua_user',
        'password': 'opcua_password_123',
        'database': 'opcua_history',
        'host': 'localhost',
        'port': 5432
    }
    
    # 1. Удаление таблиц узлов
    print("1. Демонстрация удаления таблиц узлов...")
    success = await db_manager.remove_node_tables(['ns=2;s=DemoVariable1', 'ns=2;s=DemoVariable2'])
    if success:
        print("✓ Таблицы узлов удалены успешно\n")
    else:
        print("✗ Ошибка удаления таблиц узлов\n")
    
    # 2. Полная очистка данных
    print("2. Демонстрация полной очистки данных...")
    success = await db_manager.clear_all_data()
    if success:
        print("✓ Все данные очищены успешно\n")
    else:
        print("✗ Ошибка очистки данных\n")
    
    # 3. Восстановление из бэкапа (если есть)
    backup_file = "opcua_history_backup.backup"
    if Path(backup_file).exists():
        print("3. Демонстрация восстановления из бэкапа...")
        success = await db_manager.restore_database(backup_file)
        if success:
            print("✓ База данных восстановлена из бэкапа\n")
        else:
            print("✗ Ошибка восстановления из бэкапа\n")
    else:
        print("3. Файл бэкапа не найден, пропускаем восстановление\n")
    
    print("=== Дополнительные возможности продемонстрированы ===")


if __name__ == "__main__":
    try:
        # Запуск основной демонстрации
        asyncio.run(main())
        
        # Запуск демонстрации дополнительных возможностей
        asyncio.run(demo_advanced_features())
        
    except KeyboardInterrupt:
        print("\n\nДемонстрация прервана пользователем")
    except Exception as e:
        print(f"\n\nОшибка во время демонстрации: {e}")
        logging.exception("Детали ошибки:")
