"""
CLI интерфейс для DatabaseManager.

Предоставляет командную строку для основных операций управления базой данных.
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import Optional

from .db_manager import DatabaseManager


def setup_logging(verbose: bool = False) -> None:
    """Настройка логирования."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


async def cmd_create_database(args: argparse.Namespace) -> int:
    """Команда создания базы данных."""
    try:
        db_manager = DatabaseManager(args.master_password)
        
        success = await db_manager.create_database(
            user=args.user,
            password=args.password,
            database=args.database,
            host=args.host,
            port=args.port,
            superuser=args.superuser,
            superuser_password=args.superuser_password,
            enable_timescaledb=not args.no_timescaledb
        )
        
        if success:
            print(f"✓ База данных '{args.database}' создана успешно")
            return 0
        else:
            print("✗ Ошибка создания базы данных")
            return 1
            
    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        if args.verbose:
            logging.exception("Детали ошибки:")
        return 1


async def cmd_backup(args: argparse.Namespace) -> int:
    """Команда создания резервной копии."""
    try:
        db_manager = DatabaseManager(args.master_password)
        
        backup_path = await db_manager.backup_database(
            backup_path=args.output,
            backup_format=args.format,
            compression=not args.no_compression
        )
        
        if backup_path:
            print(f"✓ Резервная копия создана: {backup_path}")
            return 0
        else:
            print("✗ Ошибка создания резервной копии")
            return 1
            
    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        if args.verbose:
            logging.exception("Детали ошибки:")
        return 1


async def cmd_restore(args: argparse.Namespace) -> int:
    """Команда восстановления из резервной копии."""
    try:
        db_manager = DatabaseManager(args.master_password)
        
        success = await db_manager.restore_database(
            backup_path=args.backup_file,
            drop_existing=args.drop_existing
        )
        
        if success:
            print("✓ База данных восстановлена успешно")
            return 0
        else:
            print("✗ Ошибка восстановления базы данных")
            return 1
            
    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        if args.verbose:
            logging.exception("Детали ошибки:")
        return 1


async def cmd_migrate(args: argparse.Namespace) -> int:
    """Команда миграции схемы."""
    try:
        db_manager = DatabaseManager(args.master_password)
        
        # Загрузка скриптов миграции из файла
        if args.migration_file:
            with open(args.migration_file, 'r', encoding='utf-8') as f:
                migration_scripts = json.load(f)
        else:
            print("✗ Файл миграции не указан")
            return 1
        
        success = await db_manager.migrate_schema(
            target_version=args.target_version,
            migration_scripts=migration_scripts
        )
        
        if success:
            print(f"✓ Миграция к версии {args.target_version} выполнена успешно")
            return 0
        else:
            print("✗ Ошибка миграции схемы")
            return 1
            
    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        if args.verbose:
            logging.exception("Детали ошибки:")
        return 1


async def cmd_cleanup(args: argparse.Namespace) -> int:
    """Команда очистки данных."""
    try:
        db_manager = DatabaseManager(args.master_password)
        
        if args.clear_all:
            success = await db_manager.clear_all_data()
            operation = "полная очистка"
        elif args.node_ids:
            success = await db_manager.remove_node_tables(args.node_ids)
            operation = f"удаление таблиц узлов: {', '.join(args.node_ids)}"
        else:
            success = await db_manager.cleanup_old_data(
                retention_days=args.retention_days,
                node_ids=args.specific_nodes,
                event_types=args.event_types
            )
            operation = f"очистка данных старше {args.retention_days} дней"
        
        if success:
            print(f"✓ {operation} выполнена успешно")
            return 0
        else:
            print(f"✗ Ошибка выполнения: {operation}")
            return 1
            
    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        if args.verbose:
            logging.exception("Детали ошибки:")
        return 1


async def cmd_info(args: argparse.Namespace) -> int:
    """Команда получения информации о базе данных."""
    try:
        db_manager = DatabaseManager(args.master_password)
        
        db_info = await db_manager.get_database_info()
        
        if db_info:
            print("Информация о базе данных:")
            print(f"  Имя: {db_info.get('database_name', 'N/A')}")
            print(f"  Хост: {db_info.get('host', 'N/A')}:{db_info.get('port', 'N/A')}")
            print(f"  Пользователь: {db_info.get('user', 'N/A')}")
            print(f"  Версия схемы: {db_info.get('schema_version', 'N/A')}")
            print(f"  Размер: {db_info.get('database_size', 'N/A')}")
            print(f"  Таблицы переменных: {db_info.get('variable_tables', 'N/A')}")
            print(f"  Таблицы событий: {db_info.get('event_tables', 'N/A')}")
            print(f"  Создана: {db_info.get('created_at', 'N/A')}")
            return 0
        else:
            print("✗ Не удалось получить информацию о базе данных")
            return 1
            
    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        if args.verbose:
            logging.exception("Детали ошибки:")
        return 1


async def cmd_config(args: argparse.Namespace) -> int:
    """Команда управления конфигурацией."""
    try:
        db_manager = DatabaseManager(args.master_password)
        
        if args.export:
            success = db_manager.export_config(args.export)
            if success:
                print(f"✓ Конфигурация экспортирована в {args.export}")
                return 0
            else:
                print("✗ Ошибка экспорта конфигурации")
                return 1
        elif args.import_file:
            success = db_manager.import_config(args.import_file)
            if success:
                print(f"✓ Конфигурация импортирована из {args.import_file}")
                return 0
            else:
                print("✗ Ошибка импорта конфигурации")
                return 1
        elif args.change_password:
            success = db_manager.change_master_password(args.change_password)
            if success:
                print("✓ Главный пароль изменен успешно")
                return 0
            else:
                print("✗ Ошибка изменения главного пароля")
                return 1
        else:
            print("✗ Не указана операция с конфигурацией")
            return 1
            
    except Exception as e:
        print(f"✗ Критическая ошибка: {e}")
        if args.verbose:
            logging.exception("Детали ошибки:")
        return 1


def create_parser() -> argparse.ArgumentParser:
    """Создание парсера аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description="CLI для управления базой данных OPC UA History",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Создание базы данных
  python -m uapg.cli create-db --user opcua_user --password secret --database opcua_history

  # Создание резервной копии
  python -m uapg.cli backup --output backup.backup

  # Восстановление из бэкапа
  python -m uapg.cli restore --backup-file backup.backup

  # Миграция схемы
  python -m uapg.cli migrate --target-version 1.1 --migration-file migrations.json

  # Очистка старых данных
  python -m uapg.cli cleanup --retention-days 90

  # Получение информации о БД
  python -m uapg.cli info

  # Экспорт конфигурации
  python -m uapg.cli config --export config.json
        """
    )
    
    # Общие аргументы
    parser.add_argument(
        '--master-password',
        required=True,
        help='Главный пароль для шифрования конфигурации'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Подробный вывод и логирование'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Доступные команды')
    
    # Команда создания базы данных
    create_parser = subparsers.add_parser('create-db', help='Создание базы данных')
    create_parser.add_argument('--user', required=True, help='Имя пользователя')
    create_parser.add_argument('--password', required=True, help='Пароль пользователя')
    create_parser.add_argument('--database', required=True, help='Имя базы данных')
    create_parser.add_argument('--host', default='localhost', help='Хост PostgreSQL')
    create_parser.add_argument('--port', type=int, default=5432, help='Порт PostgreSQL')
    create_parser.add_argument('--superuser', default='postgres', help='Суперпользователь')
    create_parser.add_argument('--superuser-password', help='Пароль суперпользователя')
    create_parser.add_argument('--no-timescaledb', action='store_true', help='Отключить TimescaleDB')
    
    # Команда резервного копирования
    backup_parser = subparsers.add_parser('backup', help='Создание резервной копии')
    backup_parser.add_argument('--output', help='Путь для сохранения бэкапа')
    backup_parser.add_argument('--format', choices=['custom', 'plain', 'directory'], default='custom', help='Формат бэкапа')
    backup_parser.add_argument('--no-compression', action='store_true', help='Отключить сжатие')
    
    # Команда восстановления
    restore_parser = subparsers.add_parser('restore', help='Восстановление из резервной копии')
    restore_parser.add_argument('--backup-file', required=True, help='Путь к файлу бэкапа')
    restore_parser.add_argument('--drop-existing', action='store_true', help='Удалить существующую БД')
    
    # Команда миграции
    migrate_parser = subparsers.add_parser('migrate', help='Миграция схемы')
    migrate_parser.add_argument('--target-version', required=True, help='Целевая версия')
    migrate_parser.add_argument('--migration-file', required=True, help='Файл со скриптами миграции')
    
    # Команда очистки
    cleanup_parser = subparsers.add_parser('cleanup', help='Очистка данных')
    cleanup_parser.add_argument('--retention-days', type=int, default=365, help='Дни хранения данных')
    cleanup_parser.add_argument('--specific-nodes', nargs='+', help='Конкретные узлы для очистки')
    cleanup_parser.add_argument('--event-types', nargs='+', help='Типы событий для очистки')
    cleanup_parser.add_argument('--node-ids', nargs='+', help='ID узлов для удаления таблиц')
    cleanup_parser.add_argument('--clear-all', action='store_true', help='Полная очистка всех данных')
    
    # Команда информации
    info_parser = subparsers.add_parser('info', help='Информация о базе данных')
    
    # Команда конфигурации
    config_parser = subparsers.add_parser('config', help='Управление конфигурацией')
    config_group = config_parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument('--export', help='Экспорт конфигурации в файл')
    config_group.add_argument('--import-file', help='Импорт конфигурации из файла')
    config_group.add_argument('--change-password', help='Изменение главного пароля')
    
    return parser


async def main() -> int:
    """Главная функция CLI."""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    setup_logging(args.verbose)
    
    # Выполнение команды
    command_handlers = {
        'create-db': cmd_create_database,
        'backup': cmd_backup,
        'restore': cmd_restore,
        'migrate': cmd_migrate,
        'cleanup': cmd_cleanup,
        'info': cmd_info,
        'config': cmd_config
    }
    
    handler = command_handlers.get(args.command)
    if handler:
        return await handler(args)
    else:
        print(f"✗ Неизвестная команда: {args.command}")
        return 1


if __name__ == '__main__':
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nОперация прервана пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        sys.exit(1)
