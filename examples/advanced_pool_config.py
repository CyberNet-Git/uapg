"""
Пример продвинутой конфигурации пула подключений PostgreSQL для высоконагруженных OPC UA систем.

Этот пример демонстрирует:
- Настройку пула подключений для различных сценариев нагрузки
- Мониторинг состояния пула
- Обработку ошибок подключения
- Оптимизацию для TimescaleDB
"""

import asyncio
import logging
from asyncua import Server, ua
from uapg import HistoryPgSQL
import time
from datetime import datetime, timezone, UTC


# Настройка логирования для мониторинга пула подключений
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PoolMonitor:
    """Монитор для отслеживания состояния пула подключений."""
    
    def __init__(self, history_storage: HistoryPgSQL):
        self.history = history_storage
        self.stats = {
            'queries_executed': 0,
            'errors_count': 0,
            'start_time': time.time()
        }
    
    async def get_pool_status(self):
        """Получение статуса пула подключений."""
        if hasattr(self.history, '_pool') and self.history._pool:
            pool = self.history._pool
            return {
                'min_size': pool.get_min_size(),
                'max_size': pool.get_max_size(),
                'size': pool.get_size(),
                'free_size': pool.get_free_size(),
                'active_connections': pool.get_size() - pool.get_free_size()
            }
        return None
    
    def log_stats(self):
        """Логирование статистики."""
        uptime = time.time() - self.stats['start_time']
        logger.info(f"Pool Statistics - Uptime: {uptime:.1f}s, "
                   f"Queries: {self.stats['queries_executed']}, "
                   f"Errors: {self.stats['errors_count']}")


async def create_high_load_server():
    """Создание OPC UA сервера с оптимизированным пулом подключений для высокой нагрузки."""
    
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4846/freeopcua/highload/")
    server.set_server_name("High Load OPC UA Server with Optimized History")
    
    # Конфигурация безопасности
    server.set_security_policy([
        ua.SecurityPolicyType.NoSecurity,
        ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt,
    ])
    
    # Оптимизированная конфигурация пула подключений для высокой нагрузки
    history = HistoryPgSQL(
        user='postgres',
        password='postmaster',
        database='opcua',
        host='127.0.0.1',
        port=5432,
        min_size=10,     # Больше минимальных соединений для быстрого отклика
        max_size=50,     # Больше максимальных соединений для пиковых нагрузок
        max_history_data_response_size=50000  # Увеличенный размер ответа
    )
    
    await history.init()
    server.iserver.history_manager.set_storage(history)
    
    # Создание монитора пула
    monitor = PoolMonitor(history)
    
    return server, history, monitor


async def create_balanced_server():
    """Создание OPC UA сервера со сбалансированным пулом подключений."""
    
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4847/freeopcua/balanced/")
    server.set_server_name("Balanced OPC UA Server with History")
    
    server.set_security_policy([
        ua.SecurityPolicyType.NoSecurity,
    ])
    
    # Сбалансированная конфигурация пула подключений
    history = HistoryPgSQL(
        user='postgres',
        password='postmaster',
        database='opcua',
        host='127.0.0.1',
        port=5432,
        min_size=5,      # Умеренное количество минимальных соединений
        max_size=25,     # Умеренное количество максимальных соединений
        max_history_data_response_size=20000
    )
    
    await history.init()
    server.iserver.history_manager.set_storage(history)
    
    monitor = PoolMonitor(history)
    
    return server, history, monitor


async def create_resource_efficient_server():
    """Создание OPC UA сервера с ресурсоэффективным пулом подключений."""
    
    server = Server()
    await server.init()
    server.set_endpoint("opc.tcp://0.0.0.0:4848/freeopcua/efficient/")
    server.set_server_name("Resource Efficient OPC UA Server with History")
    
    server.set_security_policy([
        ua.SecurityPolicyType.NoSecurity,
    ])
    
    # Ресурсоэффективная конфигурация пула подключений
    history = HistoryPgSQL(
        user='postgres',
        password='postmaster',
        database='opcua',
        host='127.0.0.1',
        port=5432,
        min_size=2,      # Минимальное количество соединений
        max_size=10,     # Ограниченное количество максимальных соединений
        max_history_data_response_size=10000
    )
    
    await history.init()
    server.iserver.history_manager.set_storage(history)
    
    monitor = PoolMonitor(history)
    
    return server, history, monitor


async def run_server_with_monitoring(server, history, monitor, server_name: str):
    """Запуск сервера с мониторингом пула подключений."""
    
    # Создание адресного пространства
    idx = await server.register_namespace("http://example.com")
    
    # Создание нескольких переменных для тестирования
    variables = []
    for i in range(5):
        var = await server.nodes.objects.add_variable(
            idx, f"TestVariable_{i}", 0.0
        )
        await var.set_writable()
        await server.historize_node_data_change(var, period=None, count=0)
        variables.append(var)
    
    async with server:
        logger.info(f"{server_name} started successfully")
        
        try:
            t = 0
            while True:
                # Обновление значений переменных
                for i, var in enumerate(variables):
                    value = (i + 1) * math.sin(time.time() / 10 + i)
                    await var.set_value(ua.Variant(value, ua.VariantType.Double))
                
                # Логирование статистики каждые 10 секунд
                if t % 10 == 0:
                    pool_status = await monitor.get_pool_status()
                    if pool_status:
                        logger.info(f"{server_name} Pool Status: {pool_status}")
                    monitor.log_stats()
                
                t += 1
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            logger.info(f"Stopping {server_name}")
        finally:
            await server.stop()
            await history.stop()


async def main():
    """Основная функция с выбором типа сервера."""
    
    print("Выберите тип OPC UA сервера:")
    print("1. High Load Server (min_size=10, max_size=50)")
    print("2. Balanced Server (min_size=5, max_size=25)")
    print("3. Resource Efficient Server (min_size=2, max_size=10)")
    
    try:
        choice = input("Введите номер (1-3): ").strip()
        
        if choice == "1":
            server, history, monitor = await create_high_load_server()
            await run_server_with_monitoring(server, history, monitor, "High Load Server")
        elif choice == "2":
            server, history, monitor = await create_balanced_server()
            await run_server_with_monitoring(server, history, monitor, "Balanced Server")
        elif choice == "3":
            server, history, monitor = await create_resource_efficient_server()
            await run_server_with_monitoring(server, history, monitor, "Resource Efficient Server")
        else:
            print("Неверный выбор. Запускаю сбалансированный сервер по умолчанию.")
            server, history, monitor = await create_balanced_server()
            await run_server_with_monitoring(server, history, monitor, "Balanced Server (Default)")
            
    except Exception as e:
        logger.error(f"Ошибка при запуске сервера: {e}")
        raise


if __name__ == "__main__":
    import math
    asyncio.run(main())
