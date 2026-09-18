"""Сервер OPC UA с историей в TimescaleDB.

Поднимает сервер, историзует переменную и источник событий, пишет несколько
значений и событий и читает их обратно через HistoryRead.

    UAPG_DSN=postgresql://uapg_test:uapg_test@127.0.0.1:55432/uapg_test \\
        python examples/server_with_history.py
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from asyncua import Client, Server, ua

from uapg import HistoryTimescaleV2
from uapg.v2.events_config import EventsV2Config
from uapg.v2.storage_mode import StorageMode

ENDPOINT = "opc.tcp://127.0.0.1:48410/uapg-example/"


def connection_from_env() -> dict:
    dsn = urlparse(os.environ.get("UAPG_DSN", "postgresql://postgres:postgres@127.0.0.1:5432/opcua"))
    return {
        "host": dsn.hostname,
        "port": dsn.port or 5432,
        "user": dsn.username,
        "password": dsn.password,
        "database": dsn.path.lstrip("/"),
    }


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("asyncua").setLevel(logging.WARNING)

    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)
    idx = await server.register_namespace("http://example.com/uapg")

    storage = HistoryTimescaleV2(
        **connection_from_env(),
        global_retention_period=timedelta(days=30),
        events_storage_mode=StorageMode.DUAL,
        events_v2_config=EventsV2Config.from_csv(indexed="Severity"),
    )
    server.iserver.history_manager.set_storage(storage)
    await storage.init()
    await server.start()

    try:
        await storage.expose_history_settings_nodes(server, idx)
        await storage.expose_history_metrics_nodes(server, idx)

        temperature = await server.nodes.objects.add_variable(idx, "Temperature", 20.0)
        await server.historize_node_data_change(temperature, period=None)

        pump = await server.nodes.objects.add_object(idx, "Pump")
        await pump.set_event_notifier(
            [ua.EventNotifier.SubscribeToEvents, ua.EventNotifier.HistoryRead]
        )
        # Генератор добавляет источнику ссылку GeneratesEvent — без неё asyncua
        # не подпишется на события при историзации.
        alarms = await server.get_event_generator(ua.ObjectIds.BaseEventType, pump)
        await server.historize_node_event(pump, period=None)

        for step in range(5):
            await temperature.write_value(20.0 + step)
            alarms.event.Severity = 100 * (step + 1)
            alarms.event.Message = ua.LocalizedText(f"шаг {step}")
            await alarms.trigger()
            await asyncio.sleep(0.3)
        await asyncio.sleep(1.5)  # дать буферу записи сброситься

        now = datetime.now(timezone.utc)
        async with Client(ENDPOINT) as client:
            values = await client.get_node(temperature.nodeid).read_raw_history(
                now - timedelta(minutes=5), now
            )
            events = await client.get_node(pump.nodeid).read_event_history(
                now - timedelta(minutes=5), now
            )

        print("значения:", [dv.Value.Value for dv in values])
        print("события:", [(e.Severity, e.Message.Text) for e in events])

        await storage.refresh_history_metrics_nodes()
        written = storage.get_performance_metrics()["write"]["variables"]["flushed_items_total"]
        print("записано значений:", written)
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
