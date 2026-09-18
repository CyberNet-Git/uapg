"""Сквозной путь: сервер OPC UA → uapg → TimescaleDB → клиент OPC UA.

Единственный тест, где бэкенд работает так, как в проде: его вызывает
HistoryManager настоящего сервера asyncua, а данные читает настоящий клиент
через HistoryRead. Всё остальное проверяет слои по отдельности.
"""

from __future__ import annotations

import asyncio
import socket
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Tuple

import pytest
from asyncua import Client, Server, ua

from tests.conftest import connect_kwargs
from uapg import HistoryTimescaleV2
from uapg.codec import make_datavalue
from uapg.v2.storage_mode import StorageMode

pytestmark = pytest.mark.integration


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def _wait_for(predicate: Any, timeout: float = 15.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(0.2)
    raise AssertionError("условие не наступило за отведённое время")


@pytest.fixture
async def stand(pg_database: str) -> AsyncIterator[Tuple[Server, HistoryTimescaleV2, str, int]]:
    port = _free_port()
    url = f"opc.tcp://127.0.0.1:{port}/uapg/"
    server = Server()
    await server.init()
    server.set_endpoint(url)
    idx = await server.register_namespace("http://uapg.test")

    storage = HistoryTimescaleV2(
        **connect_kwargs(pg_database),
        events_storage_mode=StorageMode.DUAL,
        history_write_max_batch_interval_sec=0.05,
    )
    server.iserver.history_manager.set_storage(storage)
    await storage.init()

    await server.start()
    try:
        yield server, storage, url, idx
    finally:
        await server.stop()


async def test_values_roundtrip_through_opcua(stand: Tuple[Server, HistoryTimescaleV2, str, int]) -> None:
    server, storage, url, idx = stand
    sensor = await server.nodes.objects.add_variable(idx, "Temperature", 0.0)
    await server.historize_node_data_change(sensor, period=None)

    for value in (21.5, 22.0, 22.5, 23.0):
        await sensor.write_value(value)
        await asyncio.sleep(0.15)

    async def written() -> bool:
        rows = await storage._db.fetchval("SELECT count(*) FROM variables_history")
        return int(rows) >= 3

    await _wait_for(written)

    async with Client(url) as client:
        node = client.get_node(sensor.nodeid)
        history = await node.read_raw_history(
            datetime.now(timezone.utc) - timedelta(minutes=5), datetime.now(timezone.utc)
        )
    values = [dv.Value.Value for dv in history]
    # Первое уведомление после подписки подавляется: это значение уже в базе.
    assert values[-3:] == [22.0, 22.5, 23.0]
    assert all(dv.SourceTimestamp is not None for dv in history)


async def test_bad_quality_value_is_stored_and_read_back(
    stand: Tuple[Server, HistoryTimescaleV2, str, int],
) -> None:
    """Значение с кодом Bad в 0.2.15 не записывалось и уносило пачку."""
    server, storage, url, idx = stand
    sensor = await server.nodes.objects.add_variable(idx, "Pressure", 1.0)
    await server.historize_node_data_change(sensor, period=None)
    await sensor.write_value(ua.DataValue(Value=ua.Variant(2.0, ua.VariantType.Double)))
    await asyncio.sleep(0.2)
    bad = make_datavalue(
        value=ua.Variant(3.0, ua.VariantType.Double),
        status=ua.StatusCode(ua.StatusCodes.BadSensorFailure),
        source_timestamp=datetime.now(timezone.utc),
    )
    await sensor.write_value(bad)

    async def written() -> bool:
        rows = await storage._db.fetchval("SELECT count(*) FROM variables_history WHERE statuscode < 0")
        return int(rows) >= 1

    await _wait_for(written)

    async with Client(url) as client:
        history = await client.get_node(sensor.nodeid).read_raw_history(
            datetime.now(timezone.utc) - timedelta(minutes=5), datetime.now(timezone.utc)
        )
    assert any(dv.StatusCode.value == ua.StatusCodes.BadSensorFailure for dv in history)


async def test_events_roundtrip_through_opcua(stand: Tuple[Server, HistoryTimescaleV2, str, int]) -> None:
    server, storage, url, idx = stand
    source = await server.nodes.objects.add_object(idx, "Pump")
    await source.set_event_notifier(
        [ua.EventNotifier.SubscribeToEvents, ua.EventNotifier.HistoryRead]
    )
    # Генератор добавляет источнику ссылку GeneratesEvent, а asyncua историзует
    # только типы, на которые такая ссылка есть — поэтому он создаётся первым.
    generator = await server.get_event_generator(ua.ObjectIds.BaseEventType, source)
    await server.historize_node_event(source, period=None)

    for severity in (100, 500, 900):
        generator.event.Severity = severity
        generator.event.Message = ua.LocalizedText(f"авария {severity}")
        await generator.trigger()
        await asyncio.sleep(0.1)

    async def written() -> bool:
        rows = await storage._db.fetchval("SELECT count(*) FROM events_ts")
        return int(rows) >= 3

    await _wait_for(written)

    async with Client(url) as client:
        node = client.get_node(source.nodeid)
        events = await node.read_event_history(
            datetime.now(timezone.utc) - timedelta(minutes=5), datetime.now(timezone.utc)
        )
    assert sorted(event.Severity for event in events) == [100, 500, 900]
    assert {event.Message.Text for event in events} == {"авария 100", "авария 500", "авария 900"}


async def test_settings_and_metrics_are_published(
    stand: Tuple[Server, HistoryTimescaleV2, str, int],
) -> None:
    """Узлы — контракт с сервером: имена и типы должны совпадать с 0.2.15."""
    server, storage, url, idx = stand
    await storage.expose_history_settings_nodes(server, idx)
    await storage.expose_history_metrics_nodes(server, idx)
    await storage.refresh_history_metrics_nodes()

    async with Client(url) as client:
        history = await client.nodes.server.get_child([f"{idx}:History"])
        settings = await history.get_child([f"{idx}:HistorySettings"])
        metrics = await history.get_child([f"{idx}:HistoryMetrics"])

        storage_type = await (await settings.get_child([f"{idx}:StorageType"])).read_value()
        mode = await (await settings.get_child([f"{idx}:EventsStorageMode"])).read_value()
        timescale = await (await settings.get_child([f"{idx}:TimescaleExtensionAvailable"])).read_value()
        queue = await metrics.get_child([f"{idx}:WriteVariablesQueueMaxSize"])
        queue_value = await queue.read_data_value()

    assert storage_type == "timescale"
    assert mode == "dual"
    assert timescale is True
    assert queue_value.Value.VariantType == ua.VariantType.Int64
    assert queue_value.Value.Value == 10000
