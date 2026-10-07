"""Тесты сериализации payload событий и SQL вставки события.

Заменяет прежний tests/test_sql_syntax.py, который проверял `_format_event` —
метод, собиравший колонку на каждое поле события. Такой схемы давно нет: payload
кладётся целиком в JSONB-колонку `_eventdata` как карта `field -> "base64:<variant>"`,
а на чтении восстанавливается обратно. Именно на этом пути в 0.2.15 ломались
значения полей при HistoryRead, поэтому round-trip стоит держать под тестом.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from asyncua import ua

from uapg import HistoryPgSQL


@pytest.fixture
def history() -> HistoryPgSQL:
    return HistoryPgSQL(
        user="test_user",
        password="test_password",
        database="test_db",
        host="localhost",
    )


class TestEventPayloadCodec:
    """`_event_to_binary_map` ↔ `_binary_map_to_event_values`."""

    def test_round_trip_preserves_value_and_variant_type(self, history):
        source = {
            "Message": ua.Variant("авария линии", ua.VariantType.String),
            "Severity": ua.Variant(700, ua.VariantType.UInt16),
            "Measurement": ua.Variant(3.14, ua.VariantType.Double),
            "Active": ua.Variant(True, ua.VariantType.Boolean),
        }

        encoded = history._event_to_binary_map(source)
        decoded = history._binary_map_to_event_values(encoded)

        assert set(decoded) == set(source)
        for key, variant in source.items():
            assert decoded[key].VariantType == variant.VariantType, key
            if isinstance(variant.Value, float):
                assert decoded[key].Value == pytest.approx(variant.Value), key
            else:
                assert decoded[key].Value == variant.Value, key

    def test_encoded_map_is_json_serializable_base64(self, history):
        encoded = history._event_to_binary_map(
            {"Message": ua.Variant("x", ua.VariantType.String)}
        )

        # В колонку JSONB уходит именно json.dumps этой карты.
        assert json.loads(json.dumps(encoded)) == encoded
        assert encoded["Message"].startswith("base64:")

    def test_empty_payload_round_trips_to_empty(self, history):
        assert history._event_to_binary_map({}) == {}
        assert history._binary_map_to_event_values({}) == {}

    def test_node_id_field_survives_round_trip(self, history):
        node_id = ua.NodeId("Events.LineFault", 2)
        encoded = history._event_to_binary_map(
            {"EventType": ua.Variant(node_id, ua.VariantType.NodeId)}
        )

        decoded = history._binary_map_to_event_values(encoded)

        assert decoded["EventType"].Value == node_id

    def test_none_value_is_passed_through_as_none(self, history):
        """В старых строках поле могло остаться None — декодер не должен падать."""
        assert history._binary_map_to_event_values({"Broken": None}) == {"Broken": None}

    def test_non_base64_value_is_returned_as_is(self, history):
        """Значения, записанные не кодеком (например, вручную), остаются как есть."""
        assert history._binary_map_to_event_values({"Plain": "just text"}) == {
            "Plain": "just text"
        }

    def test_undecodable_value_becomes_none_without_raising(self, history):
        decoded = history._binary_map_to_event_values({"Garbage": "base64:!!!not-b64!!!"})

        assert decoded == {"Garbage": None}


class TestExtractVariantValues:
    """`_extract_variant_values` готовит payload к JSON без Variant-обёрток."""

    def test_unwraps_variants(self, history):
        extracted = history._extract_variant_values(
            {
                "Message": ua.Variant("текст", ua.VariantType.String),
                "Severity": ua.Variant(500, ua.VariantType.UInt16),
            }
        )

        assert extracted == {"Message": "текст", "Severity": 500}

    def test_plain_values_pass_through(self, history):
        assert history._extract_variant_values({"Plain": 7}) == {"Plain": 7}

    def test_result_is_json_serializable(self, history):
        extracted = history._extract_variant_values(
            {
                "EventType": ua.Variant(ua.NodeId("Events.X", 2), ua.VariantType.NodeId),
                "Time": ua.Variant(
                    datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc),
                    ua.VariantType.DateTime,
                ),
            }
        )

        json.dumps(extracted)  # не должно бросать


class TestEventInsertSql:
    """Форма INSERT для события — то, что проверял прежний test_sql_query_construction."""

    @pytest.fixture
    def connected(self, history):
        pool = MagicMock()
        pool._closed = False
        conn = AsyncMock()
        pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        history._pool = pool
        history._initialized = True
        return history, conn

    async def test_save_event_inserts_payload_as_single_jsonb_column(self, connected):
        history, conn = connected
        source_node = ua.NodeId("LineFault", 1)
        event = Mock()
        event.SourceNode = source_node
        event.EventType = ua.NodeId("Events.LineFault", 2)
        event.Time = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
        event.get_event_props_as_fields_dict.return_value = {
            "Message": ua.Variant("авария", ua.VariantType.String),
            "Severity": ua.Variant(700, ua.VariantType.UInt16),
        }
        history._datachanges_period[source_node] = (None, 0)

        await history.save_event(event)

        inserts = [
            call.args
            for call in conn.execute.await_args_list
            if "INSERT INTO" in call.args[0]
        ]
        assert inserts, "событие должно писаться INSERT-ом"
        sql, *params = inserts[0]
        assert sql == (
            'INSERT INTO "evt_1_LineFault" (_timestamp, _eventtypename, _eventdata) '
            "VALUES ($1, $2, $3)"
        )
        # Третий параметр — сериализованная карта payload, а не колонка на поле.
        payload = json.loads(params[2])
        assert set(payload) == {"Message", "Severity"}
        assert all(value.startswith("base64:") for value in payload.values())
        # И она декодируется обратно в исходные значения.
        decoded = history._binary_map_to_event_values(payload)
        assert decoded["Message"].Value == "авария"
        assert decoded["Severity"].Value == 700
