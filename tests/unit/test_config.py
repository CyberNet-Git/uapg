"""Нормализация настроек.

Значения проверяются явными ожиданиями, а не сравнением с прежней реализацией:
это и есть описание контракта. Совпадение с 0.2.15 для всех случаев ниже
проверено отдельно при переносе.
"""

from __future__ import annotations

import json
from pathlib import Path

from uapg.core.config import (
    DEFAULT_DB_APPLICATION_NAME,
    CacheSettings,
    ConnectionSettings,
    Keepalive,
    StorageSettings,
    Timeouts,
    WriteSettings,
)

BASELINE_API = Path(__file__).parents[1] / "contract" / "baseline" / "api.json"


class TestTimeouts:
    def test_non_positive_means_unlimited(self) -> None:
        timeouts = Timeouts.build(query_sec=0, command_sec=-5)
        assert timeouts.query_sec is None
        assert timeouts.command_sec is None

    def test_none_stays_none(self) -> None:
        timeouts = Timeouts.build(query_sec=None, command_sec=None)
        assert timeouts.query_sec is None
        assert timeouts.command_sec is None

    def test_waits_have_floors(self) -> None:
        """Ожидание замка и создания пула ограничивают друг друга: нулей быть не должно."""
        timeouts = Timeouts.build(pool_close_sec=0.01, pool_create_sec=0.5, lock_wait_sec=0.2)
        assert timeouts.pool_close_sec == 0.1
        assert timeouts.pool_create_sec == 1.0
        assert timeouts.lock_wait_sec == 1.0

    def test_command_timeout_covers_flush_budget(self) -> None:
        """asyncpg не должен обрывать INSERT раньше прикладного таймаута флаша."""
        timeouts = Timeouts.build(command_sec=60.0, flush_sec=120.0)
        assert timeouts.effective_command_sec == 120.0

    def test_command_timeout_unlimited_without_both(self) -> None:
        timeouts = Timeouts.build(command_sec=None, flush_sec=0)
        assert timeouts.effective_command_sec is None

    def test_flush_operation_falls_back_to_query_timeout(self) -> None:
        timeouts = Timeouts.build(query_sec=30.0, flush_sec=0)
        assert timeouts.flush_operation_sec == 30.0

    def test_buffer_budget_covers_two_attempts_and_lock(self) -> None:
        timeouts = Timeouts.build(flush_sec=120.0, lock_wait_sec=60.0)
        assert timeouts.buffer_flush_sec == 300.0

    def test_buffer_budget_unlimited_without_flush_timeout(self) -> None:
        assert Timeouts.build(flush_sec=0).buffer_flush_sec == 0.0


class TestKeepalive:
    def test_floors(self) -> None:
        keepalive = Keepalive.build(idle_sec=-1, interval_sec=0, count=0, user_timeout_sec=-3)
        assert (keepalive.idle_sec, keepalive.interval_sec, keepalive.count) == (0, 1, 1)
        assert keepalive.user_timeout_sec == 0.0

    def test_disabled_when_idle_is_zero(self) -> None:
        assert not Keepalive.build(idle_sec=0).enabled
        assert Keepalive.build(idle_sec=30).enabled


class TestConnectionSettings:
    def test_blank_application_name_falls_back(self) -> None:
        assert ConnectionSettings.build(application_name="   ").application_name == (
            DEFAULT_DB_APPLICATION_NAME
        )

    def test_application_name_is_trimmed(self) -> None:
        assert ConnectionSettings.build(application_name=" custom ").application_name == "custom"

    def test_overrides_apply_only_known_keys(self) -> None:
        base = ConnectionSettings.build(user="a", database="b", schema="public", max_size=7)
        merged = base.with_overrides({"user": "c", "schema": "hist", "created_at": "ignored"})
        assert (merged.user, merged.schema) == ("c", "hist")
        assert merged.database == "b"
        assert merged.max_size == 7

    def test_overrides_without_known_keys_keep_object(self) -> None:
        base = ConnectionSettings.build(user="a")
        assert base.with_overrides({"version": "2.0"}) is base


class TestWriteSettings:
    def test_global_consistency_waits_for_flush(self) -> None:
        assert WriteSettings.build(read_consistency_mode="global").wait_for_flush
        assert not WriteSettings.build(read_consistency_mode="local").wait_for_flush


def test_metrics_config_keys_match_contract() -> None:
    """Из ключей метрик строятся имена узлов OPC UA, поэтому набор фиксирован."""
    baseline = json.loads(BASELINE_API.read_text())
    expected = {
        path.split(".", 1)[1]
        for path in baseline["metric_paths"]
        if path.startswith("config.")
    }
    settings = StorageSettings(
        connection=ConnectionSettings.build(),
        timeouts=Timeouts.build(),
        keepalive=Keepalive.build(),
        write=WriteSettings.build(),
        cache=CacheSettings.build(),
    )
    assert set(settings.metrics_snapshot()) == expected
