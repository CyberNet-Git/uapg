"""Tests for storage mode parsing."""

from uapg.v2.storage_mode import (
    StorageMode,
    get_events_storage_mode,
    parse_storage_mode,
    should_read_v2,
    should_write_legacy,
    should_write_v2,
)


def test_parse_storage_mode_defaults() -> None:
    assert parse_storage_mode(None) == StorageMode.DUAL
    assert parse_storage_mode("legacy") == StorageMode.LEGACY
    assert parse_storage_mode("v2") == StorageMode.V2


def test_storage_mode_flags() -> None:
    assert should_write_v2(StorageMode.DUAL)
    assert should_write_legacy(StorageMode.DUAL)
    assert should_read_v2(StorageMode.V2)
    assert not should_write_legacy(StorageMode.V2)


def test_get_events_storage_mode_monkeypatch(monkeypatch) -> None:
    monkeypatch.setenv("UAPG_EVENTS_STORAGE_MODE", "legacy")
    assert get_events_storage_mode() == StorageMode.LEGACY
