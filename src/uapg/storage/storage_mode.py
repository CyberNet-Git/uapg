"""Storage mode configuration for History V2."""

from __future__ import annotations

import os
from enum import Enum


class StorageMode(str, Enum):
    LEGACY = "legacy"
    DUAL = "dual"
    V2 = "v2"


def parse_storage_mode(raw: str | None, default: StorageMode = StorageMode.DUAL) -> StorageMode:
    if not raw:
        return default
    normalized = raw.strip().lower()
    for mode in StorageMode:
        if mode.value == normalized:
            return mode
    return default


def get_events_storage_mode() -> StorageMode:
    return parse_storage_mode(os.environ.get("UAPG_EVENTS_STORAGE_MODE"), StorageMode.DUAL)


def get_variables_storage_mode() -> StorageMode:
    return parse_storage_mode(os.environ.get("UAPG_VARIABLES_STORAGE_MODE"), StorageMode.LEGACY)


def should_write_v2(mode: StorageMode) -> bool:
    return mode in (StorageMode.DUAL, StorageMode.V2)


def should_write_legacy(mode: StorageMode) -> bool:
    return mode in (StorageMode.LEGACY, StorageMode.DUAL)


def should_read_v2(mode: StorageMode) -> bool:
    return mode in (StorageMode.DUAL, StorageMode.V2)
