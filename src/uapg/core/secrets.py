"""Чтение зашифрованной конфигурации подключения.

Формат достался от админ-утилиты, которая уходит из пакета, но читать его нужно
по-прежнему: ``HistoryTimescale.from_config_file`` и ``from_encrypted_config``
остаются в публичном API, а у пользователей лежат готовые ``.enc``-файлы.

Про стойкость этой схемы стоит знать правду: ключ Fernet хранится в отдельном
файле (по умолчанию ``.db_key``), и если он есть, мастер-пароль не участвует в
расшифровке вообще. Файл ключа рядом с файлом конфигурации равносилен
незашифрованным учётным данным.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

DEFAULT_KEY_FILE = ".db_key"
DEFAULT_CONFIG_FILE = "db_config.enc"

# Первые 16 байт файла ключа — соль PBKDF2, сам ключ Fernet идёт следом.
_SALT_LENGTH = 16

logger = logging.getLogger("uapg.secrets")


class EncryptedConfigError(RuntimeError):
    """Расшифровать конфигурацию не удалось."""


def _load_cipher(key_file: str) -> Any:
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:  # pragma: no cover - зависит от окружения
        raise EncryptedConfigError(
            "Для зашифрованной конфигурации нужен пакет cryptography: pip install cryptography"
        ) from exc

    path = Path(key_file)
    if not path.exists():
        raise EncryptedConfigError(
            f"Файл ключа {key_file} не найден: расшифровать конфигурацию нечем"
        )
    key = path.read_bytes()
    if len(key) <= _SALT_LENGTH:
        raise EncryptedConfigError(f"Файл ключа {key_file} повреждён")
    return Fernet(key[_SALT_LENGTH:])


def decrypt_config(payload: bytes, key_file: str = DEFAULT_KEY_FILE) -> Dict[str, Any]:
    """Расшифровать содержимое ``.enc`` в словарь параметров подключения."""
    cipher = _load_cipher(key_file)
    try:
        decrypted = cipher.decrypt(payload)
    except Exception as exc:
        raise EncryptedConfigError(f"Расшифровка не удалась: {exc}") from exc
    try:
        data = json.loads(decrypted.decode())
    except Exception as exc:
        raise EncryptedConfigError(f"Конфигурация не разбирается как JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise EncryptedConfigError("Конфигурация должна быть объектом JSON")
    return data


def load_connection_config(
    *,
    config_file: Optional[str] = None,
    encrypted_config: Optional[str] = None,
    master_password: Optional[str] = None,
    key_file: str = DEFAULT_KEY_FILE,
) -> Optional[Dict[str, Any]]:
    """Загрузить параметры подключения, если они заданы зашифрованной конфигурацией.

    Приоритет строки над файлом сохранён от 0.2.15. Возвращает ``None``, когда
    зашифрованная конфигурация не задана или не читается: вызывающий в этом
    случае работает с параметрами, переданными напрямую.
    """
    if not master_password:
        return None

    try:
        if encrypted_config:
            return decrypt_config(encrypted_config.encode(), key_file)
        if config_file:
            path = Path(config_file)
            if not path.exists():
                logger.warning("Файл конфигурации %s не найден", config_file)
                return None
            return decrypt_config(path.read_bytes(), key_file)
    except EncryptedConfigError as exc:
        logger.warning("Зашифрованная конфигурация не применена (%s), берём прямые параметры", exc)
        return None

    return None
