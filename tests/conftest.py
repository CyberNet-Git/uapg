"""Shared pytest fixtures and import stubs."""

import importlib.util
import sys
from unittest.mock import Mock

# db_manager импортирует psycopg на уровне модуля, но для основной работы с историей
# пакет не нужен: это опциональная зависимость административной утилиты. Заглушку
# ставим только когда настоящего psycopg нет, иначе тесты не видят несовместимостей
# с реальным API — именно так в create_database годами жил psycopg2-изм
# (set_isolation_level / ISOLATION_LEVEL_AUTOCOMMIT), ломавший вызов на живой БД.
if importlib.util.find_spec("psycopg") is None:  # pragma: no cover
    sys.modules.setdefault("psycopg", Mock())
