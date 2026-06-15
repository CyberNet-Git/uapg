"""Shared pytest fixtures and import stubs."""

import sys
from unittest.mock import Mock

sys.modules.setdefault("psycopg", Mock())
