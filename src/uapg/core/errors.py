"""Исключения уровня доступа к данным."""

from __future__ import annotations


class DatabaseStopping(RuntimeError):
    """Операция запрошена во время остановки бэкенда."""


class OperationTimeout(TimeoutError):
    """Операция с БД не уложилась в отведённый бюджет.

    Наследование от ``TimeoutError`` сохранено намеренно: вызывающий код и
    asyncio одинаково распознают таймаут, а сообщение дополнительно называет
    слой, чтобы в логе не приходилось гадать, чей именно бюджет исчерпан.
    """

    def __init__(self, operation: str, timeout_sec: float, layer: str) -> None:
        super().__init__(
            f"{operation} не уложилась в {timeout_sec:.1f} с (слой {layer})"
        )
        self.operation = operation
        self.timeout_sec = timeout_sec
        self.layer = layer
