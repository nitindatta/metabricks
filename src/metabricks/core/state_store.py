from __future__ import annotations

from typing import Dict, Optional, Protocol


class StateStore(Protocol):
    def get(self, key: str) -> Optional[str]:
        ...

    def set(self, key: str, value: str) -> None:
        ...

    def delete(self, key: str) -> None:
        ...


class InMemoryStateStore:
    def __init__(self, initial: Optional[Dict[str, str]] = None):
        self._data: Dict[str, str] = dict(initial or {})

    def get(self, key: str) -> Optional[str]:
        return self._data.get(key)

    def set(self, key: str, value: str) -> None:
        self._data[key] = value

    def delete(self, key: str) -> None:
        self._data.pop(key, None)
