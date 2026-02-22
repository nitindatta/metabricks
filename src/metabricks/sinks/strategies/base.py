from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from metabricks.core.contracts import DataEnvelope


class WriterStrategy(ABC):
    @abstractmethod
    def supports_payload_type(self, payload_type: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def write(self, envelope: DataEnvelope, config: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError
