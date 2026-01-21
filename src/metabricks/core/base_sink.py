from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from metabricks.core.contracts import DataEnvelope
from metabricks.core.logger import get_logger


class BaseSink(ABC):
    def __init__(self, config: Any):
        self.config = config
        self.log = get_logger(self.__class__.__name__)

    # --- Required method ---
    @abstractmethod
    def write(self, envelope: DataEnvelope) -> Dict[str, Any]:
        raise NotImplementedError

    # --- Optional lifecycle hooks ---
    def validate_config(self):
        return True

    def pre_write(self):
        pass

    def post_write(self, output):
        """Hook after writing data (e.g. optimize, vacuum, audit)."""
        return output

    # --- Logging helpers ---
    def log_info(self, msg: str):
        self.log.info(msg)

    def log_warn(self, msg: str):
        self.log.warning(msg)

    def log_error(self, msg: str, exc: Exception = None):
        if exc:
            self.log.exception(msg)
        else:
            self.log.error(msg)
