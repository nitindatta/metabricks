from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, Optional, Tuple, Type


ConnectorKey = Tuple[str, str]


class ConnectorRegistryError(RuntimeError):
    pass


@dataclass(frozen=True)
class ConnectorRegistration:
    system_type: str
    extraction_mode: str
    connector_class: Type[Any]


class ConnectorRegistry:
    _registry: ClassVar[Dict[ConnectorKey, Type[Any]]] = {}

    @classmethod
    def register(
        cls,
        *,
        system_type: str,
        extraction_mode: str,
        connector_class: Type[Any],
        overwrite: bool = False,
    ) -> None:
        key = (system_type, extraction_mode)
        if not overwrite and key in cls._registry:
            existing = cls._registry[key]
            raise ConnectorRegistryError(
                f"Connector already registered for system_type={system_type!r}, extraction_mode={extraction_mode!r}: {existing}"
            )
        cls._registry[key] = connector_class

    @classmethod
    def get(cls, system_type: str, extraction_mode: str) -> Type[Any]:
        key = (system_type, extraction_mode)
        try:
            return cls._registry[key]
        except KeyError as exc:
            raise ConnectorRegistryError(
                f"No connector registered for system_type={system_type!r}, extraction_mode={extraction_mode!r}"
            ) from exc

    @classmethod
    def try_get(cls, system_type: str, extraction_mode: str) -> Optional[Type[Any]]:
        return cls._registry.get((system_type, extraction_mode))

    @classmethod
    def clear(cls) -> None:
        cls._registry.clear()


def register_connector(
    *,
    system_type: str,
    extraction_mode: str,
    overwrite: bool = False,
) -> Callable[[Type[Any]], Type[Any]]:
    def decorator(connector_class: Type[Any]) -> Type[Any]:
        ConnectorRegistry.register(
            system_type=system_type,
            extraction_mode=extraction_mode,
            connector_class=connector_class,
            overwrite=overwrite,
        )
        return connector_class

    return decorator
