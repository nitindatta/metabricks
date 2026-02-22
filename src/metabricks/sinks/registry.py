from __future__ import annotations

from typing import Any, Callable, ClassVar, Dict, Optional, Tuple, Type


SinkKey = Tuple[str, str]


class SinkRegistryError(RuntimeError):
    pass


class SinkRegistry:
    _registry: ClassVar[Dict[SinkKey, Type[Any]]] = {}

    @classmethod
    def register(
        cls,
        *,
        system_type: str,
        mode: str,
        sink_class: Type[Any],
        overwrite: bool = False,
    ) -> None:
        key = (system_type, mode)
        if not overwrite and key in cls._registry:
            existing = cls._registry[key]
            raise SinkRegistryError(
                f"Sink already registered for system_type={system_type!r}, mode={mode!r}: {existing}"
            )
        cls._registry[key] = sink_class

    @classmethod
    def get(cls, system_type: str, mode: str) -> Type[Any]:
        key = (system_type, mode)
        try:
            return cls._registry[key]
        except KeyError as exc:
            raise SinkRegistryError(
                f"No sink registered for system_type={system_type!r}, mode={mode!r}"
            ) from exc

    @classmethod
    def try_get(cls, system_type: str, mode: str) -> Optional[Type[Any]]:
        return cls._registry.get((system_type, mode))

    @classmethod
    def clear(cls) -> None:
        cls._registry.clear()


def register_sink(
    *,
    system_type: str,
    mode: str,
    overwrite: bool = False,
) -> Callable[[Type[Any]], Type[Any]]:
    def decorator(sink_class: Type[Any]) -> Type[Any]:
        SinkRegistry.register(
            system_type=system_type,
            mode=mode,
            sink_class=sink_class,
            overwrite=overwrite,
        )
        return sink_class

    return decorator
