from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, Optional, Tuple

from metabricks.core.secrets_provider import SecretsProvider


WiringKey = Tuple[str, str]


class SinkWiringRegistryError(RuntimeError):
    pass


@dataclass(frozen=True)
class BuiltSinkArgs:
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


BuilderFn = Callable[..., BuiltSinkArgs]


class SinkWiringRegistry:
    _registry: ClassVar[Dict[WiringKey, BuilderFn]] = {}

    @classmethod
    def register(
        cls,
        *,
        system_type: str,
        mode: str,
        builder: BuilderFn,
        overwrite: bool = False,
    ) -> None:
        key = (system_type, mode)
        if not overwrite and key in cls._registry:
            raise SinkWiringRegistryError(
                f"Sink wiring already registered for system_type={system_type!r}, mode={mode!r}"
            )
        cls._registry[key] = builder

    @classmethod
    def get(cls, system_type: str, mode: str) -> BuilderFn:
        key = (system_type, mode)
        try:
            return cls._registry[key]
        except KeyError as exc:
            raise SinkWiringRegistryError(
                f"No sink wiring registered for system_type={system_type!r}, mode={mode!r}"
            ) from exc

    @classmethod
    def clear(cls) -> None:
        cls._registry.clear()


def register_sink_wiring(
    *,
    system_type: str,
    mode: str,
    overwrite: bool = False,
) -> Callable[[BuilderFn], BuilderFn]:
    def decorator(builder: BuilderFn) -> BuilderFn:
        SinkWiringRegistry.register(
            system_type=system_type,
            mode=mode,
            builder=builder,
            overwrite=overwrite,
        )
        return builder

    return decorator


def ensure_secrets_provider(_: Optional[SecretsProvider]) -> None:
    return
