from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, Optional, Tuple

from metabricks.core.secrets_provider import SecretsProvider


WiringKey = Tuple[str, str]


class SourceWiringRegistryError(RuntimeError):
    pass


@dataclass(frozen=True)
class BuiltConnectorArgs:
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


BuilderFn = Callable[..., BuiltConnectorArgs]


class SourceWiringRegistry:
    _registry: ClassVar[Dict[WiringKey, BuilderFn]] = {}

    @classmethod
    def register(
        cls,
        *,
        system_type: str,
        extraction_mode: str,
        builder: BuilderFn,
        overwrite: bool = False,
    ) -> None:
        key = (system_type, extraction_mode)
        if not overwrite and key in cls._registry:
            raise SourceWiringRegistryError(
                f"Wiring already registered for system_type={system_type!r}, extraction_mode={extraction_mode!r}"
            )
        cls._registry[key] = builder

    @classmethod
    def get(cls, system_type: str, extraction_mode: str) -> BuilderFn:
        key = (system_type, extraction_mode)
        try:
            return cls._registry[key]
        except KeyError as exc:
            raise SourceWiringRegistryError(
                f"No wiring registered for system_type={system_type!r}, extraction_mode={extraction_mode!r}"
            ) from exc

    @classmethod
    def clear(cls) -> None:
        cls._registry.clear()


def register_source_wiring(
    *,
    system_type: str,
    extraction_mode: str,
    overwrite: bool = False,
) -> Callable[[BuilderFn], BuilderFn]:
    def decorator(builder: BuilderFn) -> BuilderFn:
        SourceWiringRegistry.register(
            system_type=system_type,
            extraction_mode=extraction_mode,
            builder=builder,
            overwrite=overwrite,
        )
        return builder

    return decorator


# Common kwargs for builder functions
BuilderKwargs = dict[str, Any]


def ensure_secrets_provider(_: Optional[SecretsProvider]) -> None:
    # Placeholder hook if we want to enforce presence for certain auth modes.
    return
