import pytest

from metabricks.wiring.source_registry import (
    BuiltConnectorArgs,
    SourceWiringRegistry,
    SourceWiringRegistryError,
    register_source_wiring,
)


def setup_function() -> None:
    SourceWiringRegistry.clear()


def test_register_and_get_builder():
    def builder(**_kwargs):
        return BuiltConnectorArgs(args=(1,), kwargs={"x": 2})

    SourceWiringRegistry.register(system_type="api", extraction_mode="batch", builder=builder)
    got = SourceWiringRegistry.get("api", "batch")

    built = got()
    assert built.args == (1,)
    assert built.kwargs == {"x": 2}


def test_missing_wiring_raises():
    with pytest.raises(SourceWiringRegistryError, match="No wiring registered"):
        SourceWiringRegistry.get("api", "batch")


def test_register_source_wiring_decorator():
    @register_source_wiring(system_type="api", extraction_mode="batch")
    def builder(**_kwargs):
        return BuiltConnectorArgs(args=(), kwargs={})

    assert SourceWiringRegistry.get("api", "batch") is builder
