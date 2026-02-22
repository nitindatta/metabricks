import pytest

from metabricks.connectors.registry import (
    ConnectorRegistry,
    ConnectorRegistryError,
    register_connector,
)


def setup_function() -> None:
    ConnectorRegistry.clear()


def test_register_and_get_round_trip():
    class Dummy:
        pass

    ConnectorRegistry.register(system_type="api", extraction_mode="batch", connector_class=Dummy)

    assert ConnectorRegistry.get("api", "batch") is Dummy
    assert ConnectorRegistry.try_get("api", "batch") is Dummy


def test_get_missing_raises_helpful_error():
    with pytest.raises(ConnectorRegistryError, match="No connector registered"):
        ConnectorRegistry.get("api", "batch")


def test_duplicate_registration_raises_by_default():
    class Dummy1:
        pass

    class Dummy2:
        pass

    ConnectorRegistry.register(system_type="api", extraction_mode="batch", connector_class=Dummy1)

    with pytest.raises(ConnectorRegistryError, match="already registered"):
        ConnectorRegistry.register(system_type="api", extraction_mode="batch", connector_class=Dummy2)


def test_overwrite_allows_re_registration():
    class Dummy1:
        pass

    class Dummy2:
        pass

    ConnectorRegistry.register(system_type="api", extraction_mode="batch", connector_class=Dummy1)
    ConnectorRegistry.register(
        system_type="api", extraction_mode="batch", connector_class=Dummy2, overwrite=True
    )

    assert ConnectorRegistry.get("api", "batch") is Dummy2


def test_register_connector_decorator_registers_class():
    @register_connector(system_type="api", extraction_mode="batch")
    class Dummy:
        pass

    assert ConnectorRegistry.get("api", "batch") is Dummy
