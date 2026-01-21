from metabricks.connectors.registry import ConnectorRegistry


def setup_function() -> None:
    ConnectorRegistry.clear()

    # Ensure a clean import so decorator registration runs again.
    import sys

    sys.modules.pop("metabricks.connectors.api.batch", None)
    sys.modules.pop("metabricks.connectors.api", None)


def test_importing_api_package_registers_batch_connector():
    import metabricks.connectors.api.batch  # noqa: F401

    connector_cls = ConnectorRegistry.get("api", "batch")
    assert connector_cls.__name__ == "ApiBatchConnector"
