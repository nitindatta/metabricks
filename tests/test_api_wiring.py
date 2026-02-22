from metabricks.models.source_config import ApiSourceConfig
from metabricks.wiring.api_wiring import build_api_connection, build_api_request


def test_api_wiring_builds_runtime_types_without_connector_config_imports():
    cfg = ApiSourceConfig(
        connection={"system_type": "api", "base_url": "https://example.test"},
        endpoint="/v1/data",
        extraction_mode="batch",
    )

    conn = build_api_connection(cfg)
    req = build_api_request(cfg)

    assert conn.base_url == "https://example.test"
    assert conn.timeout_seconds == 30.0
    assert req.endpoint == "/v1/data"
    assert req.method == "GET"
