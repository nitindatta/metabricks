import pytest
from unittest.mock import MagicMock

import httpx

from metabricks.connectors.api.batch import ApiBatchConnector
from metabricks.connectors.api.types import ApiAuth, ApiConnection, ApiRequest
from metabricks.core.contracts import DataEnvelope


def test_api_batch_connector_extracts_json_envelope():
    client = MagicMock(spec=httpx.Client)

    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"ok": True}
    client.request.return_value = response

    conn = ApiConnection(
        base_url="https://example.test",
        timeout_seconds=1.0,
        headers={"X-Test": "1"},
        auth=ApiAuth(kind="none"),
    )
    req = ApiRequest(endpoint="/v1/data", method="GET")

    connector = ApiBatchConnector(conn, req, client=client)
    env = connector.extract()

    client.request.assert_called_once_with("GET", "/v1/data")
    assert isinstance(env, DataEnvelope)
    assert env.payload_type == "json"
    assert env.data == {"ok": True}
    assert env.metadata["endpoint"] == "/v1/data"


def test_api_batch_connector_raises_on_http_error():
    client = MagicMock(spec=httpx.Client)

    response = MagicMock()
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "boom",
        request=MagicMock(),
        response=MagicMock(status_code=500),
    )
    client.request.return_value = response

    conn = ApiConnection(
        base_url="https://example.test",
        timeout_seconds=1.0,
        headers={},
        auth=ApiAuth(kind="none"),
    )
    req = ApiRequest(endpoint="/v1/data", method="GET")

    connector = ApiBatchConnector(conn, req, client=client)

    with pytest.raises(httpx.HTTPStatusError):
        connector.extract()
