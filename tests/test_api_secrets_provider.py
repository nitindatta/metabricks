from unittest.mock import MagicMock, patch

import httpx

from metabricks.models.pipeline_config import PipelineConfig


class FakeSecretsProvider:
    def __init__(self, value: str):
        self.value = value
        self.calls: list[tuple[str, str]] = []

    def get_secret(self, vault_ref: str, secret_key: str):
        self.calls.append((vault_ref, secret_key))
        return self.value


def test_api_key_auth_resolves_from_secrets_provider():
    from metabricks.orchestrator import run_pipeline

    secrets_provider = FakeSecretsProvider("super-secret")

    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"ok": True}

    client_instance = MagicMock(spec=httpx.Client)
    client_instance.request.return_value = response

    cfg = PipelineConfig(
        pipeline_name="p",
        source={
            "system_type": "api",
            "extraction_mode": "batch",
            "connection": {
                "system_type": "api",
                "base_url": "https://example.test",
                "timeout_seconds": 5.0,
                "headers": {},
                "auth": {
                    "kind": "api_key",
                    "api_key_name": "X-API-Key",
                    "api_key_secret_ref": {"vault_ref": "scope1", "secret_key": "key1"},
                },
            },
            "endpoint": "/v1/data",
            "method": "GET",
        },
        sink=None,
    )

    with patch("httpx.Client", return_value=client_instance) as client_ctor:
        env = run_pipeline("inv-1", cfg, secrets_provider=secrets_provider)

    assert secrets_provider.calls == [("scope1", "key1")]

    # Connector should create the http client with auth header already attached.
    _, kwargs = client_ctor.call_args
    assert kwargs["base_url"] == "https://example.test"
    assert kwargs["timeout"] == 5.0
    assert kwargs["headers"]["X-API-Key"] == "super-secret"

    client_instance.request.assert_called_once_with("GET", "/v1/data")
    assert env.payload_type == "json"
    assert env.data == {"ok": True}
