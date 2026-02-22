import pytest

from metabricks.connectors.api.auth import build_auth_headers
from metabricks.connectors.api.types import ApiAuth


def test_build_auth_headers_none():
    assert build_auth_headers(ApiAuth(kind="none")) == {}


def test_build_auth_headers_api_key():
    headers = build_auth_headers(ApiAuth(kind="api_key", api_key_name="X-API-Key", api_key_value="v"))
    assert headers == {"X-API-Key": "v"}


def test_build_auth_headers_bearer():
    headers = build_auth_headers(ApiAuth(kind="bearer", bearer_token="t"))
    assert headers["Authorization"] == "Bearer t"


def test_build_auth_headers_basic():
    headers = build_auth_headers(ApiAuth(kind="basic", username="u", password="p"))
    assert headers["Authorization"].startswith("Basic ")


def test_build_auth_headers_invalid_api_key_missing_name():
    with pytest.raises(ValueError):
        build_auth_headers(ApiAuth(kind="api_key", api_key_value="v"))
