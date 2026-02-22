from __future__ import annotations

from typing import Dict, Optional

from metabricks.connectors.api.types import ApiAuth, ApiConnection, ApiRequest
from metabricks.core.secrets_provider import SecretsProvider
from metabricks.models.source_config import ApiSourceConfig
from metabricks.wiring.source_registry import BuiltConnectorArgs, register_source_wiring


def _headers_from_source(cfg: ApiSourceConfig) -> Dict[str, str]:
    return dict(cfg.connection.headers)


def _auth_from_source(cfg: ApiSourceConfig, secrets_provider: Optional[SecretsProvider] = None) -> ApiAuth:
    auth = cfg.connection.auth
    kind = auth.kind

    if kind == "none":
        return ApiAuth(kind="none")

    if kind == "api_key":
        api_key_value = auth.api_key_value
        if api_key_value is None and auth.api_key_secret_ref is not None:
            if secrets_provider is None:
                raise ValueError("api_key_secret_ref provided but no secrets_provider was passed")
            api_key_value = secrets_provider.get_secret(
                auth.api_key_secret_ref.vault_ref,
                auth.api_key_secret_ref.secret_key,
            )
            if api_key_value is None:
                raise ValueError(
                    "secrets_provider returned None for api_key_secret_ref="
                    f"{auth.api_key_secret_ref.vault_ref!r}/{auth.api_key_secret_ref.secret_key!r}"
                )
        return ApiAuth(
            kind="api_key",
            api_key_name=auth.api_key_name,
            api_key_value=api_key_value,
        )

    if kind == "bearer":
        return ApiAuth(kind="bearer", bearer_token=auth.bearer_token)

    if kind == "basic":
        return ApiAuth(kind="basic", username=auth.username, password=auth.password)

    if kind == "oauth2":
        return ApiAuth(
            kind="oauth2",
            oauth_token_url=auth.oauth_token_url,
            scope=auth.scope,
            client_id=auth.client_id,
            client_secret=auth.client_secret,
        )

    raise ValueError(f"Unsupported api auth kind: {kind!r}")


def build_api_connection(
    cfg: ApiSourceConfig,
    *,
    secrets_provider: Optional[SecretsProvider] = None,
) -> ApiConnection:
    # Phase 4 will flesh out auth config properly; for now, demonstrate decoupling.
    # This wiring module is the only layer allowed to read Pydantic config.
    headers = _headers_from_source(cfg)
    timeout_seconds = float(cfg.connection.timeout_seconds)
    auth = _auth_from_source(cfg, secrets_provider=secrets_provider)

    return ApiConnection(
        base_url=cfg.connection.base_url,
        timeout_seconds=timeout_seconds,
        headers=headers,
        auth=auth,
    )


def build_api_request(cfg: ApiSourceConfig) -> ApiRequest:
    return ApiRequest(endpoint=cfg.endpoint, method=cfg.method)


@register_source_wiring(system_type="api", extraction_mode="batch")
def build_api_batch_connector_args(
    *,
    source: ApiSourceConfig,
    secrets_provider: Optional[SecretsProvider] = None,
) -> BuiltConnectorArgs:
    connection = build_api_connection(source, secrets_provider=secrets_provider)
    request = build_api_request(source)

    # Let the connector construct its own HTTP client (base_url + headers + auth).
    return BuiltConnectorArgs(args=(connection, request), kwargs={})
