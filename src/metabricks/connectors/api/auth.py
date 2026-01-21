from __future__ import annotations

import base64
from typing import Dict

from metabricks.connectors.api.types import ApiAuth


def build_auth_headers(auth: ApiAuth) -> Dict[str, str]:
    if auth.kind == "none":
        return {}

    if auth.kind == "api_key":
        if not auth.api_key_name or not auth.api_key_value:
            raise ValueError("api_key auth requires api_key_name and api_key_value")
        return {auth.api_key_name: auth.api_key_value}

    if auth.kind == "bearer":
        if not auth.bearer_token:
            raise ValueError("bearer auth requires bearer_token")
        return {"Authorization": f"Bearer {auth.bearer_token}"}

    if auth.kind == "basic":
        if not auth.username or not auth.password:
            raise ValueError("basic auth requires username and password")
        token = base64.b64encode(f"{auth.username}:{auth.password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    if auth.kind == "oauth2":
        # OAuth2 token fetching belongs in a higher-level auth strategy (wiring layer or auth module).
        raise NotImplementedError("oauth2 auth is not implemented")

    raise ValueError(f"Unsupported auth kind: {auth.kind!r}")
