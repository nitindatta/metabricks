from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Literal, Optional


HttpMethod = Literal["GET", "POST"]


@dataclass(frozen=True)
class ApiAuth:
    kind: Literal["none", "api_key", "bearer", "basic", "oauth2"] = "none"

    api_key_name: Optional[str] = None
    api_key_value: Optional[str] = None

    bearer_token: Optional[str] = None

    username: Optional[str] = None
    password: Optional[str] = None

    oauth_token_url: Optional[str] = None
    scope: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


@dataclass(frozen=True)
class ApiConnection:
    base_url: str
    timeout_seconds: float
    headers: Dict[str, str]
    auth: ApiAuth


@dataclass(frozen=True)
class ApiRequest:
    endpoint: str
    method: HttpMethod = "GET"
