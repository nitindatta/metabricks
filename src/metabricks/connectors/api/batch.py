from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from metabricks.connectors.api.auth import build_auth_headers
from metabricks.connectors.api.types import ApiAuth, ApiConnection, ApiRequest
from metabricks.connectors.registry import register_connector
from metabricks.core.contracts import DataEnvelope


@register_connector(system_type="api", extraction_mode="batch")
class ApiBatchConnector:
    def __init__(
        self,
        connection: ApiConnection,
        request: ApiRequest,
        *,
        client: Optional[httpx.Client] = None,
    ):
        self.connection = connection
        self.request = request

        headers = dict(connection.headers)
        headers.update(build_auth_headers(connection.auth))

        self._client = client or httpx.Client(
            base_url=connection.base_url,
            timeout=connection.timeout_seconds,
            headers=headers,
        )

    def extract(self) -> DataEnvelope:
        resp = self._client.request(self.request.method, self.request.endpoint)
        resp.raise_for_status()
        
        # Try to parse as JSON; provide helpful error if it fails
        try:
            data: Any = resp.json()
        except Exception as e:
            # Log the actual response to help debug
            response_text = resp.text[:500] if hasattr(resp, 'text') else str(resp.content)[:500]
            status = resp.status_code if hasattr(resp, 'status_code') else '?'
            content_type = resp.headers.get('content-type', 'unknown') if hasattr(resp, 'headers') else 'unknown'
            raise ValueError(
                f"Failed to parse API response as JSON. "
                f"Status: {status}, Content-Type: {content_type}. "
                f"Response preview: {response_text}"
            ) from e
        
        return DataEnvelope(
            payload_type="json",
            data=data,
            metadata={
                "system_type": "api",
                "endpoint": self.request.endpoint,
                "method": self.request.method,
            },
        )
