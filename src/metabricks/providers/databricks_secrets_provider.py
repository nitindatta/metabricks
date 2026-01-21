from __future__ import annotations

import base64
from typing import Optional

import httpx

from metabricks.core.secrets_provider import SecretsProvider


class DatabricksSecretsProvider(SecretsProvider):
    """Secrets provider that fetches secrets from Databricks Secrets API."""

    def __init__(self, databricks_host: str, token: str):
        self.databricks_host = databricks_host
        self.token = token
        self._client = httpx.Client(
            base_url=f"https://{databricks_host}/api/2.0",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )

    def get_secret(self, vault_ref: str, secret_key: str) -> Optional[str]:
        """Get a secret from Databricks.

        Args:
            vault_ref: Databricks secret scope name.
            secret_key: Databricks secret key name.

        Returns:
            The decoded secret value, or None if not found / unauthorized.
        """
        try:
            response = self._client.post(
                "/secrets/get",
                json={"scope": vault_ref, "key": secret_key},
            )
            response.raise_for_status()
            data = response.json()
            raw_val = data.get("value")
            if not raw_val:
                return None

            try:
                decoded = base64.b64decode(raw_val).decode("utf-8")
                return decoded
            except Exception:
                return str(raw_val)
        except httpx.HTTPStatusError:
            return None
        except Exception:
            return None

    def close(self) -> None:
        self._client.close()
