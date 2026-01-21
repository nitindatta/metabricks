from __future__ import annotations

from typing import Optional, Protocol


class SecretsProvider(Protocol):
    def get_secret(self, vault_ref: str, secret_key: str) -> Optional[str]:
        ...
