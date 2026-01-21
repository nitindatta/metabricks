from __future__ import annotations

from typing import Annotated, Dict, Literal, Optional, Union

from pydantic import BaseModel, Field, PositiveFloat, model_validator


# -----------------
# API connections
# -----------------


class ApiAuthNoneConfig(BaseModel):
    kind: Literal["none"] = "none"


class SecretRefConfig(BaseModel):
    """Reference to a secret stored in a vault/backend.

    For Databricks Secrets this maps to (scope, key).
    """

    vault_ref: str
    secret_key: str


class ApiAuthApiKeyConfig(BaseModel):
    kind: Literal["api_key"] = "api_key"

    api_key_name: str
    api_key_value: Optional[str] = None
    api_key_secret_ref: Optional[SecretRefConfig] = None

    @model_validator(mode="after")
    def _validate_secret_source(self) -> "ApiAuthApiKeyConfig":
        if self.api_key_value is None and self.api_key_secret_ref is None:
            raise ValueError("api_key auth requires either api_key_value or api_key_secret_ref")
        return self


class ApiAuthBearerConfig(BaseModel):
    kind: Literal["bearer"] = "bearer"

    bearer_token: Optional[str] = None


class ApiAuthBasicConfig(BaseModel):
    kind: Literal["basic"] = "basic"

    username: str
    password: Optional[str] = None


class ApiAuthOAuth2Config(BaseModel):
    kind: Literal["oauth2"] = "oauth2"

    oauth_token_url: str
    scope: Optional[str] = None

    client_id: Optional[str] = None
    client_secret: Optional[str] = None


ApiAuthConfig = Annotated[
    Union[
        ApiAuthNoneConfig,
        ApiAuthApiKeyConfig,
        ApiAuthBearerConfig,
        ApiAuthBasicConfig,
        ApiAuthOAuth2Config,
    ],
    Field(discriminator="kind"),
]


class ApiConnectionConfig(BaseModel):
    system_type: Literal["api"] = "api"

    base_url: str
    timeout_seconds: PositiveFloat = 30.0
    headers: Dict[str, str] = Field(default_factory=dict)

    auth: ApiAuthConfig = Field(default_factory=ApiAuthNoneConfig)


# -----------------
# JDBC connections
# -----------------


class JdbcConnectionConfig(BaseModel):
    system_type: Literal["jdbc"] = "jdbc"

    connection_url: str
    user: Optional[str] = None
    driver: Optional[str] = None


# -----------------
# File connections
# -----------------


class FileConnectionConfig(BaseModel):
    system_type: Literal["file"] = "file"

    # Placeholder for shared filesystem/credentials settings.
    # Object-level path/pattern lives in Source/Sink specs.
    root: Optional[str] = None


# -----------------
# Kafka connections
# -----------------


class KafkaConnectionConfig(BaseModel):
    system_type: Literal["kafka"] = "kafka"

    bootstrap_servers: str


# -----------------
# Databricks connections
# -----------------


class DatabricksConnectionConfig(BaseModel):
    system_type: Literal["databricks"] = "databricks"

    # For now, the in-cluster Spark session is assumed. These are placeholders for
    # future expansion (SQL warehouse / serverless / UC scoping, etc.).
    catalog: Optional[str] = None
    schema_name: Optional[str] = None


ConnectionConfig = Annotated[
    Union[
        ApiConnectionConfig,
        JdbcConnectionConfig,
        FileConnectionConfig,
        KafkaConnectionConfig,
        DatabricksConnectionConfig,
    ],
    Field(discriminator="system_type"),
]
