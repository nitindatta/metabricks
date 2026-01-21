from __future__ import annotations

from typing import Optional

from metabricks.connectors.types import KafkaConnection
from metabricks.core.secrets_provider import SecretsProvider
from metabricks.models.source_config import KafkaSourceConfig
from metabricks.wiring.source_registry import BuiltConnectorArgs, register_source_wiring


def build_kafka_connection(cfg: KafkaSourceConfig) -> KafkaConnection:
    return KafkaConnection(
        bootstrap_servers=cfg.connection.bootstrap_servers,
        topic=cfg.topic,
        starting_offsets=cfg.starting_offsets,
        kafka_options=dict(cfg.kafka_options or {}),
    )


@register_source_wiring(system_type="kafka", extraction_mode="streaming")
def build_kafka_streaming_connector_args(
    *,
    source: KafkaSourceConfig,
    secrets_provider: Optional[SecretsProvider] = None,
) -> BuiltConnectorArgs:
    # secrets_provider unused for Spark-based connector.
    _ = secrets_provider

    connection = build_kafka_connection(source)
    return BuiltConnectorArgs(args=(connection,), kwargs={})