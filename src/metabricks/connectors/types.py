from __future__ import annotations

from dataclasses import dataclass


@dataclass
class KafkaConnection:
    bootstrap_servers: str
    topic: str
    starting_offsets: str = "latest"
    kafka_options: dict[str, str] | None = None