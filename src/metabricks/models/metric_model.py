import math
from typing import Any
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field,model_validator

def _row_to_dict(v):
    # Spark Row has asDict()
    if hasattr(v, "asDict"):
        try:
            return {k: _row_to_dict(val) for k, val in v.asDict(recursive=True).items()}
        except TypeError:
            # some Row.asDict() implementations don't accept recursive
            return {k: _row_to_dict(val) for k, val in v.asDict().items()}

    if isinstance(v, dict):
        return {k: _row_to_dict(val) for k, val in v.items()}
    if isinstance(v, list):
        return [_row_to_dict(x) for x in v]
    return v

class _StreamingQueryProgressModel(BaseModel):
    def __snake_to_camel(value: str) -> str:
        value = value.split("_")
        return value[0] + "".join(word.capitalize() for word in value[1:])

    model_config = ConfigDict(populate_by_name=True, alias_generator=__snake_to_camel)

    @model_validator(mode="before")
    @classmethod
    def normalize_spark_rows(cls, data):
        return _row_to_dict(data)

class SinkProgress(_StreamingQueryProgressModel):
    description: str | None = None
    num_output_rows: int | None = None
    metrics: dict[str, Any] = Field(default_factory=dict)


class Metrics(_StreamingQueryProgressModel):
    values: list[int] = Field(default_factory=list)
    cnt: int | None = None

    @property
    def cnt_metric(self) -> int | None:
        return self.values[0] if self.values else self.cnt


class ObservedMetrics(_StreamingQueryProgressModel):
    metrics: Metrics | None = None

    def get_record_count(self) -> int | None:
        return self.metrics.cnt_metric if self.metrics else None


class StreamingQueryProgress(_StreamingQueryProgressModel):
    id: UUID
    run_id: UUID
    name: str | None = None
    timestamp: str | None = None
    batch_id: int
    batch_duration: int
    sink: SinkProgress
    observed_metrics: ObservedMetrics = Field(default_factory=ObservedMetrics)
    num_input_rows: int | None
    input_rows_per_second: float | None
    processed_rows_per_second: float | None

    @property
    def batch_duration_seconds(self) -> int:
        return math.ceil(self.batch_duration / 1000)
