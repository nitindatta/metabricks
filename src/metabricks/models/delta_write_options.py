"""Write-time options for Delta writers.

These are transient options applied during write operations,
separate from persistent table properties.
"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class DeltaBatchWriteOptions(BaseModel):
    """Options specific to batch Delta writes."""

    mode: Literal["overwrite", "append"] = "overwrite"
    merge_schema: bool = False  # Evolve schema on write

    def to_write_options(self) -> dict[str, str]:
        """Convert to Spark write options."""
        opts = {}
        if self.merge_schema:
            opts["mergeSchema"] = "true"
        return opts


class DeltaStreamWriteOptions(BaseModel):
    """Options specific to streaming Delta writes."""

    output_mode: Literal["append", "complete", "update"] = "append"
    merge_schema: bool = True  # Usually enabled for streaming
    checkpoint_location: str = Field(...)  # Required for fault tolerance
    trigger_mode: Literal["availableNow", "continuous", "processingTime"] = "availableNow"
    trigger_interval: Optional[str] = None  # e.g., "10 seconds" for processingTime

    @field_validator("trigger_interval")
    @classmethod
    def validate_trigger_interval(cls, v: Optional[str], info):
        if info.data.get("trigger_mode") == "processingTime" and not v:
            raise ValueError("trigger_interval required for processingTime mode")
        if info.data.get("trigger_mode") != "processingTime" and v:
            raise ValueError("trigger_interval only valid for processingTime mode")
        return v

    def to_write_options(self) -> dict[str, str]:
        """Convert to Spark writeStream options."""
        opts = {
            "checkpointLocation": self.checkpoint_location,
            "mergeSchema": "true" if self.merge_schema else "false",
        }
        return opts

    def to_trigger_dict(self) -> dict:
        """Convert to trigger dict for writeStream.trigger()."""
        if self.trigger_mode == "availableNow":
            return {"availableNow": True}
        elif self.trigger_mode == "continuous":
            return {"continuous": "0"}
        elif self.trigger_mode == "processingTime":
            return {"processingTime": self.trigger_interval}
        else:
            return {"availableNow": True}
