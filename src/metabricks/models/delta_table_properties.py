"""Strongly-typed Delta table properties for Databricks.

Separates table-level properties (SET TBLPROPERTIES) from write-time options.
All properties are typed with Pydantic validation.
"""

from __future__ import annotations

from typing import Dict, Literal, Optional

from pydantic import BaseModel, Field, field_validator


class DeltaColumnMapping(BaseModel):
    """Delta column mapping for rename/drop support."""

    enabled: bool = False
    mode: Literal["none", "id", "name"] = "none"

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str, info):
        if info.data.get("enabled") and v == "none":
            raise ValueError("mode cannot be 'none' when enabled=True")
        return v

    def to_sql_properties(self) -> Dict[str, str]:
        """Convert to TBLPROPERTIES dict for SQL."""
        return {
            "delta.columnMapping.enabled": "true" if self.enabled else "false",
            "delta.columnMapping.mode": self.mode,
        }


class DeltaChangeDataFeed(BaseModel):
    """Delta Change Data Feed configuration."""

    enabled: bool = False

    def to_sql_properties(self) -> Dict[str, str]:
        """Convert to TBLPROPERTIES dict for SQL."""
        return {
            "delta.enableChangeDataFeed": "true" if self.enabled else "false",
        }


class DeltaOptimization(BaseModel):
    """Delta auto-optimization settings."""

    auto_compact: bool = False
    optimize_write: bool = False
    target_file_size_bytes: Optional[int] = None  # e.g., 1073741824 for 1GB
    tune_file_sizes_for_rewrites: bool = False

    def to_sql_properties(self) -> Dict[str, str]:
        """Convert to TBLPROPERTIES dict for SQL."""
        props = {
            "delta.autoOptimize.autoCompact": "true" if self.auto_compact else "false",
            "delta.autoOptimize.optimizeWrite": "true" if self.optimize_write else "false",
            "delta.tuneFileSizesForRewrites": "true" if self.tune_file_sizes_for_rewrites else "false",
        }
        if self.target_file_size_bytes is not None:
            props["delta.targetFileSize"] = str(self.target_file_size_bytes)
        return props


class DeltaTimeTravel(BaseModel):
    """Delta time travel and history retention settings."""

    log_retention_days: int = 30  # Transaction log retention (default 30)
    deleted_file_retention_days: int = 7  # Time travel window (default 7)
    checkpoint_interval_commits: int = 10  # Checkpoint frequency (default 10)

    @field_validator("log_retention_days", "deleted_file_retention_days", "checkpoint_interval_commits")
    @classmethod
    def validate_positive(cls, v: int):
        if v <= 0:
            raise ValueError("Duration/interval must be positive")
        return v

    def to_sql_properties(self) -> Dict[str, str]:
        """Convert to TBLPROPERTIES dict for SQL."""
        return {
            "delta.logRetentionDuration": f"{self.log_retention_days} days",
            "delta.deletedFileRetentionDuration": f"{self.deleted_file_retention_days} days",
            "delta.checkpointInterval": str(self.checkpoint_interval_commits),
        }


class DeltaConstraints(BaseModel):
    """Delta table constraints for data quality."""

    check_constraints: Dict[str, str] = Field(default_factory=dict)  # name -> expression
    invariant_constraints: Dict[str, str] = Field(default_factory=dict)  # deprecated, for compatibility

    def to_sql_properties(self) -> Dict[str, str]:
        """Convert to TBLPROPERTIES dict for SQL (invariants only)."""
        # CHECK constraints are applied via ALTER TABLE ADD CONSTRAINT
        # Invariants (deprecated) go to TBLPROPERTIES
        return {
            f"delta.invariants.{name}": expr
            for name, expr in self.invariant_constraints.items()
        }


class DeltaClustering(BaseModel):
    """Delta clustering strategies for query optimization."""

    z_order_columns: Optional[list[str]] = None  # For data skipping
    liquid_clustering_columns: Optional[list[str]] = None  # Modern alternative to partitioning
    bloom_filter_columns: Optional[list[str]] = None  # Point lookup optimization

    def to_sql_properties(self) -> Dict[str, str]:
        """These are typically applied via OPTIMIZE/clustering commands, not TBLPROPERTIES."""
        # Metadata properties for documentation
        props = {}
        if self.z_order_columns:
            props["delta.zorderColumns"] = ",".join(self.z_order_columns)
        if self.liquid_clustering_columns:
            props["delta.liquidClusteringColumns"] = ",".join(self.liquid_clustering_columns)
        if self.bloom_filter_columns:
            props["delta.bloomFilterColumns"] = ",".join(self.bloom_filter_columns)
        return props


class DeltaTableProperties(BaseModel):
    """Complete, strongly-typed Delta table properties.

    These are set via SQL DDL (SET TBLPROPERTIES) and persist with the table.
    Separate from write-time options which are transient.
    """

    # Governance
    description: Optional[str] = None  # Table comment/documentation
    owner: Optional[str] = None  # Table owner principal
    custom_properties: Dict[str, str] = Field(default_factory=dict)  # User-defined properties

    # Features
    column_mapping: DeltaColumnMapping = Field(default_factory=DeltaColumnMapping)
    change_data_feed: DeltaChangeDataFeed = Field(default_factory=DeltaChangeDataFeed)

    # Optimization
    optimization: DeltaOptimization = Field(default_factory=DeltaOptimization)

    # History & Retention
    time_travel: DeltaTimeTravel = Field(default_factory=DeltaTimeTravel)

    # Quality
    constraints: DeltaConstraints = Field(default_factory=DeltaConstraints)

    # Clustering
    clustering: DeltaClustering = Field(default_factory=DeltaClustering)

    def to_sql_properties(self) -> Dict[str, str]:
        """Convert all properties to flat TBLPROPERTIES dict for SQL."""
        props = {}

        # Description
        if self.description:
            props["comment"] = self.description

        # Owner
        if self.owner:
            props["owner"] = self.owner

        # Features
        props.update(self.column_mapping.to_sql_properties())
        props.update(self.change_data_feed.to_sql_properties())

        # Optimization
        props.update(self.optimization.to_sql_properties())

        # Time Travel
        props.update(self.time_travel.to_sql_properties())

        # Constraints
        props.update(self.constraints.to_sql_properties())

        # Clustering metadata
        props.update(self.clustering.to_sql_properties())

        # Custom properties
        props.update(self.custom_properties)

        return props

    @staticmethod
    def defaults_for_enterprise() -> DeltaTableProperties:
        """Recommended defaults for enterprise/production use."""
        return DeltaTableProperties(
            column_mapping=DeltaColumnMapping(enabled=True, mode="name"),
            change_data_feed=DeltaChangeDataFeed(enabled=False),
            optimization=DeltaOptimization(
                auto_compact=True,
                optimize_write=True,
                tune_file_sizes_for_rewrites=False,
            ),
            time_travel=DeltaTimeTravel(
                log_retention_days=30,
                deleted_file_retention_days=7,
                checkpoint_interval_commits=10,
            ),
        )

    @staticmethod
    def defaults_for_high_volume() -> DeltaTableProperties:
        """Optimized for high-volume analytical workloads."""
        return DeltaTableProperties(
            column_mapping=DeltaColumnMapping(enabled=True, mode="name"),
            optimization=DeltaOptimization(
                auto_compact=True,
                optimize_write=True,
                target_file_size_bytes=1073741824,  # 1GB
                tune_file_sizes_for_rewrites=True,
            ),
            time_travel=DeltaTimeTravel(
                log_retention_days=7,  # Shorter retention for space
                deleted_file_retention_days=1,
                checkpoint_interval_commits=5,
            ),
        )

    @staticmethod
    def defaults_for_cdc() -> DeltaTableProperties:
        """Optimized for Change Data Feed consumption."""
        return DeltaTableProperties(
            column_mapping=DeltaColumnMapping(enabled=True, mode="name"),
            change_data_feed=DeltaChangeDataFeed(enabled=True),
            optimization=DeltaOptimization(auto_compact=False, optimize_write=False),
        )
