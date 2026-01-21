"""
Custom exception classes for metabricks framework.

Provides structured error handling with domain-specific exceptions
for different layers of the ingestion pipeline.
"""

from enum import Enum
from typing import Any, Callable, Dict, Optional


class MetabricksException(Exception):
    """Base exception class for all metabricks exceptions."""

    pass


class NoSourceDataError(MetabricksException):
    """
    Raised when a source returns no data (empty result set).
    
    This is a general exception that occurs during data extraction when:
    - No rows match the query filter criteria
    - The source table is empty for the given time period
    - A logical_date/partition has no data
    
    Can be customized with context about what criteria resulted in no data.
    
    Example:
        >>> raise NoSourceDataError(
        ...     reason="No data for logical_date",
        ...     details={"logical_date": "2024-01-15", "source": "databricks"}
        ... )
    """

    def __init__(self, reason: str, details: dict = None):
        self.reason = reason
        self.details = details or {}
        message = f"{reason}"
        if self.details:
            message += f" - {self.details}"
        super().__init__(message)


class ConnectorError(MetabricksException):
    """Raised when a connector fails during extraction or initialization."""

    pass


class SinkError(MetabricksException):
    """Raised when a sink fails during write or initialization."""

    pass


class TransformationError(MetabricksException):
    """Raised when a transformation operation fails."""

    pass


class SchemaValidationError(MetabricksException):
    """Raised when data does not conform to expected schema."""

    pass


class EmptyDataPolicy(Enum):
    """Policy for handling empty data from sources."""
    
    FAIL = "fail"              # Raise NoSourceDataError (default)
    WARN = "warn"              # Log warning and continue
    ALLOW = "allow"            # Return empty DataEnvelope silently


class EmptyDataHandler:
    """
    Handles empty data scenarios based on configured policy.
    
    Usage:
        >>> handler = EmptyDataHandler(policy=EmptyDataPolicy.FAIL)
        >>> handler.handle(reason="No data found", details={"table": "source"})
        # Raises NoSourceDataError
        
        >>> handler = EmptyDataHandler(policy=EmptyDataPolicy.WARN)
        >>> handler.handle(reason="No data found", details={"table": "source"})
        # Logs warning and returns True (continue processing)
    """
    
    def __init__(
        self,
        policy: EmptyDataPolicy = EmptyDataPolicy.FAIL,
        logger: Optional[Any] = None,
        custom_handler: Optional[Callable[[str, Dict], Any]] = None,
    ):
        """
        Initialize the handler.
        
        Args:
            policy: How to handle empty data (FAIL, WARN, ALLOW)
            logger: Logger instance for WARN policy
            custom_handler: Custom function to handle empty data
        """
        self.policy = policy
        self.logger = logger
        self.custom_handler = custom_handler
    
    def handle(self, reason: str, details: Optional[Dict[str, Any]] = None) -> bool:
        """
        Handle empty data based on policy.
        
        Args:
            reason: Human-readable reason for empty data
            details: Additional context about the empty data
            
        Returns:
            True if processing should continue, False if it should stop
            
        Raises:
            NoSourceDataError: If policy is FAIL
        """
        details = details or {}
        
        if self.custom_handler:
            return self.custom_handler(reason, details)
        
        if self.policy == EmptyDataPolicy.FAIL:
            raise NoSourceDataError(reason=reason, details=details)
        
        elif self.policy == EmptyDataPolicy.WARN:
            if self.logger:
                self.logger.warning(f"{reason}: {details}")
            else:
                import warnings
                warnings.warn(f"{reason} - {details}", UserWarning)
            return True  # Continue processing
        
        elif self.policy == EmptyDataPolicy.ALLOW:
            # Silently allow empty data
            return True
        
        return True
