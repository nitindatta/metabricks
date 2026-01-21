import logging
import sys
import contextvars
from typing import Optional

# Context variable to carry the current run id across the call chain
_RUN_ID: contextvars.ContextVar[str] = contextvars.ContextVar("run_id", default="-")


class _InvocationFilter(logging.Filter):
    """Logging filter that injects the invocation_id from contextvars into the record."""

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        try:
            run_id = _RUN_ID.get()
            # Prefer run_id going forward, but keep invocation_id for backward compatibility.
            record.run_id = run_id
            record.invocation_id = run_id
        except Exception:
            record.run_id = "-"
            record.invocation_id = "-"
        return True


def _build_formatter() -> logging.Formatter:
    return logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | run=%(run_id)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def configure_root_logger(level: str = "INFO") -> None:
    """
    Configure root logger and metabricks-specific logger.
    
    Root logger stays at INFO to suppress library noise (py4j, databricks, etc).
    Only metabricks namespace logs are set to the requested level.
    
    Args:
        level: Log level for metabricks logs (DEBUG, INFO, WARNING, ERROR). 
               Other libraries stay at INFO.
    
    Safe to call multiple times; it will not duplicate handlers (idempotent).
    """
    root = logging.getLogger()
    
    # Check if we already configured our handler (has _InvocationFilter)
    for h in root.handlers:
        if isinstance(h, logging.StreamHandler) and any(isinstance(f, _InvocationFilter) for f in h.filters):
            # Already configured; just update metabricks logger level
            metabricks_logger = logging.getLogger("metabricks")
            metabricks_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
            return

    # Remove any existing handlers in notebook environments to avoid duplication
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_build_formatter())
    handler.addFilter(_InvocationFilter())
    handler.setLevel(logging.INFO)  # Root stays at INFO to suppress library noise
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    
    # Configure metabricks-specific logger to requested level
    metabricks_logger = logging.getLogger("metabricks")
    metabricks_logger.setLevel(getattr(logging, level.upper(), logging.INFO))


def get_logger(name: str = "metabricks", level: str = "INFO") -> logging.Logger:
    """
    Get a module-specific logger configured for notebook-friendly stdout and invocation context.
    """
    configure_root_logger(level)
    logger = logging.getLogger(name)
    # Avoid changing handlers on the child logger; rely on root configuration
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger


def push_run_id(run_id: Optional[str]) -> Optional[contextvars.Token]:
    """Set the current run id in context and return a token for later reset."""
    if not run_id:
        return None
    return _RUN_ID.set(run_id)


def reset_run_id(token: Optional[contextvars.Token]) -> None:
    """Reset the run id context using the provided token (if any)."""
    if token is None:
        return
    try:
        _RUN_ID.reset(token)
    except Exception:
        # Don't let logging context cleanup crash app flows
        pass


# Backward compatibility aliases
push_invocation_id = push_run_id
reset_invocation_id = reset_run_id