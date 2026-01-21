from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Mapping


# Only allow simple identifiers to avoid clobbering other templating schemes
# like {{secrets/...}} which are handled elsewhere.
_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*"
_PATTERN = re.compile(rf"\{{\{{\s*({_IDENTIFIER})\s*\}}\}}|\$\{{({_IDENTIFIER})\}}")


def default_template_vars(*, now: datetime | None = None) -> dict[str, str]:
    now = now or datetime.now(timezone.utc)
    return {
        "extract_ts_yyyy": f"{now.year:04d}",
        "extract_ts_MM": f"{now.month:02d}",
        "extract_ts_dd": f"{now.day:02d}",
        "extract_ts_HH": f"{now.hour:02d}",
        "extract_ts_mm": f"{now.minute:02d}",
        "extract_ts_ss": f"{now.second:02d}",
        "extract_ts_yyyyMMddHHmmssffffff": now.strftime("%Y%m%d%H%M%S%f"),
    }


def resolve_template_string(value: str, variables: Mapping[str, Any]) -> str:
    if "{{" not in value and "${" not in value:
        return value

    def _repl(match: re.Match[str]) -> str:
        key = match.group(1) or match.group(2)
        if key in variables and variables[key] is not None:
            return str(variables[key])
        return match.group(0)

    return _PATTERN.sub(_repl, value)


def resolve_templates(obj: Any, variables: Mapping[str, Any]) -> Any:
    """Recursively resolve {{var}} and ${var} in any strings inside obj.

    - Only replaces variables that match a safe identifier pattern.
    - Leaves unknown placeholders untouched.
    - Leaves non-matching placeholders (e.g. {{secrets/foo}}) untouched.
    """

    if obj is None:
        return None

    if isinstance(obj, str):
        return resolve_template_string(obj, variables)

    if isinstance(obj, list):
        return [resolve_templates(x, variables) for x in obj]

    if isinstance(obj, tuple):
        return tuple(resolve_templates(x, variables) for x in obj)

    if isinstance(obj, dict):
        return {
            resolve_templates(k, variables): resolve_templates(v, variables)
            for k, v in obj.items()
        }

    return obj
