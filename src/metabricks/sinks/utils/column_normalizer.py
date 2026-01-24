from __future__ import annotations

import re
from typing import Dict, Iterable, List, Literal, Tuple

from metabricks.core.contracts import ExecutionContext


NormalizationStrategy = Literal["spark_safe", "lower_snake"]

_INVALID_CHARS = re.compile(r"[^0-9A-Za-z_]+")
_DUP_UNDERSCORES = re.compile(r"_+")


def normalize_column_name(name: str, strategy: NormalizationStrategy) -> str:
    cleaned = _INVALID_CHARS.sub("_", name.strip())
    cleaned = _DUP_UNDERSCORES.sub("_", cleaned).strip("_")
    if not cleaned:
        cleaned = "col"
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    if strategy in ("spark_safe", "lower_snake"):
        cleaned = cleaned.lower()
    return cleaned


def build_column_mapping(columns: Iterable[str], strategy: NormalizationStrategy) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    used: Dict[str, str] = {}
    for col in columns:
        base = normalize_column_name(col, strategy)
        candidate = base
        counter = 1
        while candidate in used and used[candidate] != col:
            candidate = f"{base}_{counter}"
            counter += 1
        mapping[col] = candidate
        used[candidate] = col
    return mapping


def normalize_dataframe_columns(df, strategy: NormalizationStrategy) -> Tuple[object, Dict[str, str], bool]:
    mapping = build_column_mapping(df.columns, strategy)
    changed = any(original != normalized for original, normalized in mapping.items())
    if not changed:
        return df, mapping, False
    for original, normalized in mapping.items():
        if original != normalized:
            df = df.withColumnRenamed(original, normalized)
    return df, mapping, True


def resolve_column_list(columns: Iterable[str], mapping: Dict[str, str], label: str) -> List[str]:
    mapped: List[str] = []
    mapping_values = set(mapping.values())
    missing: List[str] = []
    for col in columns:
        if col in mapping:
            mapped.append(mapping[col])
        elif col in mapping_values:
            mapped.append(col)
        else:
            missing.append(col)
    if missing:
        raise ValueError(f"{label} refers to unknown columns after normalization: {missing}")
    return mapped


def resolve_overwrite_scope(
    overwrite_scope: List[Dict[str, str]] | None,
    mapping: Dict[str, str],
) -> List[Dict[str, str]] | None:
    if not overwrite_scope:
        return overwrite_scope
    resolved: List[Dict[str, str]] = []
    for entry in overwrite_scope:
        normalized_entry: Dict[str, str] = {}
        for key, value in entry.items():
            mapped_key = resolve_column_list([key], mapping, "overwrite_scope")[0]
            if mapped_key in normalized_entry and mapped_key != key:
                raise ValueError(
                    f"overwrite_scope has conflicting keys after normalization: {key} -> {mapped_key}"
                )
            normalized_entry[mapped_key] = value
        resolved.append(normalized_entry)
    return resolved


def store_column_mapping(
    context: ExecutionContext | None,
    mapping: Dict[str, str],
    key: str,
) -> None:
    if context is None:
        return
    context.metadata[key] = dict(mapping)
