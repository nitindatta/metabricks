from metabricks.sinks.utils.column_normalizer import (
    build_column_mapping,
    normalize_column_name,
    resolve_column_list,
)


def test_normalize_column_name_spark_safe_uses_snake_case():
    assert normalize_column_name("Customer ID", "spark_safe") == "customer_id"


def test_normalize_column_name_lower_snake_forces_lowercase():
    assert normalize_column_name("Customer ID", "lower_snake") == "customer_id"


def test_build_column_mapping_resolves_collisions():
    mapping = build_column_mapping(["A B", "A-B"], "lower_snake")
    assert mapping["A B"] == "a_b"
    assert mapping["A-B"] == "a_b_1"


def test_resolve_column_list_accepts_normalized_names():
    mapping = {"User ID": "user_id", "City": "city"}
    resolved = resolve_column_list(["user_id", "City"], mapping, "partition_by")
    assert resolved == ["user_id", "city"]
