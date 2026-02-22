from __future__ import annotations

from metabricks.core.template_resolution import resolve_template_string, resolve_templates


def test_resolve_template_string_replaces_identifier_placeholders_only():
    s = "s3://bucket/{{env}}/{{snapshot_date}}/{{secrets/foo}}/${run_id}"
    out = resolve_template_string(
        s,
        {
            "env": "dev",
            "snapshot_date": "2026-01-19",
            "run_id": "r1",
        },
    )

    assert out == "s3://bucket/dev/2026-01-19/{{secrets/foo}}/r1"


def test_resolve_templates_deep_replaces_values_in_nested_structures():
    obj = {
        "a": "{{x}}",
        "b": ["${y}", {"c": "prefix-{{x}}"}],
        "keep": "{{secrets/bar}}",
    }
    out = resolve_templates(obj, {"x": 1, "y": "two"})
    assert out["a"] == "1"
    assert out["b"][0] == "two"
    assert out["b"][1]["c"] == "prefix-1"
    assert out["keep"] == "{{secrets/bar}}"
