from metabricks.core.state_store import InMemoryStateStore


def test_in_memory_state_store_round_trip():
    store = InMemoryStateStore()
    assert store.get("missing") is None

    store.set("k", "v")
    assert store.get("k") == "v"

    store.delete("k")
    assert store.get("k") is None
