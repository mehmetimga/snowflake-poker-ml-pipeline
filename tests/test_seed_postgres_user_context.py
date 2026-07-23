from types import SimpleNamespace

from scripts.seed_postgres_user_context import UPSERT, context_rows


def test_context_rows_carry_event_scope_into_composite_upsert() -> None:
    event = SimpleNamespace(
        tenant_id="tenant-a",
        product_id="poker",
        payload={"user_id": "player-1", "context_version": 1},
    )

    assert context_rows([event]) == [
        {
            "tenant_id": "tenant-a",
            "product_id": "poker",
            "user_id": "player-1",
            "context_version": 1,
        }
    ]
    assert (
        "ON CONFLICT (tenant_id, product_id, user_id, context_version)"
        in UPSERT
    )
