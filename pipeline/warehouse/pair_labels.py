"""Restricted, idempotent loading for delayed pair labels."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Mapping

import pandas as pd

from pipeline.events import PairHandLabel
from pipeline.warehouse.factory import Warehouse
from pipeline.warehouse.sql import delete_by_values


def load_pair_labels(
    warehouse: Warehouse,
    labels: Iterable[PairHandLabel | Mapping[str, object]],
) -> int:
    unique: dict[str, PairHandLabel] = {}
    for raw in labels:
        if isinstance(raw, PairHandLabel):
            label = raw
        else:
            values = dict(raw)
            values.setdefault("dataset_split", values.pop("source_dataset_split", None))
            values.pop("benchmark_split", None)
            label = PairHandLabel.model_validate(values)
        key = str(label.example_id)
        previous = unique.get(key)
        if previous is not None and previous != label:
            raise ValueError(f"example_id collision with different label: {key}")
        unique[key] = label
    if not unique:
        return 0
    ingested_at = datetime.now(timezone.utc)
    frame = pd.DataFrame(
        [
            {
                "example_id": str(label.example_id),
                "dataset_id": label.dataset_id,
                "dataset_split": label.dataset_split,
                "hand_id": label.hand_id,
                "pair_key": label.pair_key,
                "player_a": label.player_a,
                "player_b": label.player_b,
                "is_collusive": label.is_collusive,
                "collusion_pair_id": label.collusion_pair_id,
                "label_available_at": label.label_available_at,
                "provenance": label.provenance,
                "ingested_at": ingested_at,
            }
            for label in unique.values()
        ]
    )
    warehouse.execute("BEGIN")
    try:
        if warehouse.kind != "duckdb":
            delete_by_values(
                warehouse,
                "PAIR_LABELS",
                "example_id",
                frame["example_id"].tolist(),
            )
        warehouse.write_pandas(frame, "PAIR_LABELS")
        warehouse.execute("COMMIT")
    except Exception:
        warehouse.execute("ROLLBACK")
        raise
    return len(frame)
