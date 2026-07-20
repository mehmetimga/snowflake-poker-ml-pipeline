from __future__ import annotations

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from pipeline.ml.ensemble import (
    _portable_logistic,
    make_hand_grouped_folds,
    portable_logistic_predict,
)


def test_hand_grouped_folds_assign_every_row_once_without_hand_overlap() -> None:
    hand_ids = np.repeat([f"hand-{index}" for index in range(30)], 15)
    labels = np.zeros(len(hand_ids), dtype=np.int8)
    for hand in (1, 5, 9, 13, 17, 21, 25, 29):
        labels[hand * 15] = 1
    folds = make_hand_grouped_folds(labels, hand_ids, folds=4, seed=17)
    assert set(folds) == {0, 1, 2, 3}
    assert all(len(set(folds[hand_ids == hand])) == 1 for hand in set(hand_ids))


def test_portable_logistic_matches_sklearn_pipeline() -> None:
    generator = np.random.default_rng(9)
    matrix = generator.normal(size=(200, 3))
    labels = (matrix[:, 0] - 0.5 * matrix[:, 1] > 0).astype(int)
    model = make_pipeline(StandardScaler(), LogisticRegression(random_state=9))
    model.fit(matrix, labels)
    contract = _portable_logistic(model, ["a", "b", "c"])
    expected = model.predict_proba(matrix)[:, 1]
    actual = portable_logistic_predict(contract, matrix)
    np.testing.assert_allclose(actual, expected, rtol=1e-12, atol=1e-12)
