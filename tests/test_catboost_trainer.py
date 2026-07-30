from __future__ import annotations

import numpy as np

from pipeline.ml.trainers import catboost_trainer


def test_catboost_training_disables_runtime_working_files(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeCatBoostClassifier:
        def __init__(self, **kwargs: object) -> None:
            captured["kwargs"] = kwargs

        def fit(self, *args: object, **kwargs: object) -> None:
            captured["fit_args"] = args
            captured["fit_kwargs"] = kwargs

    monkeypatch.setattr(
        catboost_trainer, "CatBoostClassifier", FakeCatBoostClassifier
    )
    train = np.array([[0.0], [1.0]])
    labels = np.array([0, 1])
    validation = np.array([[0.5], [0.75]])
    validation_labels = np.array([0, 1])

    model = catboost_trainer.train_catboost(
        train, labels, validation, validation_labels
    )

    assert isinstance(model, FakeCatBoostClassifier)
    assert captured["kwargs"]["allow_writing_files"] is False
    assert captured["fit_args"] == (train, labels)
    assert captured["fit_kwargs"] == {
        "eval_set": (validation, validation_labels),
        "verbose": False,
    }
