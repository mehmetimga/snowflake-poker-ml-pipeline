"""SageMaker Training entrypoint for CatBoost (BYOC, GPU when CAT_TASK_TYPE=GPU)."""

from __future__ import annotations

from pipeline.sm.common import configure_models_dir
from pipeline.ml.train import train_all
from pipeline.warehouse import get_warehouse


def main() -> None:
    configure_models_dir()
    wh = get_warehouse()
    train_all(wh, only=["catboost"])
    wh.close()


if __name__ == "__main__":
    main()
