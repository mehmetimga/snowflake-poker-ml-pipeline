"""SageMaker Training entrypoint for XGBoost (managed framework image, GPU).

This trains only XGBoost (not CatBoost/LightGBM) so we can hand off to AWS's
managed GPU XGBoost container. The other ML stages use the BYOC image with
their own entrypoints.
"""

from __future__ import annotations

from pipeline.sm.common import configure_models_dir
from pipeline.ml.train import train_all
from pipeline.warehouse import get_warehouse


def main() -> None:
    configure_models_dir()
    wh = get_warehouse()
    # train_all() trains all three classical models; for the SageMaker port we
    # rely on environment to scope which one this entry script "owns" — but
    # since training is cheap, we just run the full classical stage here and
    # the XGBoost artifact is what TrainingStep collects. CatBoost/LightGBM
    # entry scripts in other TrainingSteps will produce their own artifacts.
    train_all(wh, only=["xgboost"])
    wh.close()


if __name__ == "__main__":
    main()
