"""SageMaker Processing entrypoint for batch inference -> ALERTS."""

from __future__ import annotations

from pipeline.sm.common import configure_models_dir
from pipeline.inference.scorer import score_warehouse
from pipeline.warehouse import get_warehouse


def main() -> None:
    configure_models_dir()
    wh = get_warehouse()
    score_warehouse(wh)
    wh.close()


if __name__ == "__main__":
    main()
