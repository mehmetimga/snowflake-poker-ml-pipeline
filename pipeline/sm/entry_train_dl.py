"""SageMaker Training entrypoint for LSTM + Transformer (BYOC, GPU).

This is the primary GPU workload — runs on ml.g5.xlarge by default.
The training function already auto-detects CUDA via torch.cuda.is_available().
"""

from __future__ import annotations

from pipeline.sm.common import configure_models_dir
from pipeline.dl.train import train_sequence_models
from pipeline.warehouse import get_warehouse


def main() -> None:
    configure_models_dir()
    wh = get_warehouse()
    train_sequence_models(wh)
    wh.close()


if __name__ == "__main__":
    main()
