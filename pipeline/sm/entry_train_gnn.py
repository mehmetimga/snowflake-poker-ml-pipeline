"""SageMaker Training entrypoint for the GNN stage (VGAE + simple HGT, BYOC, GPU)."""

from __future__ import annotations

from pipeline.sm.common import configure_models_dir
from pipeline.gnn.train import train_gnn
from pipeline.warehouse import get_warehouse


def main() -> None:
    configure_models_dir()
    wh = get_warehouse()
    train_gnn(wh)
    wh.close()


if __name__ == "__main__":
    main()
