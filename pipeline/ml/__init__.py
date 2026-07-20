from .evaluation import evaluate_model
from .pair_model import PairPreprocessor, PlattCalibrator


_PAIR_DATASET_EXPORTS = frozenset(
    {
        "MODEL_CATEGORICAL_FEATURE_COLUMNS",
        "MODEL_FEATURE_COLUMNS",
        "MODEL_NUMERIC_FEATURE_COLUMNS",
        "PairDatasetBuildConfig",
    }
)


def __getattr__(name: str):
    if name in _PAIR_DATASET_EXPORTS:
        from . import pair_dataset

        return getattr(pair_dataset, name)
    raise AttributeError(name)


def build_pair_datasets(*args, **kwargs):
    """Load the warehouse/event feature stack only when dataset build is requested."""
    from .pair_dataset import build_pair_datasets as _build_pair_datasets

    return _build_pair_datasets(*args, **kwargs)


def train_all(*args, **kwargs):
    """Import the heavier training stack only when it is actually requested."""
    from .train import train_all as _train_all

    return _train_all(*args, **kwargs)


__all__ = [
    "MODEL_CATEGORICAL_FEATURE_COLUMNS",
    "MODEL_FEATURE_COLUMNS",
    "MODEL_NUMERIC_FEATURE_COLUMNS",
    "PairDatasetBuildConfig",
    "PairPreprocessor",
    "PlattCalibrator",
    "build_pair_datasets",
    "evaluate_model",
    "train_all",
]
