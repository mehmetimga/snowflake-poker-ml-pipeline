from .lstm_encoder import LSTMEncoder
from .transformer import TransformerEncoder
from .focal_loss import FocalLoss


def train_sequence_models(*args, **kwargs):
    """Load warehouse-coupled sequence training only when it is requested."""
    from .train import train_sequence_models as _train_sequence_models

    return _train_sequence_models(*args, **kwargs)

__all__ = ["LSTMEncoder", "TransformerEncoder", "FocalLoss", "train_sequence_models"]
