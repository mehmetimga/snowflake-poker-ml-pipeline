"""PostgreSQL/Debezium ingress and local simulation contracts.

The production decoder remains intentionally absent. Connection-owning
simulation commands live under ``scripts/``; this package keeps mapping,
binary codecs, and database writer boundaries testable in isolation.
"""

from .hand_history import (
    CDC_HAND_OUTBOX_TOPIC,
    FIXTURE_CODEC_VERSION,
    CdcAdaptedHand,
    CdcAdapterConfig,
    CdcLineage,
    CdcRecordRejected,
    HandHistoryDecoder,
    KafkaSourcePosition,
    adapt_debezium_hand_change,
    cdc_lineage_headers,
)
from .simulation_codec import (
    SIMULATION_PROTOBUF_CODEC_VERSION,
    SimulationProtobufV1Decoder,
    encode_simulation_hand,
    public_hand_payload,
)

__all__ = [
    "CDC_HAND_OUTBOX_TOPIC",
    "FIXTURE_CODEC_VERSION",
    "CdcAdaptedHand",
    "CdcAdapterConfig",
    "CdcLineage",
    "CdcRecordRejected",
    "HandHistoryDecoder",
    "KafkaSourcePosition",
    "adapt_debezium_hand_change",
    "cdc_lineage_headers",
    "SIMULATION_PROTOBUF_CODEC_VERSION",
    "SimulationProtobufV1Decoder",
    "encode_simulation_hand",
    "public_hand_payload",
]
