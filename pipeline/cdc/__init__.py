"""Future PostgreSQL/Debezium ingress contracts.

This package is executable readiness code.  It does not connect to a poker
server, PostgreSQL, Debezium, or Kafka by itself.
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
]
