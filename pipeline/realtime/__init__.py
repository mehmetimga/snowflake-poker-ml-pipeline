__all__ = [
    "PatternSearchConfig",
    "RealTimeBatchResult",
    "RealTimeProcessor",
    "RollingPairMemory",
    "action_events_from_hand",
    "detect_action_patterns",
]


def __getattr__(name: str):
    if name in {"action_events_from_hand", "detect_action_patterns"}:
        from . import action_patterns

        return getattr(action_patterns, name)
    if name == "RollingPairMemory":
        from .pair_memory import RollingPairMemory

        return RollingPairMemory
    if name == "PatternSearchConfig":
        from .pattern_search import PatternSearchConfig

        return PatternSearchConfig
    if name in {"RealTimeBatchResult", "RealTimeProcessor"}:
        from .processor import RealTimeBatchResult, RealTimeProcessor

        return {
            "RealTimeBatchResult": RealTimeBatchResult,
            "RealTimeProcessor": RealTimeProcessor,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
