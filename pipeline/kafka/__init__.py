__all__ = [
    "HandProducer",
    "WarehouseSink",
    "WorldEventProducer",
    "WorldTopics",
    "WorldWarehouseSink",
]


def __getattr__(name):
    if name == "HandProducer":
        from .producer import HandProducer

        return HandProducer
    if name == "WarehouseSink":
        from .consumer import WarehouseSink

        return WarehouseSink
    if name in {"WorldEventProducer", "WorldTopics"}:
        from .event_producer import WorldEventProducer, WorldTopics

        return {"WorldEventProducer": WorldEventProducer, "WorldTopics": WorldTopics}[name]
    if name == "WorldWarehouseSink":
        from .world_sink import WorldWarehouseSink

        return WorldWarehouseSink
    raise AttributeError(name)
